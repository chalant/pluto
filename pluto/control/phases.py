"""The phases of a simulated run, on the bus.

The controller owns the clock. At each tick it drives the run through phases: it publishes one event per phase on the
phase's topic and waits, lock-step, until the bus is idle and every participant of the phase said ``done``. A missing
``done`` (or one that carries an error) aborts the run with a ``PhaseError``; nothing waits for a timeout, so a run
stays deterministic. The order of the phases is the order the controller called its components before the bus:

==========  ====  ===============  ==========  ==========================  ===========================================
phase       rank  topic            published   subscribers                 done (participant: content)
==========  ====  ===============  ==========  ==========================  ===========================================
book        10    phase.book       per signal  broker                      broker: fill ids, stream events, watermark
metrics     20    phase.metrics    per signal  metrics                     metrics: tracker calls (0 or 1)
account     30    phase.account    per tick    broker, then the runtimes   broker: fill ids, states sent;
                                                                           runtime: fill ids, prices applied
trade       40    phase.trade      per tick    the runtimes                runtime: ids of the orders it sent
submit      50    phase.submit     per tick    broker                      broker: submitted ids, events, watermark
==========  ====  ===============  ==========  ==========================  ===========================================

A signal is one calendar's event of the tick: its two phases run one after the other for each signal, in the order of the
signals (the calendars' ledgers share a portfolio, so the writes of one signal and the reads of the next do not commute).

Messages of the phases carry ``logical_ts`` = the clock time of the tick in nanoseconds and ``phase`` = the rank above;
a ``done`` has the rank of its phase plus ``DONE_AFTER``. Data between participants travels on their own topics
(``strategy.account.<process id>``: the broker state for a runtime, ``strategy.orders`` and ``strategy.watchlist``, key
the process id: what the runtime sends back), with the rank of the phase that makes them.

Payloads are protobuf messages (``protos/clock.proto``, ``broker.proto``, ``phases.proto``), so ``codec()`` serialises every
one of them.
"""
from typing import NamedTuple

import logbook
import pandas as pd
from google.protobuf import timestamp_pb2

from pluto.bus import Bus, ProtobufCodec

from protos import broker_pb2, clock_pb2, phases_pb2


class Phase(NamedTuple):
    topic: str
    rank: int


BOOK = Phase('phase.book', 10)
METRICS = Phase('phase.metrics', 20)
ACCOUNT = Phase('phase.account', 30)
TRADE = Phase('phase.trade', 40)
SUBMIT = Phase('phase.submit', 50)

DONE_TOPIC = 'phase.done'
# a done is dispatched right after the messages of its phase
DONE_AFTER = 1

ORDERS_TOPIC = 'strategy.orders'
WATCHLIST_TOPIC = 'strategy.watchlist'

CONTROLLER_ID = 'controller'
BROKER_ID = 'broker'
METRICS_ID = 'metrics'


def account_topic(process_id: str) -> str:
    return 'strategy.account.' + process_id


def message_types() -> dict[str, type]:
    """The protobuf messages that travel on the bus, by the envelope ``type`` (the message name)."""
    return {message.DESCRIPTOR.name: message for message in (
        clock_pb2.ClockEvent,
        broker_pb2.BrokerState,
        broker_pb2.OrderBatch,
        broker_pb2.WatchList,
        phases_pb2.SignalEvent,
        phases_pb2.AccountEvent,
        phases_pb2.PhaseDone)}


def codec() -> ProtobufCodec:
    """The codec of a ``SerializingBus`` for a run."""
    return ProtobufCodec(message_types())


def message_type(message) -> str:
    return message.DESCRIPTOR.name


def proto_timestamp(ts: pd.Timestamp):
    """The protobuf timestamp of a pandas timestamp, to the nanosecond."""
    stamp = timestamp_pb2.Timestamp()
    stamp.FromNanoseconds(ts.value)
    return stamp


def pandas_timestamp(stamp) -> pd.Timestamp:
    """The UTC pandas timestamp of a protobuf timestamp, to the nanosecond."""
    return pd.Timestamp(stamp.ToNanoseconds(), tz='UTC')


class PhaseError(RuntimeError):
    """A phase did not complete: a participant failed or did not say done."""


class Summary(NamedTuple):
    """What a participant published in a phase."""
    ids: tuple[str, ...] = ()
    count: int = 0
    sequence: int = 0


def publish_done(
        bus: Bus,
        phase: Phase,
        logical_ts: int,
        participant: str,
        summary: Summary = Summary(),
        error: str = '') -> None:
    bus.publish(
        DONE_TOPIC,
        phases_pb2.PhaseDone(
            phase=phase.topic,
            participant=participant,
            ids=summary.ids,
            count=summary.count,
            sequence=summary.sequence,
            error=error),
        type=message_type(phases_pb2.PhaseDone),
        logical_ts=logical_ts,
        producer_id=participant,
        phase=phase.rank + DONE_AFTER,
        key=participant)


def report(bus: Bus, phase: Phase, logical_ts: int, participant: str, work, log: logbook.Logger) -> None:
    """Runs ``work()``, which returns a ``Summary``, and says done. A failure is logged with its traceback and
    reported in the done: the controller aborts the run, and the bus never redelivers the phase."""
    try:
        summary = work()
    except Exception as e:
        log.exception('{} failed in {}', participant, phase.topic)
        publish_done(bus, phase, logical_ts, participant, error='{}: {}'.format(type(e).__name__, e))
    else:
        publish_done(bus, phase, logical_ts, participant, summary)


class PhaseDriver(object):
    """The controller's side of the lock-step: publishes a phase event and checks the dones of the phase."""

    def __init__(self, bus: Bus, producer_id: str = CONTROLLER_ID):
        self._bus = bus
        self._producer_id = producer_id
        self._dones = []

        bus.subscribe(DONE_TOPIC, self._on_done)

    def _on_done(self, delivery) -> None:
        self._dones.append((delivery.envelope.logical_ts, delivery.envelope.payload))

    def drive(self, phase: Phase, payload, logical_ts: int, expected, key: str = '') -> tuple:
        """Publishes ``payload`` on the topic of ``phase``, runs the bus until it is idle and returns the dones, by
        participant name. ``expected`` are the participants that must say done; ``PhaseError`` if one did not, one
        failed, or the bus gave up on a delivery."""
        bus = self._bus
        dones = self._dones
        dones.clear()
        letters = len(bus.dead_letters())

        bus.publish(
            phase.topic,
            payload,
            type=message_type(payload),
            logical_ts=logical_ts,
            producer_id=self._producer_id,
            phase=phase.rank,
            key=key)
        bus.run_until_idle()

        problems = []
        seen = {}

        for done_ts, done in dones:
            if done.phase != phase.topic or done_ts != logical_ts:
                problems.append('{} sent a done of {} at {}'.format(
                    done.participant, done.phase, pd.Timestamp(done_ts, tz='UTC')))
            elif done.participant in seen:
                problems.append('{} said done twice'.format(done.participant))
            elif done.error:
                problems.append('{} failed: {}'.format(done.participant, done.error))
            else:
                seen[done.participant] = done

        missing = sorted(set(expected) - set(seen))
        if missing:
            problems.append('no done from {}'.format(', '.join(missing)))

        for letter in bus.dead_letters()[letters:]:
            problems.append('the bus gave up on a message of {} after {} attempts: {}: {}'.format(
                letter.envelope.topic, letter.attempts, letter.error_type, letter.error_message))

        if problems:
            raise PhaseError('{} at {}: {}'.format(
                phase.topic, pd.Timestamp(logical_ts, tz='UTC'), '; '.join(problems)))

        return tuple(seen[name] for name in sorted(seen))
