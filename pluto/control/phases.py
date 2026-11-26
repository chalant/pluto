"""The phases of a simulated run, on the bus.

The controller owns the clock. At each tick it drives the run through phases: it publishes one event per phase on the
phase's topic and waits, lock-step, until the bus is idle and every participant of the phase said ``done``. A missing
``done`` (or one that carries an error) aborts the run with a ``PhaseError``; nothing waits for a timeout, so a run
stays deterministic. Only the participants the controller expects are required: a ``done`` from an unexpected participant
is accepted (one for another phase or tick, or a doubled one, is an error). The order of the phases is the order the
controller called its components before the bus:

==========  ====  ===============  ==========  ==========================  ===========================================
phase       rank  topic            published   subscribers                 done
==========  ====  ===============  ==========  ==========================  ===========================================
book        10    phase.book       per signal  broker                      broker
account     30    phase.account    per tick    broker, then the runtimes   broker, each runtime
trade       40    phase.trade      per tick    the runtimes                each runtime: its orders and batches sent
submit      50    phase.submit     per tick    broker                      broker
==========  ====  ===============  ==========  ==========================  ===========================================

A signal is one calendar's event of the tick: the book phase runs for each signal, one after the other, in the order of the
signals (the calendars' ledgers share a portfolio, so the writes of one signal and those of the next do not commute).
The metrics are not a phase: a consumer of the broker service's stream computes them (``pluto.finance.metrics.consumer``).

Messages of the phases carry ``logical_ts`` = the clock time of the tick in nanoseconds and ``phase`` = the rank above;
a ``done`` has the rank of its phase plus ``DONE_AFTER``. Data between participants travels on their own topics
(``strategy.account.<process id>``: the broker state for a runtime, ``strategy.orders``, key the process id: what the
runtime sends back), with the rank of the phase that makes them.

Payloads are protobuf messages (``protos/clock.proto``, ``broker.proto``, ``phases.proto``), so ``codec()`` serialises every
one of them.
"""
from collections.abc import Callable, Iterable
from typing import NamedTuple

import logbook
import pandas as pd
from google.protobuf import timestamp_pb2
from google.protobuf.message import Message

from pluto.bus import Bus, Delivery, ProtobufCodec

from protos import broker_pb2, clock_pb2, phases_pb2


class Phase(NamedTuple):
    name: str
    rank: int

    @property
    def topic(self) -> str:
        """The topic of the phase event."""
        return 'phase.' + self.name

    @property
    def done_topic(self) -> str:
        """The topic of the dones of the phase: a participant listens to the phases it needs, the controller to all."""
        return 'phase.done.' + self.name


BOOK = Phase('book', 10)
ACCOUNT = Phase('account', 30)
TRADE = Phase('trade', 40)
SUBMIT = Phase('submit', 50)

# the dones of every phase
DONES = 'phase.done.>'
# a done is dispatched right after the messages of its phase
DONE_AFTER = 1

ORDERS_TOPIC = 'strategy.orders'

CONTROLLER_ID = 'controller'
BROKER_ID = 'broker'


def account_topic(process_id: str) -> str:
    return 'strategy.account.' + process_id


def message_types() -> dict[str, type]:
    """The protobuf messages that travel on the bus, by the envelope ``type`` (the message name)."""
    return {message.DESCRIPTOR.name: message for message in (
        clock_pb2.ClockEvent,
        broker_pb2.BrokerState,
        broker_pb2.OrderBatch,
        phases_pb2.SignalEvent,
        phases_pb2.AccountEvent,
        phases_pb2.PhaseDone)}


def codec() -> ProtobufCodec:
    """The codec of a ``SerializingBus`` for a run."""
    return ProtobufCodec(message_types())


def message_type(message: Message) -> str:
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


def publish_done(
        bus: Bus,
        phase: Phase,
        logical_ts: int,
        participant: str,
        orders: int = 0,
        batches: int = 0,
        error: str = '') -> None:
    """Says that ``participant`` finished its work of ``phase`` at ``logical_ts``, or failed (``error``). ``orders`` and
    ``batches`` are the number of orders, and of batches of orders, a runtime sent in the trade phase."""
    bus.publish(
        phase.done_topic,
        phases_pb2.PhaseDone(
            phase=phase.topic, participant=participant, orders=orders, batches=batches, error=error),
        type=message_type(phases_pb2.PhaseDone),
        logical_ts=logical_ts,
        producer_id=participant,
        phase=phase.rank + DONE_AFTER,
        key=participant)


def fail(bus: Bus, phase: Phase, logical_ts: int, participant: str, error: Exception, log: logbook.Logger) -> None:
    """Logs ``error``, which is being handled, with its traceback and reports it in a done: the controller aborts the
    run, and the bus never redelivers the phase (a handler that raises is redelivered, a phase is not idempotent)."""
    log.exception('{} failed in {}', participant, phase.topic)
    publish_done(bus, phase, logical_ts, participant, error='{}: {}'.format(type(error).__name__, error))


def report(
        bus: Bus,
        phase: Phase,
        logical_ts: int,
        participant: str,
        work: Callable[[], None],
        log: logbook.Logger) -> None:
    """Runs ``work()`` and says done; a failure is reported (``fail``)."""
    try:
        work()
    except Exception as e:
        fail(bus, phase, logical_ts, participant, e, log)
    else:
        publish_done(bus, phase, logical_ts, participant)


class PhaseDriver(object):
    """The controller's side of the lock-step: publishes a phase event and checks the dones of the phase."""

    def __init__(self, bus: Bus, producer_id: str = CONTROLLER_ID):
        self._bus = bus
        self._producer_id = producer_id
        self._dones = []

        bus.subscribe(DONES, self._on_done)

    def _on_done(self, delivery: Delivery) -> None:
        self._dones.append((delivery.envelope.logical_ts, delivery.envelope.payload))

    def drive(
            self,
            phase: Phase,
            payload: Message,
            logical_ts: int,
            expected: Iterable[str],
            key: str = '') -> None:
        """Publishes ``payload`` on the topic of ``phase`` and runs the bus until it is idle. ``expected`` are the
        participants that must say done; ``PhaseError`` if one did not, one failed, or the bus gave up on a delivery.
        A done of a participant that is not expected is accepted (policy: only the expected ones are required, a
        participant that joined late may answer); it is an error only if it is not of this phase and tick or is
        doubled."""
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
