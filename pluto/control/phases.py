"""The phases of a simulated run, on the bus.

The controller owns the clock. At each tick it drives the run through phases: it publishes one event per phase on the
phase's topic and waits, lock-step, until every participant it expects said ``done`` (``PhaseDriver``): on the in-memory
bus the participants answer while the bus runs, so a missing ``done`` is known when it is idle; on a networked bus the
driver goes on reading the log until the dones are in, for at most a deadline of its own (the bus has no clock). A
missing ``done`` (or one that carries an error) aborts the run with a ``PhaseError``. Only the participants the
controller expects are required: a ``done`` from an unexpected participant
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

What a message may rely on. A run must give the same result on any legal run of the bus: the messages of one
``(logical_ts, phase)`` from different producers may reach a reader in any order, and a reader may deliver in the order of
the log instead of S3 (``tests/test_pluto_phase_order.py`` runs whole runs and the fake-book runs on buses that do both). So
no handler relies on the order of messages of different producers inside a phase; what it needs from another producer is
one of these, each explicit:

* the ``done`` barrier: the controller publishes the next phase only after every expected ``done`` of the last one is in
  the log, so everything a participant published before its ``done`` is before the next phase event (a log is a prefix:
  a reader that sees the event has seen all of it). The writes of the book (``book``) and the broker states of the
  tick (``account``) are ready for the next phase this way. A ``done`` can come before data published on another topic
  (the batch of orders a runtime announces in its ``trade`` done): the controller then waits for the data it was
  announced (``PhaseDriver.announced_orders``, deadline naming the runtime) before it publishes ``submit``;
* the account state before the trade event: a runtime applies the broker state it receives on its own topic
  ``strategy.account.<id>`` (not the controller's ``phase.account`` event, so it needs no order between the two), and it
  refuses a ``phase.trade`` of a tick whose state it has not applied (a ``done`` with an error), so a bus that reorders
  or a driver that pipelines phases cannot make it trade on a stale account;
* counts: a runtime's ``trade`` done says how many orders and batches it sent, the controller puts what it accepted into
  the ``submit`` event (``SubmitEvent``; the broker side does not learn it from a second subscription to the dones), the
  broker side checks them against what it received, waiting on a networked bus for what is missing, so an orders message
  that was not handled (lost, doubled) is a ``PhaseError``; a batch that arrives after its tick closed is counted by the
  strategy manager and reported, never dropped silently;
* the order of the strategies is rebuilt from the registration order (``StrategyManager.new_orders``), not taken from the
  delivery order of their batches.

Payloads are protobuf messages (``protos/clock.proto``, ``broker.proto``, ``phases.proto``), so ``codec()`` serialises every
one of them.
"""
import time
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

# a phase on a networked bus waits this long (seconds) for its dones, then fails naming who is missing; the controller
# polls the bus in slices of POLL_SLICE seconds
DEFAULT_TIMEOUT = 300.0
POLL_SLICE = 0.25

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
        phases_pb2.PhaseDone,
        phases_pb2.SubmitEvent)}


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
    """The controller's side of the lock-step: publishes a phase event and checks the dones of the phase.

    A phase is complete when every expected participant's done is in, not when the bus is idle. On the in-memory
    bus the two are the same (when ``run_until_idle`` returns nothing else can arrive: a missing done is a
    ``PhaseError`` at once). On a networked bus the dones come from other processes: the driver delivers what it has,
    ``poll``s the bus for more and goes on until all are in, a participant failed, or ``timeout`` seconds passed
    (``PhaseError`` naming who is missing). The deadline is the controller's: the bus has no clock."""

    def __init__(self, bus: Bus, producer_id: str = CONTROLLER_ID, timeout: float | None = DEFAULT_TIMEOUT):
        """
        Parameters
        ----------
        timeout: float, optional
            Seconds a phase may wait for its dones on a networked bus (``None``: no deadline); not used in memory.
        """
        self._bus = bus
        self._producer_id = producer_id
        self._timeout = timeout
        self._dones = []
        self._accepted = {}  # phase topic -> participant -> the done accepted, of the last drive of the phase
        self._batches = {}  # (logical_ts, process id) -> [orders, batches] of the batches of orders seen in the log

        bus.subscribe(DONES, self._on_done)
        bus.subscribe(ORDERS_TOPIC, self._on_orders, rank=1)

    def _on_done(self, delivery: Delivery) -> None:
        self._dones.append((delivery.envelope.logical_ts, delivery.envelope.payload))

    def _assess(self, phase: Phase, logical_ts: int, expected: Iterable[str]):
        """What the dones so far say: the problems found, and the expected participants that have not reported."""
        problems = []
        seen = {}
        reported = set()

        for done_ts, done in self._dones:
            if done.phase != phase.topic or done_ts != logical_ts:
                problems.append('{} sent a done of {} at {}'.format(
                    done.participant, done.phase, pd.Timestamp(done_ts, tz='UTC')))
                continue

            reported.add(done.participant)

            if done.participant in seen:
                problems.append('{} said done twice'.format(done.participant))
            elif done.error:
                problems.append('{} failed: {}'.format(done.participant, done.error))
            else:
                seen[done.participant] = done

        return problems, sorted(set(expected) - set(seen)), set(expected) - reported, seen

    def drive(
            self,
            phase: Phase,
            payload: Message,
            logical_ts: int,
            expected: Iterable[str],
            key: str = '') -> None:
        """Publishes ``payload`` on the topic of ``phase`` and runs the bus until every ``expected`` participant said
        done (see the class); ``PhaseError`` if one did not, one failed, or the bus gave up on a delivery.
        A done of a participant that is not expected is accepted (policy: only the expected ones are required, a
        participant that joined late may answer); it is an error only if it is not of this phase and tick or is
        doubled."""
        bus = self._bus
        expected = tuple(expected)
        self._dones.clear()
        self._accepted.pop(phase.topic, None)
        letters = len(bus.dead_letters())
        timed_out = False
        looked = False

        bus.publish(
            phase.topic,
            payload,
            type=message_type(payload),
            logical_ts=logical_ts,
            producer_id=self._producer_id,
            phase=phase.rank,
            key=key)

        deadline = None if self._timeout is None else time.monotonic() + self._timeout

        while True:
            bus.run_until_idle()
            problems, missing, waiting, seen = self._assess(phase, logical_ts, expected)

            if not waiting or problems or not bus.networked:
                # complete, failed, or nothing else can arrive
                break

            wait = POLL_SLICE if deadline is None else min(POLL_SLICE, deadline - time.monotonic())

            if wait <= 0 and not looked:
                # the deadline may have passed in a long local handler: what is already in the log has not been read
                looked = True
                bus.poll(0)
                continue

            if wait <= 0:
                timed_out = True
                break

            bus.poll(wait)

        self._accepted[phase.topic] = seen

        if missing:
            problems.append('no done from {}{}'.format(
                ', '.join(missing), ' within {:g} s'.format(self._timeout) if timed_out else ''))

        for letter in bus.dead_letters()[letters:]:
            problems.append('the bus gave up on a message of {} after {} attempts: {}: {}'.format(
                letter.envelope.topic, letter.attempts, letter.error_type, letter.error_message))

        if problems:
            raise PhaseError('{} at {}: {}'.format(
                phase.topic, pd.Timestamp(logical_ts, tz='UTC'), '; '.join(problems)))

    def _on_orders(self, delivery: Delivery) -> None:
        envelope = delivery.envelope
        counts = self._batches.setdefault((envelope.logical_ts, envelope.key), [0, 0])
        counts[0] += len(envelope.payload.orders)
        counts[1] += 1

    def announced_orders(self, logical_ts: int, runtimes: Iterable[str]) -> list[phases_pb2.RuntimeOrders]:
        """What each runtime announced in its trade done (orders, batches), once the log shows that many batches of
        orders of the tick: the submit phase starts only then, so the broker side has them (a log is a prefix: it reads
        every batch before the submit event). The counts go into the submit event, which is where the broker side learns
        them, not from a second subscription to the dones.

        A done can come before the batch it announces (another topic, another lane): on a networked bus the driver
        waits for the batches the way it waits for dones, ``PhaseError`` naming the runtimes whose batches did not come
        within the deadline. In memory nothing else can arrive: what is missing is reported by the broker side when it
        checks the counts, as before."""
        bus = self._bus
        dones = self._accepted.get(TRADE.topic, {})
        announced = [
            phases_pb2.RuntimeOrders(
                participant=runtime,
                orders=dones[runtime].orders if runtime in dones else 0,
                batches=dones[runtime].batches if runtime in dones else 0)
            for runtime in runtimes]
        deadline = None if self._timeout is None else time.monotonic() + self._timeout

        looked = False

        while True:
            bus.run_until_idle()
            short = [a for a in announced
                     if self._batches.get((logical_ts, a.participant), (0, 0))[0] < a.orders
                     or self._batches.get((logical_ts, a.participant), (0, 0))[1] < a.batches]

            if not short or not bus.networked:
                break

            wait = POLL_SLICE if deadline is None else min(POLL_SLICE, deadline - time.monotonic())

            if wait <= 0 and not looked:
                # the deadline may have passed in a long local handler: what is already in the log has not been read
                looked = True
                bus.poll(0)
                continue

            if wait <= 0:
                raise PhaseError('{} at {}: no batch of orders from {} within {:g} s ({})'.format(
                    TRADE.topic, pd.Timestamp(logical_ts, tz='UTC'), ', '.join(a.participant for a in short),
                    self._timeout, '; '.join(
                        '{} announced {} orders in {} batches, the log has {} in {}'.format(
                            a.participant, a.orders, a.batches, *self._batches.get((logical_ts, a.participant), (0, 0)))
                        for a in short)))

            bus.poll(wait)

        for tick, key in [k for k in self._batches if k[0] <= logical_ts]:
            del self._batches[(tick, key)]

        return announced
