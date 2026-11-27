"""The phases of a simulated run, on the bus.

The controller owns the clock. At each tick it drives the run through phases: it publishes one event per phase on the
phase's topic and waits, lock-step, until every participant it expects said ``done`` (``PhaseDriver``): on the in-memory
bus the participants answer while the bus runs, so a missing ``done`` is known when it is idle; on a networked bus the
driver goes on reading the log until the dones are in, for at most a deadline of its own (the bus has no clock). A
missing ``done`` (or one that carries an error) aborts the run with a ``PhaseError``. Only the participants the
controller expects are required: a ``done`` from an unexpected participant is accepted (one for another phase or tick is an
error; the same done twice is one done, two dones that differ are an error). The order of the phases is the order the
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
  that was not handled (lost, sent twice with another id) is a ``PhaseError``; a batch that arrives after its tick closed is
  counted by the strategy manager and reported, never dropped silently (a copy of a batch it took is not late);
* the order of the strategies is rebuilt from the registration order (``StrategyManager.new_orders``), not taken from the
  delivery order of their batches.

Copies and retries. The bus delivers at least once and a publisher may have to publish again: a message must be harmless when it
comes twice. Every message has a deterministic ``msg_id`` that says what it is, not how often it was sent (``phase_msg_id``,
``done_msg_id``, ``account_msg_id``, ``orders_msg_id``), and a **step**: the driver numbers its drives 1, 2, 3, ... (one per phase
event; it only grows) and puts the number in the correlation id of the event and of everything that answers it (the done, the
broker state, the batches of orders: ``step_of``), whose ids derive from the id of the event. The clock cannot order the messages:
several clock events share a clock time, and the minute clock goes back inside a session. A publisher that gets ``PublishFailed``
publishes again with the same id (``publish_with_retry``, ``PUBLISH_ATTEMPTS`` times, then ``PhaseError``). A receiver ignores a
copy: the participants and the endpoint keep the ids they handled in the step in progress (``Handled``) and ignore any message of
an earlier step; the driver does the same for dones (a done of an earlier drive) and for the batches it counts; the strategy
manager keeps the batches it took for the last ``COPY_WINDOW`` drives, by id, and takes a batch for a copy only if it is one of
them. That is enough because of the lock-step: the next drive starts only when everything the last needed was handled, so what
arrives for an earlier one is a copy. What stays an error: a participant that does not answer, a done that contradicts the one
accepted under its id, a copy of a batch that differs from it, a batch of a closed tick that was not taken (late, also when its
copy would be older than the window), and the states a runtime is sent twice with other content (the first one wins: the broker
publishes them itself).

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

from pluto.bus import Bus, Delivery, Envelope, ProtobufCodec, PublishFailed

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


def phase_msg_id(phase: Phase, logical_ts: int, key: str, step: int) -> str:
    """The id of the event of ``phase`` that the driver published at its drive number ``step`` (``key``: the calendar of a
    book event; ``logical_ts``: the clock time): what the message is, not how often it was published."""
    return 'controller/{}/{}/{}/{}'.format(step, phase.name, logical_ts, key)


def done_msg_id(participant: str, event_id: str) -> str:
    """The id of the done of ``participant`` that answers the event with the id ``event_id``."""
    return '{}/done/{}'.format(participant, event_id)


def account_msg_id(process_id: str, event_id: str) -> str:
    """The id of the broker state sent to a runtime in answer to the account event ``event_id``."""
    return 'broker/account/{}/{}'.format(process_id, event_id)


def orders_msg_id(process_id: str, event_id: str, n: int) -> str:
    """The id of the ``n``-th batch of orders (1, 2, ...) a runtime sent while it handled the trade event ``event_id``."""
    return '{}/orders/{}/{}'.format(process_id, event_id, n)


# the dones of every phase
DONES = 'phase.done.>'
# a done is dispatched right after the messages of its phase
DONE_AFTER = 1

# a phase on a networked bus waits this long (seconds) for its dones, then fails naming who is missing; the controller
# polls the bus in slices of POLL_SLICE seconds
DEFAULT_TIMEOUT = 300.0
POLL_SLICE = 0.25

# a publish that fails with PublishFailed is repeated, with the same msg_id, this many times in all
PUBLISH_ATTEMPTS = 10

# the strategy manager remembers the batches of orders it took for this many drives (steps of the driver): a copy of one is
# ignored. A drive is a phase event; a clock event is at least four of them (book per signal, account, trade, submit), so
# the window is about 16 clock events. A copy that comes later is reported as a late batch, never dropped silently.
COPY_WINDOW = 64

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


def publish_with_retry(bus: Bus, topic: str, payload: Message, *, msg_id: str, **fields) -> None:
    """Publishes with ``msg_id``; a ``PublishFailed`` (the bus says the message may not have been taken, or that it was and
    it could not tell) is repeated, with the same ``msg_id``, up to ``PUBLISH_ATTEMPTS`` times in all: a copy that the
    first attempt left in the log is a copy of the same message, which every receiver ignores. ``PhaseError`` after the
    last attempt. No delay between attempts: the bus has no clock, the budget is a count.

    Only ``PublishFailed`` is retried (``BusClosed``, ``QueueFull``, bad arguments and every other error escape at once).
    Only the fault-injecting bus raises it today: the JetStream bus reports a message the broker refused as a plain
    ``BusError`` from its next read, so a real transient publish error is not retried yet (the bus needs to say which of
    its errors are transient; a later stage)."""
    failure = None

    for _ in range(PUBLISH_ATTEMPTS):
        try:
            bus.publish(topic, payload, msg_id=msg_id, **fields)
            return
        except PublishFailed as e:
            failure = e

    raise PhaseError('could not publish {} on {} in {} attempts: {}'.format(msg_id, topic, PUBLISH_ATTEMPTS, failure))


def step_of(envelope: Envelope) -> int | None:
    """The step of the driver that a message belongs to: the drive that published the event, carried in the correlation id
    of the event and of everything that answers it (done, broker state, batch of orders). None for a message that does not
    carry one. The step only grows, one per ``PhaseDriver.drive``, whatever the clock does: the clock times of a run are
    not ordered (the minute clock goes back inside a session) and several clock events can share one."""
    correlation = envelope.correlation_id

    return int(correlation) if correlation is not None and correlation.isdigit() else None


class Handled(object):
    """The ids of the messages a receiver handled in the current step, so that a copy is not handled again.

    ``first(step, msg_id)`` is True the first time a message is seen and False for a copy: a message of the step in
    progress whose id was seen, or any message of an earlier step (the lock-step moves to the next drive only when
    everything the last needed was handled, so what arrives for it later is a copy). The set is dropped when a later step
    starts: it holds the messages of one drive. A message without a step (``None``) is taken for one of the step in
    progress. Membership tests only: no iteration, deterministic."""

    def __init__(self):
        self._step = None
        self._ids = set()

    def first(self, step: int | None, msg_id: str) -> bool:
        if step is not None:
            if self._step is None or step > self._step:
                self._step = step
                self._ids = set()
            elif step < self._step:
                return False

        if msg_id in self._ids:
            return False

        self._ids.add(msg_id)

        return True


LOG = logbook.Logger('phases')


def publish_done(
        bus: Bus,
        phase: Phase,
        logical_ts: int,
        participant: str,
        orders: int = 0,
        batches: int = 0,
        error: str = '',
        *,
        answers: Envelope) -> None:
    """Says that ``participant`` finished its work of ``phase`` at ``logical_ts``, or failed (``error``). ``orders`` and
    ``batches`` are the number of orders, and of batches of orders, a runtime sent in the trade phase. ``answers`` is the
    event answered: the id of the done derives from its id, so a done published again (a retry) is the same message and the
    done of another event is not, and the done carries its step.

    A done that cannot be published (``PUBLISH_ATTEMPTS`` failures) is logged with its cause and not raised: the
    controller reports the missing done (it cannot do more than say who did not answer; the cause is here)."""
    try:
        publish_with_retry(
            bus,
            phase.done_topic,
            phases_pb2.PhaseDone(
                phase=phase.topic, participant=participant, orders=orders, batches=batches, error=error),
            msg_id=done_msg_id(participant, answers.msg_id),
            correlation_id=answers.correlation_id,
            type=message_type(phases_pb2.PhaseDone),
            logical_ts=logical_ts,
            producer_id=participant,
            phase=phase.rank + DONE_AFTER,
            key=participant)
    except PhaseError as e:
        LOG.error('{} could not say done in {}: {}', participant, phase.topic, e)


def fail(
        bus: Bus,
        phase: Phase,
        logical_ts: int,
        participant: str,
        error: Exception,
        log: logbook.Logger,
        *,
        answers: Envelope) -> None:
    """Logs ``error``, which is being handled, with its traceback and reports it in a done: the controller aborts the
    run. The failure is the answer to the event: a handler that raises would be redelivered, and a participant that
    ignores the copy (``Handled``) would never answer it again."""
    log.exception('{} failed in {}', participant, phase.topic)
    publish_done(
        bus, phase, logical_ts, participant, error='{}: {}'.format(type(error).__name__, error), answers=answers)


def report(
        bus: Bus,
        phase: Phase,
        logical_ts: int,
        participant: str,
        work: Callable[[], None],
        log: logbook.Logger,
        *,
        answers: Envelope) -> None:
    """Runs ``work()`` and says done; a failure is reported (``fail``). If ``work`` fails because a message it needs to
    publish could not be (``PhaseError``), that is the failure reported."""
    try:
        work()
    except Exception as e:
        fail(bus, phase, logical_ts, participant, e, log, answers=answers)
    else:
        publish_done(bus, phase, logical_ts, participant, answers=answers)


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
        self._step = 0  # the drives so far: it only grows, whatever the clock does (the minute clock goes back in a session)
        self._done_ids = {}  # msg_id -> content of the dones of this drive, to tell a copy from another done
        self._conflicts = []
        self._accepted = {}  # phase topic -> participant -> the done accepted, of the last drive of the phase
        self._batches = {}  # (logical_ts, process id) -> [orders, batches] of the batches of orders seen in the log
        self._batch_ids = set()  # msg_ids of the batches of orders counted and not yet announced

        bus.subscribe(DONES, self._on_done)
        bus.subscribe(ORDERS_TOPIC, self._on_orders, rank=1)

    def _on_done(self, delivery: Delivery) -> None:
        """Collects a done, once. A done is the same message however often it is delivered or published (its id derives
        from the id of the event it answers): a copy of one of this drive is ignored, a done under the id of one accepted
        but with other content is a problem. A done of an earlier drive (its step, carried in its correlation id, is
        below the driver's) is a copy: that drive ended only when every done it needed was accepted, or the run
        aborted. A done without a step is taken for one of this drive and judged by its phase and clock time."""
        envelope = delivery.envelope
        done = envelope.payload
        step = step_of(envelope)

        if step is not None and step < self._step:
            return

        known = self._done_ids.get(envelope.msg_id)
        content = done.SerializeToString(deterministic=True)

        if known is not None:
            if known != content:
                self._conflicts.append('{} said done of {} twice with different content'.format(
                    done.participant, done.phase))

            return

        self._done_ids[envelope.msg_id] = content
        self._dones.append((envelope.logical_ts, done))

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
        participant that joined late may answer); it is an error only if it is not of this phase and tick, or if the
        participant said two different dones (the same done twice is one done)."""
        bus = self._bus
        expected = tuple(expected)
        self._dones.clear()
        self._conflicts.clear()
        self._accepted.pop(phase.topic, None)
        letters = len(bus.dead_letters())
        timed_out = False
        looked = False

        self._step += 1
        step = self._step
        self._done_ids = {}

        publish_with_retry(
            bus,
            phase.topic,
            payload,
            msg_id=phase_msg_id(phase, logical_ts, key, step),
            correlation_id=str(step),
            type=message_type(payload),
            logical_ts=logical_ts,
            producer_id=self._producer_id,
            phase=phase.rank,
            key=key)

        deadline = None if self._timeout is None else time.monotonic() + self._timeout

        while True:
            bus.run_until_idle()
            problems, missing, waiting, seen = self._assess(phase, logical_ts, expected)
            problems.extend(self._conflicts)

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
        step = step_of(envelope)

        if step is not None and step != self._step:
            return  # a batch of another drive than the trade phase in progress: a copy (the manager reports a late one)

        if envelope.msg_id in self._batch_ids:
            return  # a copy of a batch already counted (a publisher's retry, a redelivery, a replay)

        self._batch_ids.add(envelope.msg_id)
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

        # the batches of this clock event are announced and counted: what comes later for it is a copy or late (the
        # manager reports a late one), and the next clock event starts from nothing
        self._batches.clear()
        self._batch_ids = set()

        return announced
