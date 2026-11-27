"""`SeededFaultBus`: a wrapper that makes a bus misbehave the way a networked one may, reproducibly (ET5, ET8).

Execution Topology ET5 and ET8: the failure paths that only exist when components run in different processes (a
message twice, a message late, a consumer that restarted, a participant that never answers) are tested in one process
and one thread, from a seed, and nothing runs distributed that never ran under this wrapper. Wrap any `Bus`
(`InMemoryBus`, `SerializingBus(InMemoryBus)`, later the networked one) and run the components over the wrapper. Every
random decision comes from one `random.Random(seed)` and is taken in the order the calls happen, so the same seed and
the same calls give the same trace under any `PYTHONHASHSEED`; `fault_log()` says what was injected, where and when.

Fault model
-----------

Time of the wrapper is the number of handler invocations so far (its "tick"); the bus has no clock (S5). Each fault is
a thing the contract of `pluto.bus` allows a networked bus to do (S1 to S14, and the broker spike of the vault: the
networked bus is an ordered log with a client-side dispatcher, so the global dispatch order S3 holds among the messages
that have *arrived* and only across processes after a barrier), the wrapper produces it with the public `Bus` calls of
the inner bus, and the consumer has to survive it:

Per fault: allowed because, how the wrapper produces it, what a correct consumer does.

* Duplicate delivery (`ack_lost`). Allowed: S7, delivery is at-least-once, an ack can be lost. Produced: after the
  handler returned, the wrapper nacks instead of acking, so the inner bus redelivers the same message (`attempt` + 1)
  before anything later of its lane. Consumer: idempotent on `msg_id`; what it publishes while handling carries ids
  derived from its input, so a repeated handling repeats the same messages.
* Late duplicate (`duplicate_publish`). Allowed: S7, the bus does not deduplicate and a publisher may retry. Produced: a
  second copy of a published message (same envelope, same `msg_id`) is published 1 to `max_delay` ticks later, so it
  arrives after later messages of its lane, to every subscriber, at `attempt` 1. Consumer: idempotent on `msg_id`, also
  for a copy that comes after later messages and after the protocol moved on (a stale message).
* Delayed arrival (`delay`, `max_delay`). Allowed: S1 promises FIFO per lane and nothing across lanes; S3 orders what
  has arrived, not what will. Produced: a publish is held for 1 to `max_delay` ticks, and every later publish to the
  same (topic, key) waits behind it; the inner bus dispatches by S3 what it has, so lanes interleave differently from
  the S3 order of the whole log. Consumer: never conclude from a dispatched message that an earlier-sorting one does
  not exist; wait for explicit evidence (a count, a done, a deadline), not for "idle".
* Lost delivery (`lose_delivery`). Allowed: S7, a delivery that was never acknowledged comes again. Produced: nack
  instead of calling the handler; the inner bus redelivers (`attempt` + 1) and after `max_attempts` dead-letters.
  Consumer: expect `attempt` above 1 on first sight; a message lost more often than the inner bus tries ends in a dead
  letter that nobody handled.
* Consumer crash and resume (`crash`, `max_crashes`, `SeededFaultBus.crash`). Allowed: S9, a consumer resumes at the
  stored `position_of`. Produced: subscribe again at the position of the last settled delivery (before any delivery:
  the first message logged after the subscription), either before the handler ran or after it ran and before the ack
  (one in two each). Only for exact, ungrouped topics: a position is per topic. Consumer: checkpoint with
  `position_of`; after a restart everything above the checkpoint comes again, also what it handled, so it is
  idempotent or rebuilds its state from the checkpoint. The id of the subscription changes (a new registration).
* Publish failure (`publish_fail`). Allowed: a client may see an error when nothing was sent. Produced: `PublishFailed`
  after the arguments were checked, before the inner bus saw the message (nothing logged, no `seq` used). Consumer
  (publisher): retry with the same `msg_id` (`pluto.bus.request.Request.send` does).
* Ambiguous publish (`publish_ack_lost`). Allowed: the ack was lost, the publisher cannot know. Produced: the message is
  published normally (it may be delayed), then `PublishFailed` is raised. Publisher: retry with the same `msg_id`; the
  retry is a duplicate to every consumer.
* Silent participant (`mute`, `SeededFaultBus.mute`). Allowed: a process that hung or died never acknowledges. Produced:
  the subscription is cancelled with the delivery in hand unsettled, so nothing reaches its handler, nothing is
  acknowledged, and what is published meanwhile waits in the log. `unmute` restarts it at its checkpoint like a crash
  (exact, ungrouped topics: it reads again what it had not settled); on other subscriptions a mute is permanent and
  `unmute` raises. Waiter: needs a deadline; no answer is a failure to report, not a state of the lock-step.

Contract-violating (negative tests only; need `FaultConfig(contract_violating=True)`):

* `reorder_within_lane`: a published message jumps ahead of the held message before it in the same lane, so that the
  inner bus sees them in the wrong order. S1 forbids it; a consumer that works only under this fault relied on
  something the bus does not give, and the FIFO checks of the tests must fail under it.

Not modelled (a networked bus does these, the in-process wrapper does not): loss of an accepted message without a
trace, corruption, per-subscriber lag (all subscribers of a message see it arrive at the same tick), a consumer that
loses its checkpoint, partitions that split one subscription's lanes in time. Also not modelled, and it matters for
what `lose_delivery` and `ack_lost` prove: a *late* redelivery. A nack keeps the dispatch key of the message, so the
redelivery always comes next in its lane, before any later message of any other lane; a real redelivery after an ack
timeout arrives after the other lanes moved on. A consumer that survives these faults has survived duplicates and
attempts above 1, not a redelivery out of order across lanes (`delay` and `duplicate_publish` cover that for
duplicates and arrivals, not for redeliveries). Group-member failover (a member dies and its lanes go to another, with
the unacked message redelivered there) is not modelled either: grouped subscriptions are never crashed, and a muted
member is cancelled, so the group's lanes move to the others at once.

Rules of the wrapper:

* Every call draws a fixed number of random numbers whatever the configuration (7 per publish, 5 per delivery), and
  uses them only where a fault is on: two configurations that differ in one probability take the same stream, so one
  fault can be dialled up or down while the rest of a scenario stays the same.
* Delayed and duplicated publishes live in the wrapper until they are released; the returned `Receipt` of a delayed
  one is unresolved: asking for its `position` puts everything held into the log (a barrier, like `subscribe`) and
  returns the inner bus's position (`BusError` if the message was dropped on release).
* `subscribe` is a barrier: everything held is first put into the inner bus's log (a message published before a
  subscription started is never delivered to it at `Start.LATEST`, as without faults). Every `Request` subscribes to
  its reply topic, so every request is a barrier too, and the delays of earlier publishes end there; with the inner
  bus full the barrier can raise `QueueFull` where the bare inner bus would not.
* `step` and `run_until_idle` first release what is due, and when the inner bus is idle they skip ahead in time to
  what is held, so a delayed message is always eventually delivered and "idle" means nothing is pending anywhere.
  Then nothing can arrive after `run_until_idle` returned, so `networked` is that of the inner bus (False over
  `InMemoryBus`) and `PhaseDriver` treats a missing done as the immediate error it is.
* `FaultConfig(release_on_poll=True)` is the other mode: `step` never skips ahead, `run_until_idle` returns when the
  inner bus is idle although messages are held, and they arrive only when the caller `poll`s (a poll skips ahead to the
  next held message and releases what is due). The bus is then `networked` (True), because messages can arrive after
  idle: `PhaseDriver` waits for the expected dones with `poll`, as for a participant in another process. This is the
  mode that tests what a driver that waits does with a late done; over an in-memory inner bus it is the only way
  to make a delay outlive a `run_until_idle`.
* The wrapper owns `seq` (per producer, from 1, like the in-memory bus) so that the receipt of a held message has its
  final `msg_id`.
* Injected redeliveries (`ack_lost`, `lose_delivery`) happen at most `max_redeliveries` times per message and
  subscription (also across crashes), and count as attempts of the inner bus: keep `max_attempts` of the inner bus
  above `max_redeliveries` plus the failures the handlers produce themselves, or a message is dead-lettered by the
  injection.
* A held message that the inner bus then refuses (other than `QueueFull`, which makes it wait) is dropped and the
  error is raised from the `step` that released it: with delays on, argument errors of a message the wrapper could not
  check itself (a payload the codec cannot encode) surface late.
* A crash or an unmute subscribes the replacement before it cancels the old subscription; if the replay does not fit
  (`QueueFull`) the crash is skipped and logged (`CRASH_SKIPPED`), the unmute raises.
"""

import enum
import random
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from heapq import heappop, heappush
from typing import Any, NamedTuple

from pluto.bus.api import (
    Bus,
    BusClosed,
    BusError,
    DeadLetter,
    Delivery,
    Handler,
    Position,
    QueueFull,
    Receipt,
    Start,
    Subscription,
    exact_topic,
    is_exact,
    topic_tokens,
)


class PublishFailed(BusError):
    """The publisher sees a failed publish (injected). Retry with the same `msg_id`: the message may or may not have
    reached the bus."""


class FaultKind(enum.StrEnum):
    PUBLISH_FAILED = "publish_failed"  # nothing reached the bus
    PUBLISH_ACK_LOST = "publish_ack_lost"  # reached the bus, the publisher saw an error
    DELAYED = "delayed"  # a publish held for some ticks
    QUEUED_BEHIND = "queued_behind"  # a publish held only because an earlier message of its lane is held
    DUPLICATE_PUBLISH = "duplicate_publish"  # a second copy scheduled
    REORDERED = "reordered"  # contract-violating: jumped ahead of the previous message of its lane
    LOST_DELIVERY = "lost_delivery"  # the handler was not called, the delivery will come again
    DUPLICATE_DELIVERY = "duplicate_delivery"  # the handler ran, the ack was lost, the delivery will come again
    CRASH = "crash"  # the subscription restarted from its checkpoint
    CRASH_SKIPPED = "crash_skipped"  # a crash that did not happen: the replay would not fit in the inner bus
    MUTED = "muted"  # the subscription went silent: cancelled, its delivery in hand never acknowledged
    UNMUTED = "unmuted"  # a muted subscription restarted at its checkpoint
    RELEASE_FAILED = "release_failed"  # a held message the inner bus refused, dropped


class Fault(NamedTuple):
    """One injected fault. `tick` is the time of the wrapper (handler invocations so far); `subscription` the id of
    the subscription at that time, None for publish faults."""

    index: int
    tick: int
    kind: FaultKind
    topic: str
    key: str
    msg_id: str
    subscription: int | None
    detail: str


def _probability(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int | float) or not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be a probability between 0 and 1, got {value!r}")


@dataclass(frozen=True, slots=True)
class FaultConfig:
    """What to inject; every probability is per decision point (per publish, per delivery) and 0 turns the fault off.
    The default injects nothing: the wrapper is then transparent."""

    publish_fail: float = 0.0
    publish_ack_lost: float = 0.0
    delay: float = 0.0
    duplicate_publish: float = 0.0
    max_delay: int = 8  # ticks (handler invocations): delays and late duplicates are drawn from 1 to this
    lose_delivery: float = 0.0
    ack_lost: float = 0.0
    max_redeliveries: int = 1  # injected redeliveries per message and subscription
    crash: float = 0.0
    max_crashes: int = 3  # per subscription
    mute: float = 0.0
    reorder_within_lane: float = 0.0  # contract-violating
    contract_violating: bool = False  # must be True to use the contract-violating faults
    release_on_poll: bool = False  # held messages arrive only when the caller polls: the bus is `networked`

    def __post_init__(self) -> None:
        for name in (
            "publish_fail",
            "publish_ack_lost",
            "delay",
            "duplicate_publish",
            "lose_delivery",
            "ack_lost",
            "crash",
            "mute",
            "reorder_within_lane",
        ):
            _probability(name, getattr(self, name))
        for name in ("max_delay", "max_redeliveries", "max_crashes"):
            value = getattr(self, name)
            if value.__class__ is not int or value < (1 if name == "max_delay" else 0):
                raise ValueError(f"{name} must be an int of at least {1 if name == 'max_delay' else 0}, got {value!r}")
        if self.reorder_within_lane and not self.contract_violating:
            raise ValueError("reorder_within_lane violates the contract: set contract_violating=True to use it")
        if self.release_on_poll.__class__ is not bool:
            raise ValueError(f"release_on_poll must be a bool, got {self.release_on_poll!r}")


class _Pending:
    """What a receipt of a held message waits for: the inner bus's receipt, once the message reached its log."""

    __slots__ = ("receipt",)

    def __init__(self) -> None:
        self.receipt: Receipt | None = None


class _Item:
    """A message the wrapper holds: the arguments of the inner `publish`, the tick it may go, whose position waits."""

    __slots__ = ("args", "due", "pending")

    def __init__(self, args: dict[str, Any], due: int, pending: _Pending | None) -> None:
        self.args = args
        self.due = due
        self.pending = pending


class _Handle:
    """The subscription the caller holds. The inner subscription behind it is replaced when it crashes."""

    __slots__ = (
        "serial",
        "topic",
        "handler",
        "rank",
        "exact",
        "callback",
        "inner",
        "checkpoint",
        "crashes",
        "muted",
        "cancelled",
    )

    def __init__(self, serial: int, topic: str, handler: Handler, rank: int) -> None:
        self.serial = serial
        self.topic = topic
        self.handler = handler
        self.rank = rank
        self.exact = False  # an exact topic without a group: a crash can resume it from a position
        self.callback: Handler
        self.inner: Subscription
        # where a restart resumes: a position, `Start.EARLIEST`, or the receipt of the first message logged after the
        # subscription started at the end of the log; None while none of these is known
        self.checkpoint: Position | Start | Receipt | None = None
        self.crashes = 0
        self.muted = False
        self.cancelled = False

    @property
    def id(self) -> int:
        """The id of the inner subscription (it changes when the subscription crashes and registers again)."""
        return self.inner.id

    def cancel(self) -> None:
        self.cancelled = True
        self.inner.cancel()


def _validate(topic: object, logical_ts: object, phase: object, seq: object, key: object, producer_id: object) -> None:
    """The argument checks of `InMemoryBus.publish`, for a message the wrapper holds instead of publishing."""
    for name, value in (("logical_ts", logical_ts), ("phase", phase), ("seq", seq)):
        if value.__class__ is not int:
            raise TypeError(f"{name} must be an int, got {value.__class__.__name__}")
    if key.__class__ is not str or producer_id.__class__ is not str:
        raise TypeError("key and producer_id must be str")
    exact_topic(topic)  # type: ignore[arg-type]


class SeededFaultBus:
    """A `Bus` over another `Bus` that injects the faults of `FaultConfig` from a seed. See the module docstring."""

    def __init__(self, inner: Bus, seed: int, faults: FaultConfig | None = None) -> None:
        if seed.__class__ is not int:
            raise TypeError(f"seed must be an int, got {seed.__class__.__name__}")
        self._inner = inner
        self._rng = random.Random(seed)
        self._cfg = FaultConfig() if faults is None else faults
        self._closed = False
        self._in_handler = False
        self._outcome_nacked = False  # the last explicit ack or nack of the delivery being handled
        self._now = 0  # handler invocations so far
        self._seq: dict[str, int] = {}
        self._held: dict[tuple[str, str], deque[_Item]] = {}  # lane -> messages waiting, in lane order
        self._dups: list[tuple[int, int, dict[str, Any]]] = []  # heap of (due tick, counter, publish arguments)
        self._dup_count = 0
        self._spent: dict[tuple[int, str, int], int] = {}  # (subscription serial, topic, offset) -> redeliveries
        self._subscriptions: list[_Handle] = []
        self._waiting: dict[str, list[_Handle]] = {}  # topic -> handles that resume at the next message logged there
        self._log: list[Fault] = []

    # diagnosis

    def fault_log(self) -> tuple[Fault, ...]:
        """Every injected fault so far, in order."""
        return tuple(self._log)

    def in_flight(self) -> int:
        """The messages and late duplicates the wrapper still holds, not yet in the inner bus's log."""
        return sum(len(queue) for queue in self._held.values()) + len(self._dups)

    def mute(self, subscription: Subscription) -> None:
        """Make a subscription silent from now on, like a hung or dead process: it is cancelled, nothing reaches its
        handler, nothing is acknowledged, and what is published meanwhile waits in the log."""
        handle = self._handle(subscription)
        if not handle.muted and not handle.cancelled:
            handle.muted = True
            self._note(FaultKind.MUTED, handle.topic, "", "", handle.id, "muted by the caller")
            handle.inner.cancel()

    def unmute(self, subscription: Subscription) -> None:
        """Let a muted subscription answer again: it restarts at its checkpoint like a crashed consumer and reads again
        what it had not settled, including what was published while it was silent. Only for an exact topic without a
        group; on any other a mute is permanent and this raises `ValueError`. `QueueFull` if the replay does not fit."""
        handle = self._handle(subscription)
        if not handle.muted or handle.cancelled:
            return
        if not handle.exact:
            raise ValueError("only a subscription to an exact topic without a group can resume: its mute is permanent")
        if not self._restart(handle):
            raise QueueFull("the replay of a muted subscription does not fit in the inner bus")
        handle.muted = False
        self._note(FaultKind.UNMUTED, handle.topic, "", "", handle.id, f"resumes at {handle.checkpoint}")

    def crash(self, subscription: Subscription) -> None:
        """Crash a subscription now: it restarts at its checkpoint (an exact topic without a group). Called from its own
        handler it is a crash before the ack. `QueueFull` if the replay does not fit."""
        handle = self._handle(subscription)
        if not handle.exact or handle.muted or handle.cancelled:
            raise ValueError("only a live subscription to an exact topic without a group can crash and resume")
        if not self._restart(handle):
            raise QueueFull("the replay of a crashed subscription does not fit in the inner bus")
        handle.crashes += 1
        self._note(FaultKind.CRASH, handle.topic, "", "", handle.id, f"by the caller; resumes at {handle.checkpoint}")

    def _handle(self, subscription: Subscription) -> _Handle:
        if not isinstance(subscription, _Handle) or subscription not in self._subscriptions:
            raise ValueError("not a subscription of this bus")
        return subscription

    def _note(
        self,
        kind: FaultKind,
        topic: str,
        key: str,
        msg_id: str,
        subscription: int | None = None,
        detail: str = "",
    ) -> None:
        self._log.append(Fault(len(self._log), self._now, kind, topic, key, msg_id, subscription, detail))

    # publish

    def publish(
        self,
        topic: str,
        payload: Any,
        *,
        type: str,
        logical_ts: int,
        producer_id: str,
        phase: int = 0,
        key: str = "",
        seq: int | None = None,
        msg_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        schema_version: int = 1,
    ) -> Receipt:
        self._check_open()
        cfg = self._cfg
        rng = self._rng
        # every call draws the same numbers whatever the configuration, so that two configurations that differ in one
        # probability take the same stream; the amounts are used only when the fault is on
        fail = rng.random() < cfg.publish_fail
        ack_lost = rng.random() < cfg.publish_ack_lost
        hold = rng.random() < cfg.delay
        duplicate = rng.random() < cfg.duplicate_publish
        jump = rng.random() < cfg.reorder_within_lane
        delay = rng.randint(1, cfg.max_delay)
        late = rng.randint(1, cfg.max_delay)

        if fail:
            _validate(topic, logical_ts, phase, 1 if seq is None else seq, key, producer_id)
            self._note(FaultKind.PUBLISH_FAILED, topic, key, "" if msg_id is None else msg_id)
            raise PublishFailed(f"publish to {topic!r} failed (injected)")

        assigned = self._seq.get(producer_id, 0) + 1 if seq is None else seq
        final_id = f"{producer_id}:{assigned}" if msg_id is None else msg_id
        args = {
            "topic": topic,
            "payload": payload,
            "type": type,
            "logical_ts": logical_ts,
            "producer_id": producer_id,
            "phase": phase,
            "key": key,
            "seq": assigned,
            "msg_id": final_id,
            "correlation_id": correlation_id,
            "reply_to": reply_to,
            "schema_version": schema_version,
        }
        lane = (topic, key)
        queue = self._held.get(lane)
        if not hold and not queue:
            receipt = self._inner.publish(**args)  # a refusal leaves everything as it was
            self._logged(topic, receipt)
            if seq is None:
                self._seq[producer_id] = assigned
        else:
            _validate(topic, logical_ts, phase, assigned, key, producer_id)
            if seq is None:
                self._seq[producer_id] = assigned
            pending = _Pending()
            item = _Item(args, self._now + delay, pending)
            if queue is None:
                queue = self._held[lane] = deque()
            if hold:
                self._note(FaultKind.DELAYED, topic, key, final_id, detail=f"until tick {item.due}")
            else:
                detail = "an earlier message of the lane is held"
                self._note(FaultKind.QUEUED_BEHIND, topic, key, final_id, detail=detail)
            if jump and queue:
                queue.insert(len(queue) - 1, item)
                detail = "ahead of the previous message of the lane"
                self._note(FaultKind.REORDERED, topic, key, final_id, detail=detail)
            else:
                queue.append(item)
            receipt = Receipt(final_id, self._resolver(pending))
        if duplicate:
            self._dup_count += 1
            heappush(self._dups, (self._now + late, self._dup_count, args))
            self._note(FaultKind.DUPLICATE_PUBLISH, topic, key, final_id, detail=f"copy due at tick {self._now + late}")
        if ack_lost:
            self._note(FaultKind.PUBLISH_ACK_LOST, topic, key, final_id)
            raise PublishFailed(f"publish to {topic!r} failed after the bus took it (injected)")
        return receipt

    def _release(self) -> int:
        """Move the late duplicates that are due into their lanes, then publish the head of every lane whose time has
        come, in lane order. Returns how many things moved; 0 with items left means the inner bus refused them all."""
        moved = 0
        dups = self._dups
        while dups and dups[0][0] <= self._now:
            args = heappop(dups)[2]
            self._held.setdefault((args["topic"], args["key"]), deque()).append(_Item(args, self._now, None))
            moved += 1
        for lane in list(self._held):
            queue = self._held[lane]
            while queue and queue[0].due <= self._now:
                item = queue[0]
                try:
                    receipt = self._inner.publish(**item.args)
                except QueueFull:
                    break  # waits until the receivers drained
                except Exception as error:
                    queue.popleft()
                    if not queue:
                        del self._held[lane]
                    args = item.args
                    self._note(FaultKind.RELEASE_FAILED, args["topic"], args["key"], args["msg_id"], detail=repr(error))
                    raise
                queue.popleft()
                moved += 1
                self._logged(item.args["topic"], receipt)
                if item.pending is not None:
                    item.pending.receipt = receipt
            if not queue:
                del self._held[lane]
        return moved

    def _resolver(self, pending: _Pending) -> Callable[[], Position]:
        """What `Receipt.position` calls for a held message: put everything held into the log (like a subscription, it
        is a barrier), then the position the inner bus gave it. `BusError` if the message was dropped on release."""

        def resolve() -> Position:
            self._flush()
            if pending.receipt is None:
                raise BusError("the message of this receipt was dropped before it reached the log")
            return pending.receipt.position

        return resolve

    def _next_due(self) -> int:
        due = [queue[0].due for queue in self._held.values()]
        if self._dups:
            due.append(self._dups[0][0])
        return min(due)

    def _flush(self) -> None:
        """Put everything held into the inner bus, skipping ahead in time."""
        while self._held or self._dups:
            self._now = max(self._now, self._next_due())
            if not self._release():
                raise QueueFull("the messages the wrapper holds cannot be released: a receiver is full")

    # subscribe

    def subscribe(
        self,
        topic_or_pattern: str,
        handler: Handler,
        *,
        group: str | None = None,
        start: Start | Position | None = None,
        rank: int = 0,
    ) -> Subscription:
        self._check_open()
        self._flush()  # what was published before the subscription is in the log before it starts
        handle = _Handle(len(self._subscriptions) + 1, topic_or_pattern, handler, rank)
        handle.callback = lambda delivery: self._deliver(handle, delivery)
        handle.inner = self._inner.subscribe(topic_or_pattern, handle.callback, group=group, start=start, rank=rank)
        handle.exact = group is None and is_exact(topic_tokens(topic_or_pattern))
        if handle.exact:
            if isinstance(start, Position) or start is Start.EARLIEST:
                handle.checkpoint = start
            else:  # a restart resumes at the first message logged from now on
                self._waiting.setdefault(topic_or_pattern, []).append(handle)
        self._subscriptions.append(handle)
        return handle

    def _logged(self, topic: str, receipt: Receipt) -> None:
        """A message reached the inner bus's log: where a subscription that started at the end of the log resumes. The
        receipt stays unresolved until a restart needs its position (a networked bus does not wait in `publish`)."""
        for handle in self._waiting.pop(topic, ()):
            if handle.checkpoint is None:
                handle.checkpoint = receipt

    # delivery

    def _deliver(self, handle: _Handle, delivery: Delivery) -> None:
        """The handler the inner bus calls: decides, from the seed, what the real handler gets to see."""
        cfg = self._cfg
        rng = self._rng
        mute = rng.random() < cfg.mute
        lose = rng.random() < cfg.lose_delivery
        crash = rng.random() < cfg.crash
        ack_lost = rng.random() < cfg.ack_lost
        before = rng.random() < 0.5  # a crash comes before the handler ran, or after it and before the ack
        envelope = delivery.envelope
        topic, key, msg_id = envelope.topic, envelope.key, envelope.msg_id
        inner = handle.inner
        self._in_handler = True
        try:
            if mute:
                self._silence(handle, topic, key, msg_id, f"the delivery of attempt {delivery.attempt} is never acked")
                return
            ident = (handle.serial, topic, delivery.offset)
            spent = self._spent.get(ident, 0)
            may_repeat = spent < cfg.max_redeliveries
            if lose and may_repeat:
                self._spent[ident] = spent + 1
                detail = f"attempt {delivery.attempt} never reached the handler"
                self._note(FaultKind.LOST_DELIVERY, topic, key, msg_id, handle.id, detail)
                self._inner.nack(delivery)
                return
            crash = crash and handle.exact and handle.crashes < cfg.max_crashes and handle.checkpoint is not None
            if crash and before and self._crash(handle, envelope, "before the handler ran"):
                return
            self._outcome_nacked = False
            handle.handler(delivery)
            if self._closed or handle.cancelled or handle.muted or handle.inner is not inner or self._outcome_nacked:
                return  # the handler closed, cancelled, muted or restarted the subscription, or nacked
            if crash and not before and self._crash(handle, envelope, "after the handler ran, before the ack"):
                return
            if ack_lost and may_repeat:
                self._spent[ident] = spent + 1
                detail = f"the ack of attempt {delivery.attempt} was lost"
                self._note(FaultKind.DUPLICATE_DELIVERY, topic, key, msg_id, handle.id, detail)
                self._inner.nack(delivery)
                return
            if handle.exact:
                handle.checkpoint = self._inner.position_of(delivery)
        finally:
            self._in_handler = False

    def _resume_point(self, handle: _Handle) -> Position | Start | None:
        point = handle.checkpoint
        return point.position if isinstance(point, Receipt) else point

    def _restart(self, handle: _Handle) -> bool:
        """Subscribe a replacement at the checkpoint, then cancel the old subscription. False when the replay does not
        fit in the inner bus (`QueueFull`): nothing changed then."""
        try:
            replacement = self._inner.subscribe(
                handle.topic, handle.callback, start=self._resume_point(handle), rank=handle.rank
            )
        except QueueFull:
            return False
        handle.inner.cancel()
        handle.inner = replacement
        return True

    def _crash(self, handle: _Handle, envelope: Any, when: str) -> bool:
        """Restart the subscription at its checkpoint; False (and logged) when the replay does not fit."""
        topic, key, msg_id = envelope.topic, envelope.key, envelope.msg_id
        detail = f"{when}; resumes at {handle.checkpoint}"
        if not self._restart(handle):
            self._note(FaultKind.CRASH_SKIPPED, topic, key, msg_id, handle.id, detail + ": the replay does not fit")
            return False
        handle.crashes += 1
        self._note(FaultKind.CRASH, topic, key, msg_id, handle.id, detail)
        return True

    def _silence(self, handle: _Handle, topic: str, key: str, msg_id: str, detail: str) -> None:
        handle.muted = True
        self._note(FaultKind.MUTED, topic, key, msg_id, handle.id, detail)
        handle.inner.cancel()

    def ack(self, delivery: Delivery) -> None:
        self._inner.ack(delivery)
        self._outcome_nacked = False

    def nack(self, delivery: Delivery) -> None:
        self._inner.nack(delivery)
        self._outcome_nacked = True

    def position_of(self, delivery: Delivery) -> Position:
        return self._inner.position_of(delivery)

    # dispatch

    @property
    def networked(self) -> bool:
        """True when a message can arrive after `run_until_idle` returned: when the inner bus is networked, or with
        `FaultConfig.release_on_poll`, where held messages arrive only when the caller polls (the driver then waits
        for them, as for a remote participant). Otherwise `run_until_idle` skips ahead in time until nothing is held,
        so when it returns nothing else can arrive, which is what `networked` False tells `PhaseDriver`: a missing
        done is then an immediate error, and it is one."""
        return self._cfg.release_on_poll or self._inner.networked

    def poll(self, timeout: float = 0.0) -> bool:
        """Release what is held that is due (with `release_on_poll`: skip ahead in time to the next held message and
        release what is due then), then poll the inner bus. True when either moved a message into a queue. Never runs a
        handler. Not networked: False, at once, nothing to read. With `release_on_poll` over an inner bus that cannot
        wait, a poll that finds nothing sleeps `timeout` seconds, as a poll of a real broker waits for a message that
        does not come: the caller's deadline is wall-clock and would otherwise be a busy loop."""
        self._check_open()
        if not self.networked:
            return False
        if self._cfg.release_on_poll and (self._held or self._dups):
            self._now = max(self._now, self._next_due())
        moved = self._release() > 0
        found = self._inner.poll(0.0 if moved else timeout) or moved
        if not found and timeout > 0 and not self._inner.networked:
            time.sleep(timeout)
        return found

    def step(self) -> bool:
        self._check_open()
        if self._in_handler:
            raise BusError("step and run_until_idle cannot be called from a handler: no nested delivery")
        while True:
            moved = self._release()
            if self._inner.step():
                self._now += 1
                return True
            if self._cfg.release_on_poll or (not self._held and not self._dups):
                return False  # what is held arrives when the caller polls
            due = self._next_due()
            if due > self._now:
                self._now = due
            elif not moved:
                raise QueueFull("the messages the wrapper holds cannot be released: a receiver is full")

    def run_until_idle(self, max_deliveries: int | None = None) -> int:
        self._check_open()
        if self._in_handler:
            raise BusError("step and run_until_idle cannot be called from a handler: no nested delivery")
        if max_deliveries is not None and (max_deliveries.__class__ is not int or max_deliveries < 0):
            raise ValueError(f"max_deliveries must be a non-negative int or None, got {max_deliveries!r}")
        count = 0
        while True:
            if max_deliveries is not None and count >= max_deliveries:
                if not self._cfg.release_on_poll:
                    self._flush()
                self._inner.run_until_idle(0)  # raises NotIdle when something is pending
                return count
            if not self.step():
                return count
            count += 1

    def dead_letters(self) -> tuple[DeadLetter, ...]:
        return self._inner.dead_letters()

    def close(self) -> None:
        self._closed = True
        self._held.clear()
        self._dups.clear()
        self._inner.close()

    def _check_open(self) -> None:
        if self._closed:
            raise BusClosed("the bus is closed")
