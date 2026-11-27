"""The deterministic in-memory bus: one thread, objects passed by reference."""

import zlib
from collections import deque
from heapq import heappop, heappush
from typing import Any

from pluto.bus.api import (
    BusClosed,
    BusError,
    DeadLetter,
    Delivery,
    Envelope,
    Handler,
    NotIdle,
    Position,
    QueueFull,
    Receipt,
    Start,
    Subscription,
    exact_topic,
    topic_tokens,
)


def _is_int(value: object) -> bool:
    return value.__class__ is int  # not bool, not numpy integers: they would reach the heap key


def _is_str(value: object) -> bool:
    return value.__class__ is str


def _matches(pattern: tuple[str, ...], topic: tuple[str, ...]) -> bool:
    if pattern[-1] == ">":
        head = pattern[:-1]
        return len(topic) > len(head) and all(p in ("*", t) for p, t in zip(head, topic))
    return len(pattern) == len(topic) and all(p in ("*", t) for p, t in zip(pattern, topic))


def _check_fields(logical_ts: object, phase: object, seq: object, key: object, producer_id: object) -> None:
    for name, value in (("logical_ts", logical_ts), ("phase", phase), ("seq", seq)):
        if not _is_int(value) and not (name == "seq" and value is None):
            raise TypeError(f"{name} must be an int, got {value.__class__.__name__}")
    if not _is_str(key) or not _is_str(producer_id):
        raise TypeError("key and producer_id must be str")


class _Subscription:
    __slots__ = ("_bus", "id", "handler", "rank", "consumer", "cancelled")

    def __init__(self, bus: "InMemoryBus", id: int, handler: Handler, rank: int) -> None:
        self._bus = bus
        self.id = id
        self.handler = handler
        self.rank = rank
        self.consumer: _Consumer
        self.cancelled = False

    def cancel(self) -> None:
        self._bus._cancel(self)


class _Cursor:
    """What a consumer has settled in one topic: every offset below `frontier`, plus the offsets in `done`."""

    __slots__ = ("frontier", "done")

    def __init__(self, first_offset: int) -> None:
        self.frontier = first_offset
        self.done: set[int] = set()  # membership tests only, never iterated

    def settle(self, offset: int) -> None:
        if offset != self.frontier:
            self.done.add(offset)
            return
        frontier = offset + 1
        while frontier in self.done:
            self.done.remove(frontier)
            frontier += 1
        self.frontier = frontier

    def after(self, offset: int) -> int:
        """The frontier if `offset` were settled too."""
        frontier = self.frontier
        while frontier == offset or frontier in self.done:
            frontier += 1
        return frontier


class _Consumer:
    """What reads a stream: one subscription, or all members of a group. Holds the lanes and the queue count."""

    __slots__ = ("pattern", "tokens", "group", "members", "lanes", "cursors", "pending", "closed")

    def __init__(self, pattern: str, tokens: tuple[str, ...], group: str | None) -> None:
        self.pattern = pattern
        self.tokens = tokens
        self.group = group
        self.members: list[_Subscription] = []
        self.lanes: dict[tuple[str, str], _Lane] = {}
        self.cursors: dict[str, _Cursor] = {}
        self.pending = 0  # queued, not yet settled
        self.closed = False

    def member_for(self, topic: str, key: str) -> _Subscription:
        """The member handling a lane: by a stable hash of its content, so it does not depend on creation order."""
        members = self.members
        if len(members) == 1:
            return members[0]
        return members[zlib.crc32(f"{topic}\0{key}".encode()) % len(members)]


class _Lane:
    """The queued messages of one (topic, key) for one consumer, FIFO, handled by one member."""

    __slots__ = ("consumer", "owner", "topic", "key", "cursor", "queue", "attempts", "stamp", "inflight")

    def __init__(self, consumer: _Consumer, owner: _Subscription, topic: str, key: str, cursor: _Cursor) -> None:
        self.consumer = consumer
        self.owner = owner
        self.topic = topic
        self.key = key
        self.cursor = cursor
        self.queue: deque[tuple[int, Envelope]] = deque()  # (log offset, envelope); the head stays until settled
        self.attempts = 0  # failed deliveries of the head
        self.stamp = 0  # bumped whenever the heap entry of the lane becomes invalid
        self.inflight = False


class InMemoryBus:
    """Deterministic single-thread bus. See the docstring of `pluto.bus` for the rules.

    Dispatch: each consumer keeps one FIFO lane per (topic, key). The heap holds the head of every non-empty lane
    that is not being handled, ordered by `(logical_ts, phase, producer_id, seq, rank, subscription id, topic,
    key)`; `step` takes the smallest. Everything in that key is message content or registration order.

    Overflow policy: a subscription holds at most `queue_capacity` undelivered messages (replay included). A
    `publish` that would exceed it for any receiving subscription raises `QueueFull` to the publisher and changes
    nothing; the publisher drains with `run_until_idle` or fails loudly. It cannot block (a single thread would
    wait for itself) and it does not drop (silent loss would change results). A handler that publishes into a
    full queue gets the `QueueFull`; unless it handles it, the delivery is nacked and, because a redelivery keeps
    its dispatch key and runs before the receiver drains, normally dead-lettered after `max_attempts`. Size
    `queue_capacity` for the largest burst of one phase.

    Lanes are never freed: use keys of low cardinality (strategy or instrument ids). Logs are never trimmed
    (retention is open).
    """

    def __init__(self, *, queue_capacity: int = 100_000, max_attempts: int = 3) -> None:
        if not _is_int(queue_capacity) or not _is_int(max_attempts) or queue_capacity < 1 or max_attempts < 1:
            raise ValueError("queue_capacity and max_attempts must be ints of at least 1")
        self._capacity = queue_capacity
        self._max_attempts = max_attempts
        self._closed = False
        self._logs: dict[str, list[Envelope]] = {}
        self._seq: dict[str, int] = {}
        self._consumers: list[_Consumer] = []  # registration order
        self._groups: dict[str, _Consumer] = {}
        self._subscriptions: dict[int, _Subscription] = {}
        self._routes: dict[str, tuple[_Consumer, ...]] = {}  # topic -> consumers, rebuilt after (un)subscribing
        self._next_subscription = 1
        self._heap: list[tuple[Any, ...]] = []
        self._current: Delivery | None = None
        self._nacked = False
        self._dead: list[DeadLetter] = []

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
        if self._closed:
            raise BusClosed("the bus is closed")
        _check_fields(logical_ts, phase, seq, key, producer_id)
        log = self._logs.get(topic)
        if log is None:
            exact_topic(topic)
        consumers = self._routes.get(topic)
        if consumers is None:
            consumers = self._build_route(topic)
        for consumer in consumers:
            if consumer.pending >= self._capacity:
                raise QueueFull(f"subscription queue for {consumer.pattern!r} is full ({self._capacity})")
        # nothing can be refused from here on: only now does the bus change
        if log is None:
            log = self._logs[topic] = []
        if seq is None:
            seq = self._seq[producer_id] = self._seq.get(producer_id, 0) + 1
        if msg_id is None:
            msg_id = f"{producer_id}:{seq}"
        envelope = Envelope(
            msg_id,
            topic,
            key,
            type,
            logical_ts,
            phase,
            producer_id,
            seq,
            correlation_id,
            reply_to,
            schema_version,
            payload,
        )
        offset = len(log)
        log.append(envelope)
        for consumer in consumers:
            self._enqueue(consumer, offset, envelope)
        return Receipt(msg_id, Position(topic, offset))

    def _build_route(self, topic: str) -> tuple[_Consumer, ...]:
        tokens = tuple(topic.split("."))
        consumers = self._routes[topic] = tuple(c for c in self._consumers if _matches(c.tokens, tokens))
        return consumers

    def _enqueue(self, consumer: _Consumer, offset: int, envelope: Envelope) -> None:
        lane = consumer.lanes.get((envelope.topic, envelope.key))
        if lane is None:
            lane = self._new_lane(consumer, offset, envelope.topic, envelope.key)
        was_empty = not lane.queue
        lane.queue.append((offset, envelope))
        consumer.pending += 1
        if was_empty:
            self._push(lane)

    def _new_lane(self, consumer: _Consumer, offset: int, topic: str, key: str) -> _Lane:
        cursor = consumer.cursors.get(topic)
        if cursor is None:
            cursor = consumer.cursors[topic] = _Cursor(offset)  # the first message this consumer gets of the topic
        lane = consumer.lanes[(topic, key)] = _Lane(consumer, consumer.member_for(topic, key), topic, key, cursor)
        return lane

    def _push(self, lane: _Lane) -> None:
        envelope = lane.queue[0][1]
        owner = lane.owner
        heappush(
            self._heap,
            (
                envelope.logical_ts,
                envelope.phase,
                envelope.producer_id,
                envelope.seq,
                owner.rank,
                owner.id,
                envelope.topic,
                envelope.key,
                lane.stamp,
                lane,
            ),
        )

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
        tokens = topic_tokens(topic_or_pattern)
        if not _is_int(rank):
            raise TypeError(f"rank must be an int, got {rank.__class__.__name__}")
        if group is not None and not _is_str(group):
            raise TypeError("group must be a str or None")
        subscription = _Subscription(self, self._next_subscription, handler, rank)
        consumer = None if group is None else self._groups.get(group)
        if consumer is not None:
            if consumer.pattern != topic_or_pattern or not (start is None or start is Start.LATEST):
                raise ValueError(f"group {group!r} reads {consumer.pattern!r}; members join it without `start`")
            self._next_subscription += 1
            self._subscriptions[subscription.id] = subscription
            subscription.consumer = consumer
            consumer.members.append(subscription)
            return subscription
        sources = self._replay_sources(topic_or_pattern, tokens, start)
        replayed = sum(len(self._logs[topic]) - first for topic, first in sources)
        if replayed > self._capacity:
            raise QueueFull(f"replay of {replayed} messages exceeds the queue capacity {self._capacity}")
        self._next_subscription += 1
        self._subscriptions[subscription.id] = subscription
        consumer = _Consumer(topic_or_pattern, tokens, group)
        consumer.members.append(subscription)
        subscription.consumer = consumer
        self._consumers.append(consumer)
        self._routes.clear()
        if group is not None:
            self._groups[group] = consumer
        for topic, first in sources:
            log = self._logs[topic]
            for offset in range(first, len(log)):
                self._enqueue(consumer, offset, log[offset])
        return subscription

    def _replay_sources(
        self, pattern: str, tokens: tuple[str, ...], start: Start | Position | None
    ) -> list[tuple[str, int]]:
        """The (topic, first offset) pairs a new subscription has to be fed from, in topic creation order."""
        if start is None or start is Start.LATEST:
            return []
        if start is Start.EARLIEST:
            return [(topic, 0) for topic in self._logs if _matches(tokens, tuple(topic.split(".")))]
        if not isinstance(start, Position):
            raise TypeError(f"start must be a Start, a Position or None, got {start.__class__.__name__}")
        if start.topic != pattern or "*" in tokens or ">" in tokens:
            raise ValueError(f"{start} needs a subscription to exactly that topic, not {pattern!r}")
        length = len(self._logs.get(start.topic, ()))
        if not _is_int(start.token) or not 0 <= start.token <= length:
            raise ValueError(f"{start} is not a position of this bus's log (length {length})")
        return [(start.topic, start.token)] if start.topic in self._logs else []

    def _cancel(self, subscription: _Subscription) -> None:
        if subscription.cancelled:
            return
        subscription.cancelled = True
        del self._subscriptions[subscription.id]
        consumer = subscription.consumer
        consumer.members.remove(subscription)
        if not consumer.members:
            consumer.closed = True
            for lane in consumer.lanes.values():
                lane.stamp += 1
                lane.queue.clear()
            self._consumers.remove(consumer)
            self._routes.clear()
            if consumer.group is not None:
                del self._groups[consumer.group]
            return
        for lane in consumer.lanes.values():
            if lane.owner is subscription:
                lane.owner = consumer.member_for(lane.topic, lane.key)
                lane.stamp += 1
                if lane.queue and not lane.inflight:
                    self._push(lane)

    # delivery outcome

    def ack(self, delivery: Delivery) -> None:
        self._check_handling(delivery)
        self._nacked = False

    def nack(self, delivery: Delivery) -> None:
        self._check_handling(delivery)
        self._nacked = True

    def position_of(self, delivery: Delivery) -> Position:
        self._check_open()
        topic = delivery.envelope.topic
        subscription = self._subscriptions.get(delivery.subscription)
        cursor = None if subscription is None else subscription.consumer.cursors.get(topic)
        if cursor is None:
            raise BusError("not a delivery of a live subscription of this bus")
        return Position(topic, cursor.after(delivery.offset))

    def _check_handling(self, delivery: Delivery) -> None:
        self._check_open()
        current = self._current
        if (
            current is None
            or current.subscription != delivery.subscription
            or current.offset != delivery.offset
            or current.attempt != delivery.attempt
            or current.envelope.topic != delivery.envelope.topic
        ):
            raise BusError("ack and nack apply only to the delivery whose handler is running")

    # dispatch

    def step(self) -> bool:
        self._check_open()
        if self._current is not None:
            raise BusError("step and run_until_idle cannot be called from a handler: no nested delivery")
        if not self._drop_stale():
            return False
        lane: _Lane = heappop(self._heap)[-1]
        offset, envelope = lane.queue[0]
        owner = lane.owner
        delivery = Delivery(envelope, lane.attempts + 1, owner.id, offset)
        lane.inflight = True
        self._current = delivery
        self._nacked = False
        error: Exception | None = None
        try:
            owner.handler(delivery)
        except Exception as exc:
            error = exc
        except BaseException:
            lane.inflight = False
            if lane.queue:  # not delivered: stays at the head of its lane (empty if the handler closed or cancelled)
                self._push(lane)
            raise
        finally:
            self._current = None
        lane.inflight = False
        consumer = lane.consumer
        if consumer.closed:  # cancelled or closed by the handler: close() empties every lane
            return True
        if error is not None or self._nacked:
            lane.attempts += 1
            if lane.attempts < self._max_attempts:
                self._push(lane)
                return True
            self._dead.append(
                DeadLetter(
                    envelope,
                    owner.id,
                    lane.attempts,
                    None if error is None else error.__class__.__qualname__,
                    None if error is None else str(error),
                )
            )
        lane.queue.popleft()
        lane.attempts = 0
        lane.cursor.settle(offset)
        consumer.pending -= 1
        if lane.queue:
            self._push(lane)
        return True

    def _drop_stale(self) -> bool:
        """Pop invalidated heap entries; True when a valid one is on top."""
        heap = self._heap
        while heap:
            entry = heap[0]
            if entry[-2] == entry[-1].stamp:
                return True
            heappop(heap)
        return False

    def _has_pending(self) -> bool:
        """Whether a delivery is pending (`LogBus` first reads what it published, which may have queued some)."""
        return self._drop_stale()

    def run_until_idle(self, max_deliveries: int | None = None) -> int:
        self._check_open()
        if self._current is not None:
            raise BusError("step and run_until_idle cannot be called from a handler: no nested delivery")
        if max_deliveries is not None and (not _is_int(max_deliveries) or max_deliveries < 0):
            raise ValueError(f"max_deliveries must be a non-negative int or None, got {max_deliveries!r}")
        count = 0
        while True:
            if max_deliveries is not None and count >= max_deliveries:
                if self._has_pending():
                    raise NotIdle(f"messages still pending after {count} deliveries")
                return count
            if not self.step():
                return count
            count += 1

    @property
    def networked(self) -> bool:
        return False

    def poll(self, timeout: float = 0.0) -> bool:
        """Nothing can arrive from outside an in-memory bus: False at once."""
        self._check_open()
        return False

    def dead_letters(self) -> tuple[DeadLetter, ...]:
        self._check_open()
        return tuple(self._dead)

    def close(self) -> None:
        self._closed = True
        for consumer in self._consumers:
            consumer.closed = True
            for lane in consumer.lanes.values():
                lane.queue.clear()
            for subscription in consumer.members:
                subscription.cancelled = True
        self._logs.clear()
        self._consumers.clear()
        self._groups.clear()
        self._subscriptions.clear()
        self._routes.clear()
        self._heap.clear()

    def _check_open(self) -> None:
        if self._closed:
            raise BusClosed("the bus is closed")
