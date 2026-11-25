"""The deterministic in-memory bus: one thread, objects passed by reference."""

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
)

_ONE = "*"
_REST = ">"


def _tokens(topic_or_pattern: str) -> tuple[str, ...]:
    if not isinstance(topic_or_pattern, str) or not topic_or_pattern:
        raise ValueError(f"topic or pattern must be a non-empty string, got {topic_or_pattern!r}")
    tokens = tuple(topic_or_pattern.split("."))
    if "" in tokens or _REST in tokens[:-1]:
        raise ValueError(f"malformed topic or pattern {topic_or_pattern!r}")
    return tokens


def _type_name(value: object) -> str:
    return type(value).__name__


def _is_exact(tokens: tuple[str, ...]) -> bool:
    return _ONE not in tokens and _REST not in tokens


def _matches(pattern: tuple[str, ...], topic: tuple[str, ...]) -> bool:
    if pattern[-1] == _REST:
        head = pattern[:-1]
        return len(topic) > len(head) and all(p in (_ONE, t) for p, t in zip(head, topic))
    return len(pattern) == len(topic) and all(p in (_ONE, t) for p, t in zip(pattern, topic))


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


class _Consumer:
    """What reads a stream: one subscription, or all members of a group. Holds the lanes and the queue count."""

    __slots__ = ("pattern", "tokens", "group", "members", "lanes", "pending", "closed")

    def __init__(self, pattern: str, tokens: tuple[str, ...], group: str | None) -> None:
        self.pattern = pattern
        self.tokens = tokens
        self.group = group
        self.members: list[_Subscription] = []
        self.lanes: dict[tuple[str, str], _Lane] = {}
        self.pending = 0  # queued, not yet acknowledged
        self.closed = False


class _Lane:
    """The queued messages of one (topic, key) for one consumer, FIFO, handled by one member."""

    __slots__ = ("index", "consumer", "owner", "queue", "attempts", "stamp", "inflight")

    def __init__(self, index: int, consumer: _Consumer, owner: _Subscription) -> None:
        self.index = index
        self.consumer = consumer
        self.owner = owner
        self.queue: deque[tuple[int, Envelope]] = deque()  # (log offset, envelope); the head stays until settled
        self.attempts = 0  # failed deliveries of the head
        self.stamp = 0  # bumped whenever the heap entry of the lane becomes invalid
        self.inflight = False


class InMemoryBus:
    """Deterministic single-thread bus. See the docstring of `pluto.bus` for the rules.

    Dispatch: each consumer keeps one FIFO lane per (topic, key). The heap holds the head of every non-empty lane
    that is not being handled, ordered by `(logical_ts, producer_id, seq, rank, subscription id, lane index)`;
    `step` takes the smallest. Everything in that key is message content or registration order.

    Overflow policy: a subscription holds at most `queue_capacity` undelivered messages (replay included). A
    `publish` that would exceed it for any receiving subscription raises `QueueFull` to the publisher and changes
    nothing; the publisher drains with `run_until_idle` or fails loudly. It cannot block (a single thread would
    wait for itself) and it does not drop (silent loss would change results).

    Lanes are never freed: use keys of low cardinality (strategy or instrument ids). Logs are never trimmed.
    """

    def __init__(self, *, queue_capacity: int = 100_000, max_attempts: int = 3) -> None:
        if queue_capacity < 1 or max_attempts < 1:
            raise ValueError("queue_capacity and max_attempts must be at least 1")
        self._capacity = queue_capacity
        self._max_attempts = max_attempts
        self._closed = False
        self._logs: dict[str, list[Envelope]] = {}
        self._seq: dict[str, int] = {}
        self._consumers: list[_Consumer] = []  # registration order
        self._groups: dict[str, _Consumer] = {}
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
        key: str = "",
        seq: int | None = None,
        msg_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        schema_version: int = 1,
    ) -> Receipt:
        self._check_open()
        if not isinstance(logical_ts, int):
            raise TypeError(f"logical_ts must be an int (nanoseconds), got {_type_name(logical_ts)}")
        if not isinstance(key, str) or not isinstance(producer_id, str):
            raise TypeError("key and producer_id must be str")
        log = self._logs.get(topic)
        if log is None:
            if not _is_exact(_tokens(topic)):
                raise ValueError(f"cannot publish to the pattern {topic!r}")
            log = self._logs[topic] = []
        if seq is None:
            seq = self._seq[producer_id] = self._seq.get(producer_id, 0) + 1
        elif not isinstance(seq, int):
            raise TypeError(f"seq must be an int, got {_type_name(seq)}")
        if msg_id is None:
            msg_id = f"{producer_id}:{seq}"
        consumers = self._consumers_for(topic)
        for consumer in consumers:
            if consumer.pending >= self._capacity:
                raise QueueFull(f"subscription queue for {consumer.pattern!r} is full ({self._capacity})")
        envelope = Envelope(
            msg_id, topic, key, type, logical_ts, producer_id, seq, correlation_id, reply_to, schema_version, payload
        )
        offset = len(log)
        log.append(envelope)
        for consumer in consumers:
            self._enqueue(consumer, offset, envelope)
        return Receipt(msg_id, Position(topic, offset))

    def _consumers_for(self, topic: str) -> tuple[_Consumer, ...]:
        consumers = self._routes.get(topic)
        if consumers is None:
            tokens = tuple(topic.split("."))
            consumers = self._routes[topic] = tuple(c for c in self._consumers if _matches(c.tokens, tokens))
        return consumers

    def _enqueue(self, consumer: _Consumer, offset: int, envelope: Envelope) -> None:
        lane = consumer.lanes.get((envelope.topic, envelope.key))
        if lane is None:
            index = len(consumer.lanes)
            lane = _Lane(index, consumer, consumer.members[index % len(consumer.members)])
            consumer.lanes[(envelope.topic, envelope.key)] = lane
        was_empty = not lane.queue
        lane.queue.append((offset, envelope))
        consumer.pending += 1
        if was_empty:
            self._push(lane)

    def _push(self, lane: _Lane) -> None:
        envelope = lane.queue[0][1]
        owner = lane.owner
        heappush(
            self._heap,
            (envelope.logical_ts, envelope.producer_id, envelope.seq, owner.rank, owner.id, lane.index, lane.stamp, lane),
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
        tokens = _tokens(topic_or_pattern)
        subscription = _Subscription(self, self._next_subscription, handler, rank)
        consumer = None if group is None else self._groups.get(group)
        if consumer is not None:
            if consumer.pattern != topic_or_pattern or start is not None:
                raise ValueError(f"group {group!r} reads {consumer.pattern!r}; members join it without `start`")
            self._next_subscription += 1
            subscription.consumer = consumer
            consumer.members.append(subscription)
            return subscription
        replay = self._replay(topic_or_pattern, tokens, start)
        if len(replay) > self._capacity:
            raise QueueFull(f"replay of {len(replay)} messages exceeds the queue capacity {self._capacity}")
        self._next_subscription += 1
        consumer = _Consumer(topic_or_pattern, tokens, group)
        consumer.members.append(subscription)
        subscription.consumer = consumer
        self._consumers.append(consumer)
        self._routes.clear()
        if group is not None:
            self._groups[group] = consumer
        for offset, envelope in replay:
            self._enqueue(consumer, offset, envelope)
        return subscription

    def _replay(
        self, pattern: str, tokens: tuple[str, ...], start: Start | Position | None
    ) -> list[tuple[int, Envelope]]:
        if start is None or start is Start.LATEST:
            return []
        if start is Start.EARLIEST:
            return [
                (offset, envelope)
                for topic, log in self._logs.items()
                if _matches(tokens, tuple(topic.split(".")))
                for offset, envelope in enumerate(log)
            ]
        if not isinstance(start, Position):
            raise TypeError(f"start must be a Start, a Position or None, got {_type_name(start)}")
        if start.topic != pattern or not _is_exact(tokens):
            raise ValueError(f"{start} needs a subscription to exactly that topic, not {pattern!r}")
        log = self._logs.get(start.topic, [])
        if not 0 <= start.offset <= len(log):
            raise ValueError(f"{start} is outside the log (length {len(log)})")
        return [(offset, log[offset]) for offset in range(start.offset, len(log))]

    def _cancel(self, subscription: _Subscription) -> None:
        if subscription.cancelled:
            return
        subscription.cancelled = True
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
                lane.owner = consumer.members[lane.index % len(consumer.members)]
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
        return Position(delivery.envelope.topic, delivery.offset + 1)

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
            if lane.queue:  # not delivered: stays at the head of its lane (empty if the handler cancelled)
                self._push(lane)
            raise
        finally:
            self._current = None
        lane.inflight = False
        consumer = lane.consumer
        if consumer.closed:
            return True
        if error is not None or self._nacked:
            lane.attempts += 1
            if lane.attempts < self._max_attempts:
                self._push(lane)
                return True
            self._dead.append(DeadLetter(envelope, owner.id, lane.attempts, error))
        lane.queue.popleft()
        lane.attempts = 0
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

    def run_until_idle(self, max_deliveries: int | None = None) -> int:
        count = 0
        while True:
            if count == max_deliveries and self._drop_stale():
                raise NotIdle(f"messages still pending after {count} deliveries")
            if not self.step():
                return count
            count += 1

    def dead_letters(self) -> tuple[DeadLetter, ...]:
        return tuple(self._dead)

    def close(self) -> None:
        self._closed = True
        self._logs.clear()
        self._consumers.clear()
        self._groups.clear()
        self._routes.clear()
        self._heap.clear()

    def _check_open(self) -> None:
        if self._closed:
            raise BusClosed("the bus is closed")
