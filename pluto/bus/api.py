"""The bus interface: value types, errors and the `Bus` protocol.

Every implementation (in-memory, serialising wrapper, log-backed networked bus) passes the same contract suite
(`tests/test_bus_contract.py`). The rules the interface fixes are in the docstring of `pluto.bus`.
"""

import enum
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NamedTuple, Protocol


class BusError(Exception):
    """Base class of the errors the bus itself raises (handler errors never reach the publisher)."""


class BusClosed(BusError):
    """The bus was closed."""


class QueueFull(BusError):
    """`publish` or a replay would exceed a subscription's queue capacity; nothing was logged or enqueued."""


class NotIdle(BusError):
    """`run_until_idle` stopped at `max_deliveries` with messages still pending (a handler loop)."""


class Envelope(NamedTuple):
    """One message. Immutable; `payload` must be treated as immutable too (a serialising bus hands out copies)."""

    msg_id: str  # idempotency key; a retry of a request reuses it
    topic: str
    key: str  # ordering lane within the topic: FIFO per (topic, key)
    type: str  # payload type name, e.g. the protobuf message name; selects the codec on a serialising bus
    logical_ts: int  # simulated time in nanoseconds; set by the producer, never read from a wall clock
    phase: int  # order among messages of the same logical_ts, lower first (the controller's phase rank)
    producer_id: str
    seq: int  # per-producer sequence
    correlation_id: str | None = None
    reply_to: str | None = None
    schema_version: int = 1
    payload: Any = None


class Position(NamedTuple):
    """Opaque place in one topic's log: store it with the consumer's state and pass it back as `start`.

    `topic` names the topic; `token` belongs to the bus that issued the position (the in-memory bus uses a log
    offset, a networked bus would encode its partitions). Callers store and pass it back, nothing else.
    """

    topic: str
    token: Any


class PositionTooOld(BusError):
    """A replay was asked from a place the log no longer has (it was trimmed: `Retention`, `Bus.trim`, or the broker's
    own limits). Never replayed from a later place instead: the caller would miss messages without knowing. `position`
    is what was asked for (None for `Start.EARLIEST`), `oldest` the earliest place that can still be replayed from."""

    def __init__(self, message: str, position: "Position | None", oldest: "Position | None") -> None:
        super().__init__(message)
        self.position = position
        self.oldest = oldest


@dataclass(frozen=True, slots=True)
class Retention:
    """How much of each topic's log a bus keeps. The default keeps everything (unlimited replay).

    `max_messages`: per topic, at least the newest `max_messages` entries are kept; older ones are dropped in batches
    (when more than a quarter more are held), so a topic holds between `max_messages` and 1.25 times that. A hard bound:
    the memory of the logs is a function of it, not of the length of a run. Pending deliveries are not affected (a
    subscription keeps the messages it has not settled); only replay and resume positions older than the window are.
    """

    max_messages: int | None = None

    def __post_init__(self) -> None:
        value = self.max_messages
        if value is not None and (value.__class__ is not int or value < 1):
            raise ValueError(f"max_messages must be an int of at least 1 or None, got {value!r}")


class Receipt:
    """Returned by `publish` once the message is accepted (in memory: logged and enqueued, not delivered).

    `position` is that of the published message: `start=position` delivers it first. A networked bus does not wait
    for the broker's acknowledgement in `publish`, so it does not know the position yet: `position` then resolves
    when it is asked for (it waits for the acknowledgement of that message, and of the ones sent before it), and the
    position is also known to the bus after its next read. `resolved` says whether it is known without waiting. A
    message the broker did not take raises `BusError` from `position`, or from the bus's next read.
    """

    __slots__ = ("msg_id", "_position")

    def __init__(self, msg_id: str, position: "Position | Callable[[], Position]") -> None:
        self.msg_id = msg_id
        self._position = position

    @property
    def position(self) -> Position:
        position = self._position
        if not isinstance(position, Position):
            position = self._position = position()
        return position

    @property
    def resolved(self) -> bool:
        return isinstance(self._position, Position)

    def __repr__(self) -> str:
        return f"Receipt({self.msg_id!r}, {self._position if self.resolved else '<unresolved>'!r})"


class Start(enum.Enum):
    """Where a new subscription begins reading its topic's log."""

    EARLIEST = "earliest"  # the whole log, in log order per lane
    LATEST = "latest"  # only messages published after the subscription (the default)


class Delivery(NamedTuple):
    """One delivery attempt of one message to one subscription. `attempt` counts from 1."""

    envelope: Envelope
    attempt: int
    subscription: int  # `Subscription.id`
    offset: int  # of the message in its topic's log; use `Bus.position_of`, do not interpret


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A message given up on after `attempts` failed deliveries to subscription `subscription`.

    The last failure is described by strings so that the record can cross a process boundary: the class name
    and the message of the exception, both None when the last outcome was an explicit nack.
    """

    envelope: Envelope
    subscription: int
    attempts: int
    error_type: str | None
    error_message: str | None


type Handler = Callable[[Delivery], None]


def topic_tokens(topic_or_pattern: str) -> tuple[str, ...]:
    """Split a dotted topic or pattern (`*` one token, a final `>` the rest); ValueError when malformed.

    The grammar is the one every bus can carry, so that a run that works on one works on all (a broker's subject syntax,
    NATS's, refuses whitespace and control characters: a topic the in-memory bus took and a networked bus could not send
    would be a run that works in one mode and fails in the other, and, on a log-backed bus, a message refused after
    `publish` returned). Whitespace and control characters are therefore refused by every bus."""
    if not isinstance(topic_or_pattern, str) or not topic_or_pattern:
        raise ValueError(f"topic or pattern must be a non-empty string, got {topic_or_pattern!r}")
    if any(character.isspace() or not character.isprintable() for character in topic_or_pattern):
        raise ValueError(f"topic or pattern {topic_or_pattern!r} has whitespace or a control character")
    tokens = tuple(topic_or_pattern.split("."))
    if "" in tokens or ">" in tokens[:-1]:
        raise ValueError(f"malformed topic or pattern {topic_or_pattern!r}")
    return tokens


def is_exact(tokens: tuple[str, ...]) -> bool:
    return "*" not in tokens and ">" not in tokens


def exact_topic(topic: str) -> str:
    """Return `topic` if it can be published to (dotted, no empty or wildcard token); ValueError otherwise."""
    if not is_exact(topic_tokens(topic)):
        raise ValueError(f"{topic!r} is a pattern, not a topic")
    return topic


class Subscription(Protocol):
    id: int  # registration order, starting at 1

    def cancel(self) -> None:
        """Stop deliveries; messages still queued for it are dropped. Idempotent, also after `Bus.close`."""


class Bus(Protocol):
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
        """Log and enqueue a message; never runs a handler. `seq` defaults to the next number of `producer_id`,
        `msg_id` to `f"{producer_id}:{seq}"`. A refused publish (`QueueFull`, bad argument) changes nothing.
        A networked bus sends the message without waiting for the broker: the receipt's position resolves later
        (see `Receipt`), and a message the broker refuses raises `BusError` from the bus's next read."""

    def subscribe(
        self,
        topic_or_pattern: str,
        handler: Handler,
        *,
        group: str | None = None,
        start: Start | Position | None = None,
        rank: int = 0,
    ) -> Subscription:
        """Register `handler` for a topic or a dotted pattern (`*` one token, a final `>` the rest).

        `start`: None (default) means `Start.LATEST`; a `Position` needs an exact topic and delivers from that
        position on. A handler that returns acknowledges the delivery, one that raises is nacked.
        Subscriptions of one `group` share a single stream: each (topic, key) lane goes to one member. The first
        member decides `start`; later members join with `start` None or `Start.LATEST` and the same pattern.
        Lower `rank` runs first when several subscriptions receive the same message."""

    def ack(self, delivery: Delivery) -> None:
        """Mark the delivery being handled as processed (the default outcome of a handler that returns)."""

    def nack(self, delivery: Delivery) -> None:
        """Mark the delivery being handled for redelivery (also the outcome of a handler that raises)."""

    def position_of(self, delivery: Delivery) -> Position:
        """The resume position of the delivery's subscription in the delivered message's topic: every message of
        that topic below it has been settled (acknowledged or dead-lettered) by the subscription, counting this
        delivery as settled. Store it as the consumer's checkpoint and pass it as `start` to resume. Messages
        above it that were already handled are delivered again (at-least-once), so deliveries that complete out
        of log order never leave a gap."""

    def step(self) -> bool:
        """Run one handler invocation. False when nothing was pending."""

    def run_until_idle(self, max_deliveries: int | None = None) -> int:
        """Run `step` until nothing is pending (handlers may publish more). Returns the invocations run;
        raises `NotIdle` when `max_deliveries` is reached with messages still pending."""

    @property
    def networked(self) -> bool:
        """True when messages can arrive that no handler of this bus object published (another process writes the
        log). `run_until_idle` is then idle *here*: the caller that waits for an answer from outside `poll`s for it.
        False in memory: when `run_until_idle` returns, nothing else can arrive."""

    def poll(self, timeout: float = 0.0) -> bool:
        """Read what other publishers put in the log since this bus last read it, waiting up to `timeout` seconds
        for the first message when there is none. Returns True when messages were read (they are queued: `step` and
        `run_until_idle` deliver them). Never runs a handler. Always False, at once, when not `networked`."""

    def oldest(self, topic: str) -> Position:
        """The earliest position of the topic's log that can still be replayed from: `Position(topic, 0)`-like for a log
        that was never trimmed, later for one that was (see `Retention`, `trim`). Subscribing at it replays what is
        left; a position below it raises `PositionTooOld`."""

    def trim(self, position: Position) -> Position:
        """The owner of a run's low-water mark: drop the entries of `position.topic` below `position` from the log (replay
        cannot reach them any more), but never past what a live subscription of the topic still needs (the lowest
        `position_of` its next delivery would give). Returns the oldest position retained afterwards, which is `position`
        when nothing held it back. `ValueError` for a position beyond the end of the log or of a topic without one."""

    def dead_letters(self) -> tuple[DeadLetter, ...]:
        """The dead-letter records so far, oldest first."""

    def close(self) -> None:
        """Drop everything pending and cancel every subscription. Idempotent; afterwards every call except
        `close` and `Subscription.cancel` raises `BusClosed`."""
