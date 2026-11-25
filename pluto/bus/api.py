"""The bus interface: value types, errors and the `Bus` protocol.

Every implementation (in-memory, serialising wrapper, later a networked one) passes the same contract suite
(`tests/test_bus_contract.py`). The rules the interface fixes are in the docstring of `pluto.bus`.
"""

import enum
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


class BusError(Exception):
    """Base class of the errors the bus itself raises (handler errors never reach the publisher)."""


class BusClosed(BusError):
    """The bus was closed."""


class QueueFull(BusError):
    """`publish` or a replay would exceed a subscription's queue capacity; nothing was logged or enqueued."""


class NotIdle(BusError):
    """`run_until_idle` stopped at `max_deliveries` with messages still pending (a handler loop)."""


@dataclass(frozen=True, slots=True)
class Envelope:
    """One message. Immutable; `payload` must be treated as immutable too (a serialising bus hands out copies)."""

    msg_id: str  # idempotency key; a retry of a request reuses it
    topic: str
    key: str  # ordering lane within the topic: FIFO per (topic, key)
    type: str  # payload type name, e.g. the protobuf message name; selects the codec on a serialising bus
    logical_ts: int  # simulated time in nanoseconds; set by the producer, never read from a wall clock
    producer_id: str
    seq: int  # per-producer sequence
    correlation_id: str | None = None
    reply_to: str | None = None
    schema_version: int = 1
    payload: Any = None


@dataclass(frozen=True, slots=True)
class Position:
    """Opaque place in one topic's log. Store it with the consumer's state and pass it back as `start`.

    Callers must not interpret or compare it; only the bus that issued it knows what the fields mean.
    """

    topic: str
    offset: int


@dataclass(frozen=True, slots=True)
class Receipt:
    """Returned by `publish` once the message is accepted (in memory: logged and enqueued, not delivered)."""

    msg_id: str
    position: Position  # of the published message: `start=position` delivers it first


class Start(enum.Enum):
    """Where a new subscription begins reading its topic's log."""

    EARLIEST = "earliest"  # the whole log, in log order per lane
    LATEST = "latest"  # only messages published after the subscription (the default)


@dataclass(frozen=True, slots=True)
class Delivery:
    """One delivery attempt of one message to one subscription. `attempt` counts from 1."""

    envelope: Envelope
    attempt: int
    subscription: int  # `Subscription.id`
    offset: int  # of the message in its topic's log; use `Bus.position_of`, do not interpret


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A message given up on after `attempts` failed deliveries to subscription `subscription`."""

    envelope: Envelope
    subscription: int
    attempts: int
    error: Exception | None  # the last exception, None when the last outcome was an explicit nack


type Handler = Callable[[Delivery], None]


class Subscription(Protocol):
    id: int  # registration order, starting at 1

    def cancel(self) -> None:
        """Stop deliveries; messages still queued for it are dropped. Idempotent."""


class Bus(Protocol):
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
        """Log and enqueue a message; never runs a handler. `seq` defaults to the next number of `producer_id`,
        `msg_id` to `f"{producer_id}:{seq}"`."""

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
        message on. A handler that returns acknowledges the delivery, one that raises is nacked.
        Subscriptions of one `group` share a single stream: each (topic, key) lane goes to one member.
        Lower `rank` runs first when several subscriptions receive the same message."""

    def ack(self, delivery: Delivery) -> None:
        """Mark the delivery being handled as processed (the default outcome of a handler that returns)."""

    def nack(self, delivery: Delivery) -> None:
        """Mark the delivery being handled for redelivery (also the outcome of a handler that raises)."""

    def position_of(self, delivery: Delivery) -> Position:
        """The position just after the delivered message: store it as the consumer's checkpoint and pass it as
        `start` to resume with the next message."""

    def step(self) -> bool:
        """Run one handler invocation. False when nothing was pending."""

    def run_until_idle(self, max_deliveries: int | None = None) -> int:
        """Run `step` until nothing is pending (handlers may publish more). Returns the invocations run;
        raises `NotIdle` when `max_deliveries` is reached with messages still pending."""

    def dead_letters(self) -> tuple[DeadLetter, ...]:
        """The dead-letter records so far, oldest first."""

    def close(self) -> None:
        """Drop everything pending; any later call raises `BusClosed`. Idempotent."""
