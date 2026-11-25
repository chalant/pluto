"""The message bus: a thin interface, a deterministic in-memory implementation, a serialising wrapper.

Components talk only through a `Bus` (Execution Topology ET1 to ET9). The in-memory implementation runs on one
thread and makes every run one of the legal runs of a networked bus. The rules below are the S-rules of the
message bus survey (vault: plans/Message Bus Options.md) that this package implements; every implementation must
pass `tests/test_bus_contract.py`.

S1 Ordering. Messages are FIFO per (topic, key) for each subscription, and nothing stronger: no order is
   promised across topics, keys or producers except the dispatch rule of S3.
S2 No nested delivery. `publish` logs and enqueues and returns; a handler only ever runs from `step` or
   `run_until_idle`, never inside `publish`, and never inside another handler (calling `step` from a handler
   raises `BusError`).
S3 Deterministic dispatch. The next delivery is the smallest head among the ready lanes by
   `(logical_ts, producer_id, seq)`, then handler rank, then registration order. It depends only on message
   content and registration order, never on dict or set iteration order, hash values or wall-clock time: the same
   publishes give the same delivery trace under any `PYTHONHASHSEED`. It is not causal: a handler may publish a
   message with a smaller `logical_ts` than the one it is handling, and that message is dispatched first.
S5 Time. The bus never reads a clock. `logical_ts` (an int, nanoseconds) is chosen by the producer and travels in
   the envelope; timers are messages the controller publishes.
S6 Handler order. `rank` is explicit, lower first; equal ranks follow registration order.
S7 Failures. Delivery is at-least-once. A handler that raises, or calls `nack`, causes a redelivery of the same
   message with its dispatch key unchanged, so before anything later in its lane (per-key order holds), with
   `Delivery.attempt` counting up; after `max_attempts` the message is dropped for that subscription and a
   `DeadLetter` is recorded. The publisher never sees a handler error. The bus does not deduplicate: a message
   published twice with the same `msg_id` is delivered twice, and handlers must be idempotent on `msg_id`.
S9 Replay. A subscription starts at `Start.EARLIEST`, `Start.LATEST` (default) or a `Position` of an exact topic.
   Each topic has an append-only log, so replay delivers logged messages as if just published, merged by S3. A
   `Position` is opaque and storable; `Receipt.position` is the published message (inclusive) and
   `Bus.position_of(delivery)` the one after the delivered message (resume point).
S10 Request and reply. `request` and `reply` (module `pluto.bus.request`) are a library over publish and
   subscribe with a correlation id and a reply topic; replies are ordinary logged messages. There is no timeout
   in the bus (S5): the caller retries with the same `msg_id`.
S11 Serialisation. Messages are immutable. `SerializingBus` round-trips every payload through bytes (protobuf
   through `SerializeToString` and `FromString`, other types through a pluggable codec) and the contract suite
   runs with it on and off.
S12 Backpressure. A subscription holds at most `queue_capacity` undelivered messages. A `publish` that would
   exceed that for any receiver raises `QueueFull` to the publisher and changes nothing (blocking would deadlock a
   single thread, dropping would change results silently).
S13 Hygiene. No unordered iteration, no randomness, deterministic ids (`seq` per producer, `msg_id` from it).
S14 Quiescence. `run_until_idle()` runs handlers until nothing is pending (handlers may publish more) and
   `step()` runs exactly one, so the controller can drive a phase and a test can assert that nothing is left.

Not here: S4 (the networked merge), seeded fault injection, start from a time, `seek`, deduplication, queue
retention limits for logs (logs are never trimmed).
"""

from pluto.bus.api import (
    Bus,
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
from pluto.bus.memory import InMemoryBus
from pluto.bus.request import Request, reply, request
from pluto.bus.serializing import Codec, JsonCodec, ProtobufCodec, SerializingBus

__all__ = [
    "Bus",
    "BusClosed",
    "BusError",
    "Codec",
    "DeadLetter",
    "Delivery",
    "Envelope",
    "Handler",
    "InMemoryBus",
    "JsonCodec",
    "NotIdle",
    "Position",
    "ProtobufCodec",
    "QueueFull",
    "Receipt",
    "Request",
    "SerializingBus",
    "Start",
    "Subscription",
    "reply",
    "request",
]
