"""The message bus: a thin interface, a deterministic in-memory implementation, a serialising wrapper, and a networked one.

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
   `(logical_ts, phase, producer_id, seq)`, then handler rank, then registration order, then (topic, key). It
   depends only on message content and registration order, never on dict or set iteration order, hash values or
   wall-clock time: the same publishes give the same delivery trace under any `PYTHONHASHSEED`, and a replay
   delivers a log in the order a live subscriber saw it. It is not causal: a handler may publish a message with a
   smaller `logical_ts` than the one it is handling, and that message is dispatched first.
S5 Time. The bus never reads a clock. `logical_ts` (an int, nanoseconds) is chosen by the producer and travels in
   the envelope; timers are messages the controller publishes. `phase` (an int) orders messages of one
   `logical_ts`, so the controller's phases need not be encoded in timestamps or producer ids.
S6 Handler order. `rank` is explicit, lower first; equal ranks follow registration order.
S7 Failures. Delivery is at-least-once. A handler that raises, or calls `nack`, causes a redelivery of the same
   message with its dispatch key unchanged, so before anything later in its lane (per-key order holds), with
   `Delivery.attempt` counting up; after `max_attempts` the message is dropped for that subscription and a
   `DeadLetter` (strings only: error type and message) is recorded. The publisher never sees a handler error.
   The bus does not deduplicate: a message published twice with the same `msg_id` is delivered twice, and
   handlers must be idempotent on `msg_id`.
S9 Replay. A subscription starts at `Start.EARLIEST`, `Start.LATEST` (default) or a `Position` of an exact topic.
   Each topic has an append-only log, so replay delivers logged messages as if just published, merged by S3.
   A `Position` is opaque and storable (its `token` belongs to the bus that issued it). `Receipt.position` is the
   published message (inclusive); on a networked bus `publish` does not wait for the broker's acknowledgement and the
   position resolves when asked for or at the next read. `Bus.position_of(delivery)` is the subscription's resume position in that
   topic: every message below it is settled, so deliveries that complete out of log order (S3 does not follow
   log order across keys) leave no gap; messages above it that were already handled are delivered again.
S10 Request and reply. `request` and `reply` (module `pluto.bus.request`) are a library over publish and
   subscribe with a correlation id and a reply topic (by default the inbox `reply.<producer_id>`); replies are
   ordinary logged messages. There is no timeout in the bus (S5): the caller retries with the same `msg_id`.
S11 Serialisation. Messages are immutable. `SerializingBus` round-trips every payload through bytes (protobuf
   through deterministic `SerializeToString` and `FromString`, other types through a pluggable codec) and the
   contract suite runs with it on and off.
S12 Backpressure. A subscription holds at most `queue_capacity` undelivered messages. A `publish` that would
   exceed that for any receiver raises `QueueFull` to the publisher and changes nothing (blocking would deadlock a
   single thread, dropping would change results silently). This is a rule of the in-memory bus, tested in
   `tests/test_bus_memory.py`, not of the shared contract.
S13 Hygiene. No unordered iteration, no randomness, deterministic ids (`seq` per producer, `msg_id` from it),
   deterministic encodings (sorted JSON keys, deterministic protobuf), group members chosen by a stable hash.
S14 Quiescence. `run_until_idle()` runs handlers until nothing is pending (handlers may publish more) and
   `step()` runs exactly one, so the controller can drive a phase and a test can assert that nothing is left. On a
   networked bus (`networked`) nothing pending means nothing pending *here*: other processes may still publish. Whoever
   waits for an answer from another process `poll`s for it (`PhaseDriver` does, with a deadline of its own).
S15 Topics. A topic is dotted tokens of printable characters without whitespace (the grammar a broker can carry, so a
   run that works on one bus works on all); patterns add `*` (one token) and a final `>` (the rest).

Groups: subscriptions of one `group` share a stream; each (topic, key) lane goes to one member, chosen by a stable
hash of the lane when it is created (so lanes created by a replay at subscribe time go to the members present
then). Closing: `close()` cancels every subscription and any later call but `close` and `cancel` raises.

Networked bus: `pluto.bus.log.LogBus` runs this dispatcher over a broker's log (S4: the broker orders by arrival, the reader by
S3, a phase barrier makes them agree); `pluto.bus.jetstream.JetStreamLog` is the NATS JetStream log (import it explicitly: it
needs `nats-py`). `networked` and `poll` are what a caller that waits for other processes uses (`PhaseDriver`). Its
differences (publish does not wait, no `QueueFull`, groups in-process) are in the docstring of `pluto.bus.log`.

Fault injection: `SeededFaultBus` (module `pluto.bus.faults`) wraps any bus and injects, from a seed, the faults a
networked bus may produce under this contract (duplicates, late arrival across lanes, lost deliveries, restarts,
publish failures, a silent participant); its docstring has the fault model.

Retention (S16). Logs are unlimited by default. `InMemoryBus(retention=Retention(max_messages=N))` (and `LogBus`) keeps at
least the newest N entries of each topic, dropping older ones in batches: the memory of the logs is a function of N, not of
the length of a run. Trimming never loses a delivery (a subscription holds the messages it has not settled); it only ends
replay: a position older than `oldest(topic)`, and `Start.EARLIEST` once a topic was trimmed, raise `PositionTooOld`, never a
shorter replay. `trim(position)` is the owner's explicit low-water mark ("everything before this tick is checkpointed"),
held back by what a live subscription may still resume from. Offsets and positions stay absolute. Idle lanes of a subscription
that is not in a group are freed (a group's lane keeps its owner, chosen when it was created, so its lanes are kept).
Dead letters are not trimmed (`dead_letters()[n:]` is how the new ones are found; a failing handler normally aborts a run).

Not here: start from a time, `seek`, deduplication.
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
    PositionTooOld,
    QueueFull,
    Receipt,
    Retention,
    Start,
    Subscription,
)
from pluto.bus.faults import Fault, FaultConfig, FaultKind, PublishFailed, SeededFaultBus
from pluto.bus.log import Log, LogBus
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
    "Fault",
    "FaultConfig",
    "FaultKind",
    "Handler",
    "InMemoryBus",
    "JsonCodec",
    "Log",
    "LogBus",
    "NotIdle",
    "Position",
    "PositionTooOld",
    "ProtobufCodec",
    "PublishFailed",
    "QueueFull",
    "Receipt",
    "Request",
    "Retention",
    "SeededFaultBus",
    "SerializingBus",
    "Start",
    "Subscription",
    "reply",
    "request",
]
