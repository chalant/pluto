"""`LogBus`: the bus over a broker that is used as a durable, totally ordered log of one run (a namespace).

A `Log` does only what a broker can guarantee: take messages in order (`append` sends and does not wait) and hand back
everything appended since the last read (`read_new`), the log's own messages among them, which is how the bus learns that
the broker took them and at which position. Everything else of the `Bus`
contract is the dispatcher of `InMemoryBus`, run on the reader's side over a mirror of the log: lanes, the S3 dispatch
order, rank, acknowledgement, redelivery and dead letters, groups, positions. That is what makes the delivery of a
networked run one of the legal runs of the in-memory bus (and the same test suite passes on both): the broker gives order
of arrival, the reader gives the order of S3.

What differs from the in-memory bus (and from the interface's first draft, `pluto.bus.__init__`):

* `publish` does not wait for the broker. The log sends the message at once; the bus learns that the broker took it, and
  its position, when it reads the message back (the next `step` after a publish, `run_until_idle`, `poll`, `subscribe`,
  `close`) or when the receipt's position is asked for; the broker's refusal (or a message that does not come back) raises
  `BusError` there, and from then on at every read (the bus cannot go on after a message it sent was lost). Nothing is
  dispatched inside `publish` (S2).
* The bus reads its own messages back from the log: one path for local and remote messages, and the position of a message
  is the log's. `step` reads the log before it dispatches whenever this bus published since its last read, so the S3
  choice is made over everything this bus sent. It does not look for what other processes wrote: idle means idle *here*
  (`networked`); whoever waits for an answer from another process `poll`s for it (the controller's `PhaseDriver` does),
  and `subscribe` reads the whole log first.
* Each bus mirrors the whole log of its namespace from the start, as the in-memory bus keeps its logs. A `Position` is
  the broker's position (a stream sequence, an offset), so a bus attached to the same namespace later, in another process,
  resumes from a stored position. Retention, trimming and the mirror's size are open.
* Not here: the in-memory bus's `QueueFull` (a rule of the in-memory bus, `pluto.bus.__init__` S12); groups are in-process
  only (the members of a group share one reader); the payload is `bytes` (wrap the bus in `SerializingBus`).

`LogBus` subclasses `InMemoryBus` and uses its dispatch state (`_logs`, `_routes`, `_enqueue`, `_drop_stale`,
`_current`...): change them together.
"""

import json
import struct
from bisect import bisect_left
from collections import deque
from typing import Any, Protocol

from pluto.bus.api import BusError, Envelope, NotIdle, Position, Receipt, exact_topic
from pluto.bus.memory import InMemoryBus, _check_fields, _is_int


class Log(Protocol):
    """A durable, totally ordered log of one run. Positions are ints, increasing in the order of the log."""

    def append(self, topic: str, key: str, data: bytes) -> None:
        """Send a message, in order after the ones appended before; does not wait for the broker."""

    def read_new(self, wait: float = 0.0, complete: bool = True) -> list[tuple[int, str, bytes, int | None]]:
        """(position, topic, data, own) of what was appended since the previous call, in log order; `own` is the
        number (1, 2, ...) of the entry among the messages this log appended, None for another writer's. Does not
        return before every message appended so far has come back (the broker has taken them; `BusError` if it did not,
        or not in order). `complete`: up to the end of the log at the time of the call (what other processes wrote
        too); if not, as far as this log's own messages. Waits up to `wait` seconds for the first message when there
        is none."""

    def close(self) -> None:
        """Wait for what was appended to be taken, release the connection. Idempotent."""


def encode(fields: tuple, payload: bytes) -> bytes:
    """A message on the wire: the envelope without topic and payload as JSON, then the payload (as is)."""
    header = json.dumps(fields, separators=(",", ":")).encode()
    return struct.pack(">I", len(header)) + header + payload


def decode(topic: str, data: bytes) -> Envelope:
    (size,) = struct.unpack_from(">I", data)
    msg_id, key, type_, ts, phase, producer, seq, correlation, reply_to, version = json.loads(data[4 : 4 + size])
    return Envelope(msg_id, topic, key, type_, ts, phase, producer, seq, correlation, reply_to, version, data[4 + size :])


class LogBus(InMemoryBus):
    """The bus over a `Log`; see the module docstring."""

    def __init__(self, log: Log, *, max_attempts: int = 3) -> None:
        super().__init__(max_attempts=max_attempts)
        self._log = log
        self._tokens: dict[str, list[int]] = {}  # per topic: the log position of each mirrored message
        self._frontier = 0  # one past the last log position read
        self._dirty = False  # published since the last read
        self._unresolved: deque[tuple[Receipt, str]] = deque()  # published, position not known yet (in log order)
        self._failure: BusError | None = None

    @property
    def networked(self) -> bool:
        return True

    # publish: handed to the log, not waited for

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
        _check_fields(logical_ts, phase, seq, key, producer_id)
        if topic not in self._logs:
            exact_topic(topic)
        if not isinstance(payload, bytes):
            raise TypeError("a LogBus carries bytes: wrap it in SerializingBus")
        if seq is None:
            seq = self._seq[producer_id] = self._seq.get(producer_id, 0) + 1
        if msg_id is None:
            msg_id = f"{producer_id}:{seq}"
        data = encode(
            (msg_id, key, type, logical_ts, phase, producer_id, seq, correlation_id, reply_to, schema_version), payload
        )
        self._log.append(topic, key, data)
        receipt = Receipt(msg_id, lambda: self._position_of_receipt(receipt))
        self._unresolved.append((receipt, topic))
        self._dirty = True
        return receipt

    def _position_of_receipt(self, receipt: Receipt) -> Position:
        """Asked for: read what this bus sent back (the broker took it, in order), which resolves the receipts."""
        if self._closed:
            raise BusError(f"the position of message {receipt.msg_id!r} is unknown: the bus was closed before it")
        self._read(complete=False)
        if not receipt.resolved:
            raise BusError(f"the position of message {receipt.msg_id!r} is unknown: the bus lost it")
        return receipt.position

    # reading the log

    def _read(self, wait: float = 0.0, complete: bool = True) -> int:
        """Read the log (what this bus sent comes back first), enqueue what is new for the subscriptions that exist.
        Returns how many messages."""
        if self._failure is not None:
            raise self._failure
        self._dirty = False
        try:
            entries = self._log.read_new(wait, complete)
        except BusError as error:
            self._failure = error  # a message that was sent may be lost: the bus cannot go on
            raise
        for token, topic, data, own in entries:
            if own is not None:
                receipt, sent_to = self._unresolved.popleft()
                if sent_to != topic:
                    self._failure = BusError("the log returned the messages of this bus out of order")
                    raise self._failure
                receipt._position = Position(topic, token)
            envelope = decode(topic, data)
            log = self._logs.get(topic)
            if log is None:
                log = self._logs[topic] = []
                self._tokens[topic] = []
            offset = len(log)
            log.append(envelope)
            self._tokens[topic].append(token)
            self._frontier = token + 1
            consumers = self._routes.get(topic)
            if consumers is None:
                consumers = self._build_route(topic)
            for consumer in consumers:
                self._enqueue(consumer, offset, envelope)
        return len(entries)

    def poll(self, timeout: float = 0.0) -> bool:
        self._check_open()
        return self._read(timeout) > 0

    def subscribe(self, topic_or_pattern, handler, *, group=None, start=None, rank=0):
        self._check_open()
        self._read()  # everything in the log now is "before" this subscription
        return super().subscribe(topic_or_pattern, handler, group=group, start=start, rank=rank)

    def _replay_sources(self, pattern, tokens, start):
        if isinstance(start, Position):
            if start.topic != pattern or "*" in tokens or ">" in tokens:
                raise ValueError(f"{start} needs a subscription to exactly that topic, not {pattern!r}")
            if not _is_int(start.token) or not 0 <= start.token <= self._frontier:
                raise ValueError(f"{start} is not a position of this log (end {self._frontier})")
            start = Position(start.topic, bisect_left(self._tokens.get(start.topic, ()), start.token))
        return super()._replay_sources(pattern, tokens, start)

    def position_of(self, delivery) -> Position:
        local = super().position_of(delivery)
        tokens = self._tokens[local.topic]
        return Position(local.topic, tokens[local.token] if local.token < len(tokens) else self._frontier)

    def _check_open(self) -> None:
        super()._check_open()
        if self._failure is not None:
            raise self._failure  # a message this bus sent was lost: nothing can be trusted after it

    def _check_not_handling(self) -> None:
        if self._current is not None:
            raise BusError("step and run_until_idle cannot be called from a handler: no nested delivery")

    # dispatch: the messages this bus published are read back first

    def step(self) -> bool:
        self._check_open()
        self._check_not_handling()
        if self._dirty:
            self._read(complete=False)  # what this bus sent: no need to ask the broker what else is there
        return super().step()

    def run_until_idle(self, max_deliveries: int | None = None) -> int:
        self._check_open()
        self._check_not_handling()
        if max_deliveries is not None and (not _is_int(max_deliveries) or max_deliveries < 0):
            raise ValueError(f"max_deliveries must be a non-negative int or None, got {max_deliveries!r}")
        count = 0
        while True:
            if max_deliveries is not None and count >= max_deliveries:
                if self._dirty:
                    self._read(complete=False)
                if self._drop_stale():
                    raise NotIdle(f"messages still pending after {count} deliveries")
                return count
            if not self.step():
                return count
            count += 1

    def close(self) -> None:
        was_closed = self._closed
        if not was_closed:
            try:
                self._read(complete=False)  # the receipts get their positions
            except BusError:
                pass
        super().close()
        if not was_closed:
            self._tokens.clear()
            self._unresolved.clear()
            self._log.close()  # which waits for what was appended
