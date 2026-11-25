"""`SerializingBus`: round-trips every payload through bytes (ET4), so the contract suite can run with it on."""

import json
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Protocol

from pluto.bus.api import Bus, DeadLetter, Delivery, Envelope, Handler, Position, Receipt, Start, Subscription


class Codec(Protocol):
    """Payload to bytes and back. `encode` must be deterministic: equal payloads give equal bytes."""

    def encode(self, payload: Any, type: str) -> bytes: ...

    def decode(self, data: bytes, type: str) -> Any: ...


class JsonCodec:
    """JSON for plain payloads (None, bool, int, float, str, list, dict with str keys). Tuples come back as lists.
    Keys are sorted, so equal dicts give equal bytes."""

    def encode(self, payload: Any, type: str) -> bytes:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()

    def decode(self, data: bytes, type: str) -> Any:
        return json.loads(data)


class ProtobufCodec:
    """Protobuf messages through `SerializeToString(deterministic=True)` and `FromString`, chosen by the envelope
    `type`.

    `message_types` maps a type name to the message class. Payloads of any other type go to `fallback`.
    """

    def __init__(self, message_types: Mapping[str, type], fallback: Codec | None = None) -> None:
        self._types = dict(message_types)
        self._fallback = JsonCodec() if fallback is None else fallback

    def encode(self, payload: Any, type: str) -> bytes:
        message_type = self._types.get(type)
        if message_type is None:
            return self._fallback.encode(payload, type)
        if not isinstance(payload, message_type):
            raise TypeError(f"type {type!r} is registered as {message_type.__name__}, got {payload.__class__.__name__}")
        return payload.SerializeToString(deterministic=True)

    def decode(self, data: bytes, type: str) -> Any:
        message_type = self._types.get(type)
        if message_type is None:
            return self._fallback.decode(data, type)
        return message_type.FromString(data)


class SerializingBus:
    """Wraps a bus: the inner bus only ever carries bytes, each subscription decodes its own copy.

    Ordering, positions, acknowledgement and dispatch are the inner bus's; `Delivery` objects handed to handlers
    are copies with a decoded payload and are accepted by `ack`, `nack` and `position_of`. A payload that cannot be
    decoded fails the handler's delivery like any handler error.
    """

    def __init__(self, inner: Bus, codec: Codec) -> None:
        self._inner = inner
        self._codec = codec

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
        return self._inner.publish(
            topic,
            self._codec.encode(payload, type),
            type=type,
            logical_ts=logical_ts,
            producer_id=producer_id,
            phase=phase,
            key=key,
            seq=seq,
            msg_id=msg_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
            schema_version=schema_version,
        )

    def subscribe(
        self,
        topic_or_pattern: str,
        handler: Handler,
        *,
        group: str | None = None,
        start: Start | Position | None = None,
        rank: int = 0,
    ) -> Subscription:
        def decoding(delivery: Delivery) -> None:
            handler(self._decoded(delivery))

        return self._inner.subscribe(topic_or_pattern, decoding, group=group, start=start, rank=rank)

    def ack(self, delivery: Delivery) -> None:
        self._inner.ack(delivery)

    def nack(self, delivery: Delivery) -> None:
        self._inner.nack(delivery)

    def position_of(self, delivery: Delivery) -> Position:
        return self._inner.position_of(delivery)

    def step(self) -> bool:
        return self._inner.step()

    def run_until_idle(self, max_deliveries: int | None = None) -> int:
        return self._inner.run_until_idle(max_deliveries)

    def dead_letters(self) -> tuple[DeadLetter, ...]:
        """Dead letters with decoded payloads; one that cannot be decoded keeps its raw bytes."""
        letters = []
        for letter in self._inner.dead_letters():
            try:
                letter = replace(letter, envelope=self._decoded_envelope(letter.envelope))
            except Exception:
                pass
            letters.append(letter)
        return tuple(letters)

    def close(self) -> None:
        self._inner.close()

    def _decoded(self, delivery: Delivery) -> Delivery:
        return Delivery(
            self._decoded_envelope(delivery.envelope), delivery.attempt, delivery.subscription, delivery.offset
        )

    def _decoded_envelope(self, envelope: Envelope) -> Envelope:
        return Envelope(
            envelope.msg_id,
            envelope.topic,
            envelope.key,
            envelope.type,
            envelope.logical_ts,
            envelope.phase,
            envelope.producer_id,
            envelope.seq,
            envelope.correlation_id,
            envelope.reply_to,
            envelope.schema_version,
            self._codec.decode(envelope.payload, envelope.type),
        )
