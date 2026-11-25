"""Request and reply as a library over publish and subscribe, so requests and replies are logged and replayable.

The bus has no clock, so there is no timeout here: whoever drives logical time (the controller) decides that a
reply is overdue and calls `Request.send` again, which publishes the same message (same `msg_id`, same
correlation id). The bus redelivers it; making the receiver idempotent on `msg_id` is the receiver's job.
"""

from typing import Any

from pluto.bus.api import Bus, Delivery, Envelope, Receipt, Subscription


class Request:
    """A published request waiting for its reply. Create with `request`."""

    def __init__(
        self,
        bus: Bus,
        topic: str,
        payload: Any,
        *,
        type: str,
        logical_ts: int,
        producer_id: str,
        key: str,
        msg_id: str,
        correlation_id: str,
        reply_to: str,
        schema_version: int,
    ) -> None:
        self.msg_id = msg_id
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.reply: Envelope | None = None
        self._bus = bus
        self._topic = topic
        self._payload = payload
        self._type = type
        self._logical_ts = logical_ts
        self._producer_id = producer_id
        self._key = key
        self._schema_version = schema_version
        self._subscription: Subscription = bus.subscribe(reply_to, self._on_reply)

    @property
    def done(self) -> bool:
        return self.reply is not None

    def send(self, *, logical_ts: int | None = None) -> Receipt:
        """Publish the request; calling it again is the retry: same `msg_id` and correlation id, optionally at a
        later logical time."""
        if logical_ts is not None:
            self._logical_ts = logical_ts
        return self._bus.publish(
            self._topic,
            self._payload,
            type=self._type,
            logical_ts=self._logical_ts,
            producer_id=self._producer_id,
            key=self._key,
            msg_id=self.msg_id,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            schema_version=self._schema_version,
        )

    def cancel(self) -> None:
        """Stop waiting; a reply that arrives later is not delivered to this request."""
        self._subscription.cancel()

    def _on_reply(self, delivery: Delivery) -> None:
        if self.reply is None and delivery.envelope.correlation_id == self.correlation_id:
            self.reply = delivery.envelope
            self._subscription.cancel()


def request(
    bus: Bus,
    topic: str,
    payload: Any,
    *,
    type: str,
    logical_ts: int,
    producer_id: str,
    msg_id: str,
    key: str = "",
    correlation_id: str | None = None,
    reply_to: str | None = None,
    schema_version: int = 1,
) -> Request:
    """Subscribe to the reply topic, then publish the request. The reply arrives when the bus is run.

    `msg_id` is the idempotency key (for orders: strategy id plus sequence). `correlation_id` defaults to it and
    `reply_to` to `reply.<producer_id>.<msg_id>`, a topic of this request alone.
    """
    pending = Request(
        bus,
        topic,
        payload,
        type=type,
        logical_ts=logical_ts,
        producer_id=producer_id,
        key=key,
        msg_id=msg_id,
        correlation_id=msg_id if correlation_id is None else correlation_id,
        reply_to=f"reply.{producer_id}.{msg_id}" if reply_to is None else reply_to,
        schema_version=schema_version,
    )
    pending.send()
    return pending


def reply(
    bus: Bus,
    delivery: Delivery,
    payload: Any,
    *,
    type: str,
    logical_ts: int,
    producer_id: str,
) -> Receipt:
    """Answer a delivered request on its `reply_to` topic with its correlation id.

    The reply's `msg_id` is derived from the request's, so answering a redelivered request twice sends the same
    reply twice; `Request` keeps the first.
    """
    envelope = delivery.envelope
    if envelope.reply_to is None:
        raise ValueError(f"message {envelope.msg_id!r} has no reply_to")
    return bus.publish(
        envelope.reply_to,
        payload,
        type=type,
        logical_ts=logical_ts,
        producer_id=producer_id,
        key=envelope.key,
        msg_id=f"{envelope.msg_id}:reply",
        correlation_id=envelope.correlation_id,
    )
