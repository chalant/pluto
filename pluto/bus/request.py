"""Request and reply as a library over publish and subscribe, so requests and replies are logged and replayable.

The bus has no clock, so there is no timeout here: whoever drives logical time (the controller) decides that a
reply is overdue and calls `Request.send` again, which publishes the same message (same `msg_id`, same
correlation id). The bus redelivers it; making the receiver idempotent on `msg_id` is the receiver's job.

Replies go to the reply topic of the request, by default one inbox per producer, `reply.<producer_id>`; each
`Request` reads the inbox and keeps the reply with its correlation id.
"""

from collections.abc import Callable
from typing import Any

from pluto.bus.api import Bus, Delivery, Envelope, Receipt, Subscription, exact_topic


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
        phase: int,
        producer_id: str,
        key: str,
        msg_id: str,
        correlation_id: str,
        reply_to: str,
        schema_version: int,
        on_reply: Callable[[Envelope], None] | None,
    ) -> None:
        self.msg_id = msg_id
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.reply: Envelope | None = None
        self.receipt: Receipt | None = None  # of the latest `send`
        self._bus = bus
        self._topic = topic
        self._payload = payload
        self._type = type
        self._logical_ts = logical_ts
        self._phase = phase
        self._producer_id = producer_id
        self._key = key
        self._schema_version = schema_version
        self._on_reply = on_reply
        self._subscription: Subscription = bus.subscribe(reply_to, self._receive)

    @property
    def done(self) -> bool:
        return self.reply is not None

    def send(self, *, logical_ts: int | None = None) -> Receipt:
        """Publish the request; calling it again is the retry: same `msg_id` and correlation id, optionally at a
        later logical time."""
        if logical_ts is not None:
            self._logical_ts = logical_ts
        self.receipt = self._bus.publish(
            self._topic,
            self._payload,
            type=self._type,
            logical_ts=self._logical_ts,
            phase=self._phase,
            producer_id=self._producer_id,
            key=self._key,
            msg_id=self.msg_id,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            schema_version=self._schema_version,
        )
        return self.receipt

    def cancel(self) -> None:
        """Stop waiting; a reply that arrives later is not delivered to this request."""
        self._subscription.cancel()

    def _receive(self, delivery: Delivery) -> None:
        envelope = delivery.envelope
        if self.reply is not None or envelope.correlation_id != self.correlation_id:
            return
        if self._on_reply is not None:
            self._on_reply(envelope)  # a failure here nacks the delivery: the callback is called again
        self.reply = envelope
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
    phase: int = 0,
    key: str = "",
    correlation_id: str | None = None,
    reply_to: str | None = None,
    schema_version: int = 1,
    on_reply: Callable[[Envelope], None] | None = None,
) -> Request:
    """Subscribe to the reply topic, then publish the request. The reply arrives when the bus is run.

    `msg_id` is the idempotency key (for orders: strategy id plus sequence). `correlation_id` defaults to it and
    `reply_to` to `reply.<producer_id>`, an exact topic. `on_reply`, if given, is called from the dispatch loop
    with the reply (a handler cannot run the bus, so this is how a component acts on a reply); the request is
    `done` once it returned. If publishing the request fails nothing stays subscribed.
    """
    pending = Request(
        bus,
        topic,
        payload,
        type=type,
        logical_ts=logical_ts,
        phase=phase,
        producer_id=producer_id,
        key=key,
        msg_id=msg_id,
        correlation_id=msg_id if correlation_id is None else correlation_id,
        reply_to=exact_topic(f"reply.{producer_id}" if reply_to is None else reply_to),
        schema_version=schema_version,
        on_reply=on_reply,
    )
    try:
        pending.send()
    except BaseException:
        pending.cancel()
        raise
    return pending


def reply(
    bus: Bus,
    delivery: Delivery,
    payload: Any,
    *,
    type: str,
    logical_ts: int,
    producer_id: str,
    phase: int = 0,
    schema_version: int = 1,
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
        phase=phase,
        producer_id=producer_id,
        key=envelope.key,
        msg_id=f"{envelope.msg_id}:reply",
        correlation_id=envelope.correlation_id,
        schema_version=schema_version,
    )
