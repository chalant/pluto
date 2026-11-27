"""NATS JetStream as the `Log` of a run: a stream `<namespace>` (file storage) with subjects `<namespace>.>`; the bus topic
`t` is the subject `<namespace>.t`; one durable pull consumer per `JetStreamLog` reads the whole stream, in sequence order,
without acknowledgements (the bus keeps its own settled frontier, `LogBus`). The position of a message is its stream
sequence.

    log = JetStreamLog()                       # a new run: a new stream, a unique namespace
    bus = SerializingBus(LogBus(log), phases.codec())
    other = LogBus(JetStreamLog(log.namespace, create=False))   # another process attaching to the same run

`nats-py` is asyncio only: the client runs on an event loop of its own in a thread, and every call blocks on it, except
`append`: it hands the message to a sender task of that loop, which publishes in order (`publish_async`, one connection, so
the order of the stream is the order of the appends) and does not wait. The acknowledgement of a message is its coming back:
every message this log appended carries a header with the log's id and its number, and `read_new` does not return before
all the messages appended so far have been read back (the broker has stored them, in order), saying which entries are its
own and in which order. So one request answers both questions, "did the broker take my messages" and "what is new in the
log", and the position of an own message is the stream sequence of its entry. A message the server refuses (no stream, no
stream responder) or that does not come back within `timeout` seconds is a `BusError`.

The connection does not reconnect (`allow_reconnect=False`): a lost connection, even a blip, is a `BusError` from the next
call and the run aborts loudly; a silent reconnect could lose messages in flight, and recovery by replay is not built (the
connection errors nats-py would log are not logged here, they are raised). `close()` finishes all its cleanup and then raises
the first failure, so a message the broker refused cannot be lost silently at the end of a run. A bus that attaches late
(`create=False`) to a long stream reads all of it at once, within `timeout` per request: a history longer than that is a
`BusError`.

JetStream stores to disk with a periodic fsync (`sync_interval`, two minutes by default on the server): a crash of the
machine can lose acknowledged messages unless the server is configured otherwise.

Retention. The stream is the durable log of the run and keeps everything unless `max_messages`, `max_age` or `max_bytes` are
given (limits policy, the oldest messages are discarded first). `WorkQueue` and `Interest` retention are not usable: they delete
a message once consumers acknowledged it, and these consumers read without acks and from the start, so every late attach and
every replay would find the history gone. The stream and the consumer are deleted at close with `drop_on_close`; a process that
crashed leaves both (the consumer expires after a day, the stream stays), so a long-lived deployment sets limits and an orphaned
run is removed with `delete_stream(namespace)`.
"""

import asyncio
import os
import socket
import threading
import urllib.parse
import uuid
from collections.abc import Coroutine
from typing import Any

import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.errors import NotFoundError

from pluto.bus.api import BusError

DEFAULT_URL = os.environ.get("PLUTO_NATS_URL", "nats://127.0.0.1:4222")

_LIMITS_MESSAGE = (
    "the stream's limits (max_messages, max_age, max_bytes) dropped messages before this log read them: a message of this "
    "log, or the entries of other writers, are missing. The limits must exceed what is written between two reads"
)

# a consumer that is not asked for messages for this long (seconds) is deleted by the server
_CONSUMER_IDLE = 24 * 3600.0
_BATCH = 1000
_SLICE = 1.0  # a request for own messages that are not stored yet waits this long, then looks at the failures again
_LOG_HEADER = "Pluto-Log"
_NUMBER_HEADER = "Pluto-N"


def server_available(url: str = DEFAULT_URL, timeout: float = 0.5) -> bool:
    """Whether something accepts connections at the NATS url (tests that need a server skip when not)."""
    parsed = urllib.parse.urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or "127.0.0.1", parsed.port or 4222), timeout=timeout):
            return True
    except OSError:
        return False


def jetstream_available(url: str = DEFAULT_URL, timeout: float = 3.0) -> bool:
    """Whether a NATS server at the url answers and has JetStream enabled (what the tests that need a broker check:
    a server without JetStream is as good as none). Must not be called from a running event loop."""
    if not server_available(url, min(timeout, 1.0)):
        return False

    async def check() -> bool:
        async def quiet(error):
            pass

        nc = await nats.connect(url, connect_timeout=timeout, allow_reconnect=False, error_cb=quiet)
        try:
            await nc.jetstream(timeout=timeout).account_info()
            return True
        except Exception:
            return False
        finally:
            await nc.close()

    try:
        return asyncio.run(asyncio.wait_for(check(), timeout * 2))
    except Exception:
        return False


class _Loop:
    """An asyncio event loop in a thread, for the synchronous callers."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="jetstream-log", daemon=True)
        self._thread.start()

    def call(self, coroutine: Coroutine, timeout: float | None) -> Any:
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout)
        except TimeoutError:
            future.cancel()  # a coroutine that timed out must not go on next to the next call
            raise

    def submit(self, function, *args) -> None:
        """Run `function` on the loop's thread, without waiting."""
        self._loop.call_soon_threadsafe(function, *args)

    def stop(self) -> None:
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(5)
        if not self._thread.is_alive() and not self._loop.is_closed():
            self._loop.close()


def delete_stream(namespace: str, url: str = DEFAULT_URL, timeout: float = 10.0) -> bool:
    """Deletes the stream of a run (and its consumers): what a crashed run leaves behind, since the stream is the durable log
    and is only dropped at close on request. True if it was there. Must not be called from a running event loop."""

    async def delete() -> bool:
        async def quiet(error):
            pass

        nc = await nats.connect(url, connect_timeout=timeout, allow_reconnect=False, error_cb=quiet)
        try:
            return await nc.jetstream(timeout=timeout).delete_stream(namespace)
        except NotFoundError:
            return False
        finally:
            await nc.close()

    try:
        return asyncio.run(asyncio.wait_for(delete(), timeout * 2))
    except Exception as error:
        raise BusError(f"cannot delete stream {namespace!r}: {error.__class__.__name__}: {error}") from error


class JetStreamLog:
    """The log of one run on a JetStream stream. See the module docstring.

    Parameters
    ----------
    namespace: str, optional
        The run: the stream's name. Default: a new unique one (`self.namespace`).
    create: bool
        Create the stream (a new run); False to attach to the stream of an existing run.
    drop_on_close: bool
        Delete the stream when this log is closed (tests, throw-away runs).
    storage: StorageType
        `FILE` (default) or `MEMORY`.
    timeout: float
        Seconds to wait for the server's answers (own messages coming back, a fetch).
    max_messages, max_age, max_bytes: optional
        The stream's limits (messages, seconds, bytes; the oldest messages are discarded first): the log's own
        retention, which a `LogBus`'s mirror (`Retention`) does not control. Unlimited by default. A bus that attaches
        after the limits dropped the first messages cannot replay from before the first one it reads (`PositionTooOld`).
    """

    first_position = 1  # JetStream sequences start at 1

    def __init__(
        self,
        namespace: str | None = None,
        *,
        create: bool = True,
        drop_on_close: bool = False,
        storage: StorageType = StorageType.FILE,
        url: str = DEFAULT_URL,
        timeout: float = 30.0,
        max_messages: int | None = None,
        max_age: float | None = None,
        max_bytes: int | None = None,
    ) -> None:
        self.namespace = namespace or "pluto" + uuid.uuid4().hex[:12]
        for name, value in (("max_messages", max_messages), ("max_age", max_age), ("max_bytes", max_bytes)):
            if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0):
                raise ValueError(f"{name} must be a positive number or None, got {value!r}")
        # the stream's own retention (LimitsPolicy, discard oldest): what the server keeps whatever the buses read
        self._limits = {
            key: value for key, value in (("max_msgs", max_messages), ("max_age", max_age), ("max_bytes", max_bytes))
            if value is not None}
        self._drop_on_close = drop_on_close
        self._timeout = timeout
        self._prefix = self.namespace + "."
        self._id = uuid.uuid4().hex[:12]
        self._appended = 0  # messages handed to the log
        self._own_read = 0  # of those, the ones read back
        self._failure: BusError | None = None
        self._closed = False
        if not server_available(url, timeout=3.0):
            raise BusError(f"cannot connect to NATS at {url}: nothing accepts connections there")
        self._io = _Loop()
        try:
            self._nc, self._sub, self._consumer = self._call(self._open(url, create, storage))
        except BaseException:
            self._io.stop()
            raise

    async def _open(self, url, create, storage):
        async def quiet(error):  # nats-py logs every failed attempt otherwise; the failure is raised below
            pass

        try:
            nc = await asyncio.wait_for(
                nats.connect(
                    url, name="pluto-" + self.namespace, connect_timeout=5, allow_reconnect=False, error_cb=quiet),
                10)
        except Exception as error:
            raise BusError(f"cannot connect to NATS at {url}: {error.__class__.__name__}: {error}") from error
        js = nc.jetstream(timeout=self._timeout)
        created = False
        try:
            if create:
                await js.add_stream(
                    StreamConfig(
                        name=self.namespace,
                        subjects=[self._prefix + ">"],
                        storage=storage,
                        **self._limits,
                    )
                )
                created = True
            consumer = f"{self.namespace}-{uuid.uuid4().hex[:8]}"
            sub = await js.pull_subscribe(
                self._prefix + ">",
                durable=consumer,
                stream=self.namespace,
                config=ConsumerConfig(
                    ack_policy=AckPolicy.NONE, deliver_policy=DeliverPolicy.ALL, inactive_threshold=_CONSUMER_IDLE
                ),
            )
            self._js = js
            self._outbox: asyncio.Queue = asyncio.Queue()
            self._unacknowledged: set = set()
            self._ack_seq: dict[int, int] = {}  # own message number -> stream sequence it was stored at, until read back
            self._read_seq = 0  # the highest stream sequence read
            self._sender = asyncio.create_task(self._send_loop())
        except BaseException:
            # a failed open leaves nothing behind: not the stream this call made, not the connection
            try:
                if created:
                    await js.delete_stream(self.namespace)
            except BaseException:
                pass
            try:
                await nc.close()
            except BaseException:
                pass
            raise
        return nc, sub, consumer

    def _call(self, coroutine, timeout: float | None = None):
        timeout = self._timeout * 2 if timeout is None else timeout
        try:
            return self._io.call(coroutine, timeout)
        except BusError:
            raise
        except TimeoutError as error:
            raise BusError(f"NATS did not answer in time: {error.__class__.__name__}") from error
        except Exception as error:
            raise BusError(f"NATS: {error.__class__.__name__}: {error}") from error

    # Log

    def append(self, topic: str, data: bytes) -> None:
        self._appended += 1
        self._io.submit(self._outbox.put_nowait, (self._prefix + topic, data, self._appended))

    async def _send_loop(self) -> None:
        """Publishes what `append` hands over, in order. A refusal is kept; it is raised by the next read."""
        while True:
            subject, data, number = await self._outbox.get()
            try:
                ack = await self._js.publish_async(
                    subject, data, headers={_LOG_HEADER: self._id, _NUMBER_HEADER: str(number)})
            except Exception as error:
                self._fail(error)
                continue
            self._unacknowledged.add(ack)
            ack.add_done_callback(lambda done, number=number: self._acknowledged(done, number))

    def _acknowledged(self, ack, number: int) -> None:
        self._unacknowledged.discard(ack)
        if ack.cancelled() or ack.exception() is not None:
            if not ack.cancelled():
                self._fail(ack.exception())
        else:
            self._ack_seq[number] = ack.result().seq

    def _fail(self, error: BaseException) -> None:
        if self._failure is None:
            self._failure = BusError(f"NATS did not take a message: {error.__class__.__name__}: {error}")
            self._failure.__cause__ = error

    def read_new(self, wait: float = 0.0, complete: bool = True) -> list[tuple[int, str, bytes, int | None]]:
        return self._call(self._fetch(wait, complete), self._timeout * 2 + wait)

    async def _fetch(self, wait: float, complete: bool) -> list[tuple[int, str, bytes, int | None]]:
        entries: list[tuple[int, str, bytes, int | None]] = []
        sub = self._sub
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        while self._own_read < self._appended:
            # this log's own messages are on their way back: a request returns them (and what is behind them) as soon as stored
            if self._failure is not None:
                raise self._failure
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise BusError("NATS did not return the messages this log appended in time")
            try:
                messages = await sub.fetch(_BATCH, timeout=min(remaining, _SLICE))
            except TimeoutError:
                if await self._own_message_dropped(check_stream=True):
                    raise BusError(_LIMITS_MESSAGE) from None
                continue
            self._take(messages, entries)
            if self._own_read < self._appended and await self._own_message_dropped(check_stream=False):
                raise BusError(_LIMITS_MESSAGE)
        if self._failure is not None:
            raise self._failure
        while complete:
            pending = (await sub.consumer_info()).num_pending
            if pending == 0:
                if entries or wait <= 0:
                    break
                try:
                    messages = await sub.fetch(_BATCH, timeout=wait)
                except TimeoutError:
                    break
                self._take(messages, entries)
                break
            self._take(await sub.fetch(min(pending, _BATCH), timeout=self._timeout), entries)
        return entries

    def _take(self, messages, entries) -> None:
        prefix_length = len(self._prefix)
        for message in messages:
            headers = message.headers or {}
            own = None
            sequence = message.metadata.sequence.stream
            if headers.get(_LOG_HEADER) == self._id:
                own = int(headers[_NUMBER_HEADER])
                self._own_read += 1
                self._ack_seq.pop(own, None)
                if own != self._own_read:
                    raise BusError(_LIMITS_MESSAGE)
            self._read_seq = sequence
            entries.append((sequence, message.subject[prefix_length:], message.data, own))

    async def _own_message_dropped(self, check_stream: bool) -> bool:
        """Whether the next own message this log waits for was stored (acknowledged) and is gone: the stream's limits dropped
        it before it was read. Known without a request when a later message has been read; with one (`check_stream`)
        when the stream's first message is later than where it was stored."""
        stored = self._ack_seq.get(self._own_read + 1)
        if stored is None:
            return False
        if self._read_seq > stored:
            return True
        return check_stream and (await self._js.stream_info(self.namespace)).state.first_seq > stored

    def close(self) -> None:
        """Sends and waits for what was appended, deletes the consumer (and the stream, `drop_on_close`), closes the
        connection and the loop, then raises the first failure: a message the server refused or did not acknowledge, or
        a cleanup that failed. Idempotent: the second call does nothing."""
        if self._closed:
            return
        self._closed = True
        failure = None
        try:
            failure = self._call(self._shutdown(), self._timeout * 3)
        except BusError as error:
            failure = error
        finally:
            self._io.stop()
        if failure is not None:
            raise failure

    async def _shutdown(self) -> BusError | None:
        """Everything is closed whatever fails; returns the first failure instead of raising it."""
        loop = asyncio.get_running_loop()
        end = loop.time() + self._timeout
        while (not self._outbox.empty() or self._unacknowledged) and loop.time() < end:
            await asyncio.sleep(0.002)
        failure = self._failure
        if failure is None and (not self._outbox.empty() or self._unacknowledged):
            failure = BusError("NATS did not acknowledge the messages this log appended before it closed")
        self._sender.cancel()
        await asyncio.gather(self._sender, return_exceptions=True)
        try:
            try:
                await self._js.delete_consumer(self.namespace, self._consumer)
                if self._drop_on_close:
                    await self._js.delete_stream(self.namespace)
            except NotFoundError:
                pass
            except Exception as error:
                failure = failure or BusError(f"NATS cleanup failed: {error.__class__.__name__}: {error}")
        finally:
            try:
                await self._nc.close()
            except Exception as error:
                failure = failure or BusError(f"NATS close failed: {error.__class__.__name__}: {error}")
        return failure
