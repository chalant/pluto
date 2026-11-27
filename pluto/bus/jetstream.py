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

JetStream stores to disk with a periodic fsync (`sync_interval`, two minutes by default on the server): a crash of the
machine can lose acknowledged messages unless the server is configured otherwise. Retention is the stream's: unlimited here.
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


class _Loop:
    """An asyncio event loop in a thread, for the synchronous callers."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="jetstream-log", daemon=True)
        self._thread.start()

    def call(self, coroutine: Coroutine, timeout: float | None) -> Any:
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(timeout)

    def submit(self, function, *args) -> None:
        """Run `function` on the loop's thread, without waiting."""
        self._loop.call_soon_threadsafe(function, *args)

    def stop(self) -> None:
        if self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(5)


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
    """

    def __init__(
        self,
        namespace: str | None = None,
        *,
        create: bool = True,
        drop_on_close: bool = False,
        storage: StorageType = StorageType.FILE,
        url: str = DEFAULT_URL,
        timeout: float = 30.0,
    ) -> None:
        self.namespace = namespace or "pluto" + uuid.uuid4().hex[:12]
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
        if create:
            await js.add_stream(StreamConfig(name=self.namespace, subjects=[self._prefix + ">"], storage=storage))
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
        self._sender = asyncio.create_task(self._send_loop())
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

    def append(self, topic: str, key: str, data: bytes) -> None:
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
            ack.add_done_callback(self._acknowledged)

    def _acknowledged(self, ack) -> None:
        self._unacknowledged.discard(ack)
        if not ack.cancelled() and ack.exception() is not None:
            self._fail(ack.exception())

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
                continue
            self._take(messages, entries)
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
            if headers.get(_LOG_HEADER) == self._id:
                own = int(headers[_NUMBER_HEADER])
                self._own_read += 1
                if own != self._own_read:
                    raise BusError("NATS stored the messages of this log out of order")
            entries.append((message.metadata.sequence.stream, message.subject[prefix_length:], message.data, own))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._call(self._shutdown())
        except BusError:
            pass
        finally:
            self._io.stop()

    async def _shutdown(self) -> None:
        # what was appended is sent and acknowledged before the connection goes
        loop = asyncio.get_running_loop()
        end = loop.time() + self._timeout
        while (not self._outbox.empty() or self._unacknowledged) and loop.time() < end:
            await asyncio.sleep(0.002)
        self._sender.cancel()
        await asyncio.gather(self._sender, return_exceptions=True)
        try:
            await self._js.delete_consumer(self.namespace, self._consumer)
            if self._drop_on_close:
                await self._js.delete_stream(self.namespace)
        except NotFoundError:
            pass
        finally:
            await self._nc.close()
