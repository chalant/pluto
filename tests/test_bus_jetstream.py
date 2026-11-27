"""The log-backed bus on NATS JetStream (pluto/bus/log.py, pluto/bus/jetstream.py): what the shared contract
(tests/test_bus_contract.py) cannot say because it needs a broker: publish does not wait, positions resolve later, a bus
attached to the same stream (another process) resumes from a position and sees what others write, a lost message is an
error, the order of S3 is reproduced across processes when the producers are waited for.

They need a NATS server with JetStream at ``PLUTO_NATS_URL`` (default nats://127.0.0.1:4222) and are skipped without one
(failures with ``PLUTO_REQUIRE_NATS=1``, see ``tests/nats_support.py``). Each test makes its own stream and deletes it.
"""
import gc
import multiprocessing
import threading
import time
import unittest
import uuid
import warnings
from unittest import mock

from tests import nats_support

nats_support.require_module()

import nats  # noqa: E402

from pluto.bus import BusError, JsonCodec, Position, SerializingBus, Start  # noqa: E402
from pluto.bus.jetstream import DEFAULT_URL, JetStreamLog, server_available  # noqa: E402
from pluto.bus.log import LogBus  # noqa: E402


def put(bus, topic='t', payload=b'x', **fields):
    fields.setdefault('logical_ts', 0)
    fields.setdefault('producer_id', 'p')
    return bus.publish(topic, payload, type='t', **fields)


class ServerTests(unittest.TestCase):
    def test_no_server_is_said_at_once(self):
        self.assertFalse(server_available('nats://127.0.0.1:1', timeout=0.2))

        with self.assertRaisesRegex(BusError, 'cannot connect to NATS'):
            JetStreamLog(url='nats://127.0.0.1:1')


@nats_support.needs_server
class JetStreamBusTests(unittest.TestCase):
    def log(self, *args, **kwargs):
        log = JetStreamLog(*args, **kwargs)
        self.addCleanup(log.close)
        return log

    def bus(self, log=None, **options):
        bus = LogBus(self.log(drop_on_close=True) if log is None else log, **options)
        self.addCleanup(bus.close)
        return bus

    def attach(self, bus):
        return self.bus(self.log(bus._log.namespace, create=False))

    def test_publish_does_not_wait_for_the_broker_and_the_position_resolves_later(self):
        bus = self.bus()
        receipts = [put(bus, payload=b'%d' % i) for i in range(50)]

        self.assertFalse(any(receipt.resolved for receipt in receipts))

        # asking for the position waits for the acknowledgement of that message and of the ones before it
        position = receipts[10].position

        self.assertEqual(position.topic, 't')
        self.assertTrue(all(receipt.resolved for receipt in receipts))
        self.assertEqual([r.position.token for r in receipts], list(range(1, 51)))

    def test_the_next_read_resolves_the_positions(self):
        bus = self.bus()
        receipts = [put(bus) for _ in range(3)]

        bus.run_until_idle()

        self.assertTrue(all(receipt.resolved for receipt in receipts))

    def test_a_receipt_position_starts_a_replay_on_another_bus_of_the_stream(self):
        first = self.bus()
        receipts = [put(first, payload=b'%d' % i, logical_ts=i) for i in range(5)]
        second = self.attach(first)
        seen = []

        second.subscribe('t', lambda d: seen.append(d.envelope.payload), start=receipts[3].position)
        second.run_until_idle()

        self.assertEqual(seen, [b'3', b'4'])

    def test_resume_from_the_stored_position_of_a_delivery_on_a_new_bus_of_the_stream(self):
        first = self.bus()
        seen, checkpoints = [], []
        first.subscribe('t', lambda d: seen.append(int(d.envelope.payload)))
        first.subscribe('t', lambda d: checkpoints.append(first.position_of(d)), rank=9)

        for i in range(10):
            put(first, payload=b'%d' % i, logical_ts=i)

        for _ in range(10):  # five messages, two handlers each
            first.step()

        self.assertEqual(seen, [0, 1, 2, 3, 4])

        # a restarted process: a new bus on the same stream, from the position stored with its state
        second = self.attach(first)
        again, everything = [], []
        second.subscribe('t', lambda d: again.append(int(d.envelope.payload)), start=checkpoints[4])
        second.subscribe('t', lambda d: everything.append(int(d.envelope.payload)), start=Start.EARLIEST)
        second.run_until_idle()

        self.assertEqual(again, [5, 6, 7, 8, 9])
        self.assertEqual(everything, list(range(10)))

    def test_a_position_that_is_not_in_the_log_is_refused(self):
        bus = self.bus()
        put(bus)
        bus.run_until_idle()

        with self.assertRaises(ValueError):
            bus.subscribe('t', lambda d: None, start=Position('t', 10 ** 6))

    def test_two_buses_on_one_stream_see_each_others_messages_in_the_order_of_s3(self):
        a = self.bus()
        b = self.attach(a)
        got_a, got_b = [], []
        a.subscribe('t', lambda d: got_a.append(d.envelope.producer_id))
        b.subscribe('t', lambda d: got_b.append(d.envelope.producer_id))
        # two lanes (keys): within one lane the order is the log's, S3 orders across lanes
        put(a, producer_id='late', logical_ts=2, key='a')
        put(b, producer_id='early', logical_ts=1, key='b')

        # what a controller does: wait until what it expects is in the log, then deliver
        for bus in (a, b):
            deadline = time.monotonic() + 10

            while len(bus._logs.get('t', ())) < 2:
                self.assertLess(time.monotonic(), deadline)
                bus.poll(0.5)
        a.run_until_idle()
        b.run_until_idle()

        self.assertEqual(got_a, ['early', 'late'])
        self.assertEqual(got_b, ['early', 'late'])

    def test_poll_waits_up_to_its_timeout_for_a_message_and_gives_up(self):
        bus = self.bus()
        started = time.monotonic()

        self.assertFalse(bus.poll(0.3))

        self.assertGreaterEqual(time.monotonic() - started, 0.25)
        self.assertLess(time.monotonic() - started, 5)

    def test_poll_returns_when_another_bus_publishes(self):
        a = self.bus()
        b = self.attach(a)
        started = time.monotonic()

        put(b, producer_id='other')

        self.assertTrue(a.poll(10))
        self.assertLess(time.monotonic() - started, 5)

    def test_what_this_bus_published_is_a_message_to_read_not_news_for_poll(self):
        bus = self.bus()
        put(bus)

        bus.run_until_idle()

        self.assertFalse(bus.poll(0))

    def test_a_message_the_broker_refused_raises_on_the_next_read_and_the_bus_stays_failed(self):
        log = self.log(drop_on_close=False)
        bus = self.bus(log)
        bus.subscribe('t', lambda d: None)

        async def delete():
            nc = await nats.connect(DEFAULT_URL)
            await nc.jetstream().delete_stream(log.namespace)
            await nc.close()

        log._io.call(delete(), 10)  # the stream goes away under the bus: nobody takes what it sends
        receipt = put(bus)  # does not wait, so it does not know

        with self.assertRaises(BusError):
            bus.run_until_idle()

        with self.assertRaises(BusError):
            bus.step()
        with self.assertRaises(BusError):
            receipt.position

        with self.assertRaises(BusError):  # the failure is not swallowed at the end either; the cleanup was done
            bus.close()

        bus.close()  # idempotent

    def test_close_raises_a_message_the_broker_refused_that_nobody_read_back(self):
        log = self.log(drop_on_close=False)
        bus = LogBus(log)

        async def delete():
            nc = await nats.connect(DEFAULT_URL)
            await nc.jetstream().delete_stream(log.namespace)
            await nc.close()

        log._io.call(delete(), 10)
        put(bus)  # the last message of a run, never read

        with self.assertRaises(BusError):
            bus.close()

        bus.close()

    def test_a_failed_attach_leaves_nothing_behind(self):
        threads = {t for t in threading.enumerate() if t.name == 'jetstream-log'}

        with warnings.catch_warnings():
            warnings.simplefilter('error')  # an unclosed loop or a task destroyed while pending would show here

            with self.assertRaisesRegex(BusError, 'NATS'):
                JetStreamLog('nostream' + uuid.uuid4().hex[:8], create=False)

            gc.collect()

        self.assertEqual({t for t in threading.enumerate() if t.name == 'jetstream-log'}, threads)

    def test_a_failed_open_deletes_the_stream_it_created(self):
        namespace = 'failed' + uuid.uuid4().hex[:8]

        async def refuse(*args, **kwargs):
            raise RuntimeError('no consumer for you')

        with mock.patch('nats.js.client.JetStreamContext.pull_subscribe', refuse):
            with self.assertRaises(BusError):
                JetStreamLog(namespace)

        with self.assertRaises(BusError):  # the stream is gone: attaching to it fails
            JetStreamLog(namespace, create=False)

    def test_closing_sends_what_was_published(self):
        log = self.log(drop_on_close=False)
        bus = LogBus(log)
        put(bus, payload=b'last')
        namespace = log.namespace
        bus.close()

        reader = self.bus(self.log(namespace, create=False, drop_on_close=True))
        seen = []
        reader.subscribe('t', lambda d: seen.append(d.envelope.payload), start=Start.EARLIEST)
        reader.run_until_idle()

        self.assertEqual(seen, [b'last'])

    def test_the_bus_carries_bytes_only(self):
        bus = self.bus()

        with self.assertRaises(TypeError):
            bus.publish('t', {'not': 'bytes'}, type='t', logical_ts=0, producer_id='p')

    def test_a_serialising_bus_over_the_stream_says_it_is_networked(self):
        bus = SerializingBus(self.bus(), JsonCodec())

        self.assertTrue(bus.networked)


def _producer(namespace, name, count, go, finished):
    bus = LogBus(JetStreamLog(namespace, create=False))
    go.wait()

    for i in range(count):
        put(bus, producer_id=name, key=name, logical_ts=i // 3)

    bus.run_until_idle()  # the acknowledgements
    finished.put(name)
    bus.close()


@nats_support.needs_server
class AcrossProcessesTests(unittest.TestCase):
    def test_the_order_of_s3_is_reproduced_when_the_consumer_waits_for_the_producers(self):
        """Four processes publish messages of the same logical times: arrival order differs from run to run, the
        delivery order after a barrier (all producers done) is the order of S3, always."""
        count, names = 20, ['p0', 'p1', 'p2', 'p3']
        traces = []

        for _ in range(3):
            consumer_log = JetStreamLog(drop_on_close=True)
            consumer = LogBus(consumer_log)
            seen = []
            consumer.subscribe('t', lambda d: seen.append((d.envelope.logical_ts, d.envelope.producer_id, d.envelope.seq)))
            context = multiprocessing.get_context('spawn')
            go, finished = context.Event(), context.Queue()
            processes = [
                context.Process(
                    target=_producer, args=(consumer_log.namespace, name, count, go, finished), daemon=True)
                for name in names]

            try:
                for process in processes:
                    process.start()

                time.sleep(1)
                go.set()

                for _ in names:
                    finished.get(timeout=60)

                deadline = time.monotonic() + 30

                while len(consumer._logs.get('t', ())) < count * len(names):
                    self.assertLess(time.monotonic(), deadline, 'the producers messages did not arrive')
                    consumer.poll(0.5)

                consumer.run_until_idle()
            finally:
                for process in processes:
                    process.join(10)

                    if process.is_alive():  # a failed test leaves no producer behind
                        process.terminate()
                        process.join(5)

                consumer.close()

            traces.append(seen)

        self.assertEqual(len(traces[0]), count * len(names))
        self.assertEqual(traces[0], sorted(traces[0]))
        self.assertEqual(traces[0], traces[1])
        self.assertEqual(traces[1], traces[2])


if __name__ == '__main__':
    unittest.main()
