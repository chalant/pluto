"""Retention of the logs (`Retention`, `Bus.trim`, `Bus.oldest`, `PositionTooOld`), on every bus that keeps a log: the in-memory
bus, `SerializingBus(InMemoryBus)` and `SerializingBus(LogBus(JetStreamLog))` (skipped without a NATS server; see
`tests/nats_support.py`). They are not part of `tests/test_bus_contract.py` because the fault-injecting wrapper's contract run
(`tests/test_bus_faults_contract.py`) lists which of that file's tests fail under which fault.

The rules: a window keeps at least the newest `max_messages` entries per topic and never loses a delivery (a subscription holds
the messages it has not settled); a position older than what is kept, and `Start.EARLIEST` after a trim, fail with
`PositionTooOld` instead of replaying less; `trim` is the owner's low-water mark, clamped to what a live subscription may still
resume from; the default keeps everything.
"""
import asyncio
import time
import unittest
import uuid

from pluto.bus import BusError, InMemoryBus, Position, PositionTooOld, Retention, Start, reply, request
from pluto.bus.faults import FaultConfig, FaultKind, SeededFaultBus
from pluto.bus.log import LogBus
from tests import nats_support
from tests.test_bus_contract import Recorder, make_memory, make_nats, make_serializing, publish

WINDOW = 5


def payloads(bus, start, topic='t'):
    """What a new subscription at `start` is delivered, in order."""
    seen = []
    bus.subscribe(topic, lambda delivery: seen.append(delivery.envelope.payload), start=start)
    bus.run_until_idle()
    return seen


class RetentionContract:
    make_bus = None

    def bus(self, **options):
        bus = type(self).make_bus(**options)
        self.addCleanup(bus.close)
        return bus

    def windowed(self, count=40, topic='t', window=WINDOW):
        bus = self.bus(retention=Retention(max_messages=window))
        receipts = [publish(bus, topic, i, ts=i, msg_id='m%d' % i) for i in range(count)]
        return bus, receipts

    def test_by_default_nothing_is_trimmed_and_replay_is_unlimited(self):
        bus = self.bus()
        receipts = [publish(bus, 't', i, ts=i) for i in range(50)]

        self.assertEqual(payloads(bus, Start.EARLIEST), list(range(50)))
        self.assertEqual(payloads(bus, bus.oldest('t')), list(range(50)))
        self.assertEqual(payloads(bus, receipts[10].position), list(range(10, 50)))

    def test_a_window_keeps_at_least_its_size_and_drops_the_oldest(self):
        bus, receipts = self.windowed()

        kept = payloads(bus, bus.oldest('t'))

        self.assertIn(len(kept), (WINDOW, WINDOW + WINDOW // 4))
        self.assertEqual(kept, list(range(40 - len(kept), 40)))

    def test_a_position_older_than_the_window_fails_clearly(self):
        bus, receipts = self.windowed()
        oldest = bus.oldest('t')

        with self.assertRaises(PositionTooOld) as raised:
            bus.subscribe('t', lambda delivery: None, start=receipts[0].position)

        self.assertEqual(raised.exception.position, receipts[0].position)
        self.assertEqual(raised.exception.oldest, oldest)
        self.assertIsInstance(raised.exception, BusError)
        # nothing was subscribed: the failed call changed nothing
        self.assertEqual(bus.oldest('t'), oldest)

    def test_the_whole_log_is_not_there_for_earliest_once_it_was_trimmed(self):
        bus, _ = self.windowed()

        with self.assertRaises(PositionTooOld) as raised:
            bus.subscribe('t', lambda delivery: None, start=Start.EARLIEST)

        self.assertIsNone(raised.exception.position)
        self.assertEqual(raised.exception.oldest, bus.oldest('t'))

    def test_what_is_left_replays_from_the_oldest_and_from_a_retained_position(self):
        bus, receipts = self.windowed()

        self.assertEqual(payloads(bus, receipts[-2].position), [38, 39])
        self.assertEqual(payloads(bus, receipts[-1].position), [39])
        self.assertEqual(payloads(bus, bus.oldest('t'))[-1], 39)

    def test_latest_and_the_end_of_the_log_are_unaffected(self):
        bus, receipts = self.windowed()
        seen = []
        bus.subscribe('t', lambda delivery: seen.append(delivery.envelope.payload))
        publish(bus, 't', 40, ts=40)
        bus.run_until_idle()

        self.assertEqual(seen, [40])

    def test_a_window_is_per_topic(self):
        bus, _ = self.windowed(topic='a')
        for i in range(3):
            publish(bus, 'b', i, ts=i)

        self.assertEqual(payloads(bus, Start.EARLIEST, 'b'), [0, 1, 2])  # nothing of b was trimmed

        with self.assertRaises(PositionTooOld):
            bus.subscribe('a', lambda delivery: None, start=Start.EARLIEST)
        with self.assertRaises(PositionTooOld):  # a pattern that covers a trimmed topic is not the whole log either
            bus.subscribe('>', lambda delivery: None, start=Start.EARLIEST)

    def test_a_window_never_loses_a_delivery(self):
        bus = self.bus(retention=Retention(max_messages=WINDOW))
        seen = []
        bus.subscribe('t', lambda delivery: seen.append(delivery.envelope.payload))
        for i in range(60):  # far more than the window, nothing run yet
            publish(bus, 't', i, ts=i)

        bus.run_until_idle()

        self.assertEqual(seen, list(range(60)))

    def test_a_checkpoint_the_window_passed_fails_clearly_and_a_recent_one_resumes(self):
        bus = self.bus(retention=Retention(max_messages=WINDOW))
        positions = []
        bus.subscribe('t', lambda delivery: positions.append(bus.position_of(delivery)))
        for i in range(60):
            publish(bus, 't', i, ts=i)
        bus.run_until_idle()

        with self.assertRaises(PositionTooOld):
            bus.subscribe('t', lambda delivery: None, start=positions[0])

        publish(bus, 't', 60, ts=60)
        publish(bus, 't', 61, ts=61)
        self.assertEqual(payloads(bus, positions[-1]), [60, 61])  # the checkpoint after the last handled message

    def test_a_key_keeps_its_group_owner_after_its_lane_went_idle_and_another_member_joined(self):
        # the owner of a lane is chosen when it is created (a stable hash over the members then) and stays; freeing an idle
        # lane must not make the next message of the key choose again among the members that joined since
        bus = self.bus()
        first, second = Recorder(), Recorder()
        bus.subscribe('t', first.handler('first'), group='g')
        keys = ['k%d' % i for i in range(12)]
        for i, key in enumerate(keys):
            publish(bus, 't', i, ts=i, key=key)
        bus.run_until_idle()
        self.assertEqual(len(first.calls), 12)  # the only member took every key

        bus.subscribe('t', second.handler('second'), group='g')
        for i, key in enumerate(keys):
            publish(bus, 't', 100 + i, ts=100 + i, key=key)
        bus.run_until_idle()

        self.assertEqual(len(first.calls), 24)
        self.assertEqual(second.calls, [])

    def test_replies_work_under_a_window(self):
        bus = self.bus(retention=Retention(max_messages=2))
        bus.subscribe(
            'svc', lambda delivery: reply(bus, delivery, delivery.envelope.payload, type='t', logical_ts=0, producer_id='svc'))
        pending = [request(bus, 'svc', i, type='t', logical_ts=i, producer_id='p', msg_id='p:%d' % i) for i in range(10)]
        bus.run_until_idle()

        self.assertEqual([p.reply.payload for p in pending], list(range(10)))

    # the explicit low-water mark

    def test_trim_drops_what_is_below_the_position_and_nothing_else(self):
        bus = self.bus()
        bus.subscribe('t', lambda delivery: None)
        receipts = [publish(bus, 't', i, ts=i) for i in range(10)]
        bus.run_until_idle()  # the live subscription has settled everything

        oldest = bus.trim(receipts[6].position)

        self.assertEqual(oldest, receipts[6].position)
        self.assertEqual(bus.oldest('t'), oldest)
        self.assertEqual(payloads(bus, oldest), [6, 7, 8, 9])
        with self.assertRaises(PositionTooOld):
            bus.subscribe('t', lambda delivery: None, start=receipts[3].position)
        with self.assertRaises(PositionTooOld):
            bus.subscribe('t', lambda delivery: None, start=Start.EARLIEST)

    def test_trim_below_what_is_already_trimmed_does_nothing(self):
        bus = self.bus()
        receipts = [publish(bus, 't', i, ts=i) for i in range(10)]
        self.assertEqual(bus.trim(receipts[6].position), receipts[6].position)

        self.assertEqual(bus.trim(receipts[2].position), receipts[6].position)

    def test_trim_is_held_back_by_a_live_subscription_that_has_not_settled_the_messages(self):
        bus = self.bus()
        receipts = [publish(bus, 't', i, ts=i) for i in range(10)]
        before = bus.oldest('t')
        seen = []
        bus.subscribe('t', lambda delivery: seen.append(delivery.envelope.payload), start=Start.EARLIEST)

        # it has all ten queued and handled none: it may still resume from the start
        self.assertEqual(bus.trim(receipts[8].position), before)

        bus.step()
        bus.step()  # two settled: it can now resume from offset 2
        oldest = bus.trim(receipts[8].position)

        self.assertNotEqual(oldest, before)
        self.assertLess(oldest.token, receipts[8].position.token)
        bus.run_until_idle()
        self.assertEqual(seen, list(range(10)))  # and nothing it still had to deliver was lost

        self.assertEqual(bus.trim(receipts[8].position), receipts[8].position)

    def test_trim_refuses_what_is_not_a_position_of_the_log(self):
        bus = self.bus()
        publish(bus, 't', 0)

        with self.assertRaises(ValueError):
            bus.trim(Position('t', 10 ** 6))
        with self.assertRaises(ValueError):
            bus.trim(Position('nobody', 5))
        with self.assertRaises(ValueError):
            bus.trim(Position('a.*', 0))
        with self.assertRaises(TypeError):
            bus.trim(('t', 0))

    def test_trim_of_a_topic_without_a_log_is_nothing(self):
        bus = self.bus()

        self.assertEqual(bus.trim(bus.oldest('nobody')), bus.oldest('nobody'))

    def test_trim_of_a_topic_without_a_log_accepts_only_its_oldest_whatever_the_rest_of_the_log_holds(self):
        bus = self.bus()
        for i in range(10):  # other topics have made the log long: positions of the log are big numbers
            publish(bus, 'other', i, ts=i)
        bus.run_until_idle()

        for token in (1, 5, 11):
            with self.assertRaises(ValueError, msg=token):
                bus.trim(Position('nobody', token))

        self.assertEqual(bus.trim(bus.oldest('nobody')), bus.oldest('nobody'))

    # what a window or a trim must not disturb

    def test_a_nacked_message_is_redelivered_and_a_dead_lettered_one_is_recorded_whatever_the_window(self):
        bus = self.bus(retention=Retention(max_messages=2), max_attempts=2)
        seen = []

        def flaky(delivery):
            seen.append((delivery.envelope.payload, delivery.attempt))
            if delivery.attempt == 1 and delivery.envelope.payload % 2 == 0:
                raise RuntimeError('first attempt of an even one')
            if delivery.envelope.payload == 7:
                raise RuntimeError('always')

        bus.subscribe('t', flaky)
        for i in range(20):
            publish(bus, 't', i, ts=i, key='k%d' % (i % 3))
        bus.run_until_idle()

        self.assertEqual(sorted({payload for payload, _ in seen}), list(range(20)))
        self.assertEqual([attempt for payload, attempt in seen if payload % 2 == 0 and payload != 7], [1, 2] * 10)
        self.assertEqual([letter.envelope.payload for letter in bus.dead_letters()], [7])

    def test_group_members_get_every_message_once_under_a_window(self):
        bus = self.bus(retention=Retention(max_messages=3))
        first, second = Recorder(), Recorder()
        bus.subscribe('t', first.handler('first'), group='g')
        bus.subscribe('t', second.handler('second'), group='g')
        for i in range(60):
            publish(bus, 't', i, ts=i, key='k%d' % (i % 6), msg_id='m%d' % i)
        bus.run_until_idle()

        self.assertEqual(sorted(first.ids() + second.ids()), sorted('m%d' % i for i in range(60)))
        self.assertTrue(first.calls and second.calls)

    def test_trim_from_inside_a_handler_loses_nothing(self):
        bus = self.bus()
        seen = []

        def handler(delivery):
            seen.append(delivery.envelope.payload)
            bus.trim(bus.position_of(delivery))  # the owner advances its low-water mark as it checkpoints

        bus.subscribe('t', handler)
        for i in range(30):
            publish(bus, 't', i, ts=i)
        bus.run_until_idle()

        self.assertEqual(seen, list(range(30)))
        with self.assertRaises(PositionTooOld):
            bus.subscribe('t', lambda delivery: None, start=Start.EARLIEST)

    def test_a_closed_bus_refuses_trim_and_oldest(self):
        bus = type(self).make_bus()
        bus.close()

        for call in (lambda: bus.oldest('t'), lambda: bus.trim(Position('t', 0))):
            with self.assertRaises(BusError):
                call()


class FaultBusRetention(unittest.TestCase):
    """A crashed subscription restarts at its checkpoint (`SeededFaultBus`): past the window that is a loud end of the run."""

    def faulty(self, window):
        bus = SeededFaultBus(InMemoryBus(retention=Retention(max_messages=window)), 1, FaultConfig(crash=1.0, max_crashes=3))
        self.addCleanup(bus.close)
        return bus

    def test_a_restart_whose_checkpoint_the_window_passed_aborts_the_run_it_is_not_a_dead_letter(self):
        bus = self.faulty(WINDOW)
        seen = []
        bus.subscribe('t', lambda delivery: seen.append(delivery.envelope.payload))
        for i in range(30):  # the checkpoint (before the first message) is far older than the window
            publish(bus, 't', i, ts=i)

        with self.assertRaises(PositionTooOld):
            bus.run_until_idle()

        self.assertEqual(bus.dead_letters(), ())  # not a handler error that was retried and given up on
        self.assertIn(FaultKind.CRASH_SKIPPED, [fault.kind for fault in bus.fault_log()])
        for call in (bus.step, bus.run_until_idle, bus.poll, lambda: publish(bus, 't', 99)):
            with self.assertRaises(PositionTooOld):  # and the bus stays stopped
                call()

    def test_a_window_larger_than_the_messages_since_the_checkpoint_lets_the_restart_resume(self):
        bus = self.faulty(100)
        seen = []
        bus.subscribe('t', lambda delivery: seen.append(delivery.envelope.payload))
        for i in range(30):
            publish(bus, 't', i, ts=i)

        bus.run_until_idle()

        self.assertEqual(bus.dead_letters(), ())
        self.assertEqual(sorted(set(seen)), list(range(30)))  # a crash replays what it had handled, nothing is missing
        self.assertTrue(any(fault.kind is FaultKind.CRASH for fault in bus.fault_log()))


class InMemoryRetention(RetentionContract, unittest.TestCase):
    make_bus = staticmethod(make_memory)

    def test_the_log_is_bounded_by_the_window_not_by_the_run(self):
        bus, _ = self.windowed(count=5000, window=100)

        self.assertLessEqual(len(bus._logs['t']), 125)
        self.assertGreaterEqual(len(bus._logs['t']), 100)
        self.assertEqual(bus.oldest('t').token + len(bus._logs['t']), 5000)

    def test_an_exact_window_when_it_has_no_slack(self):
        bus, _ = self.windowed(count=9, window=3)

        self.assertEqual(len(bus._logs['t']), 3)
        self.assertEqual(bus.oldest('t'), Position('t', 6))

    def test_offsets_stay_absolute_after_a_trim(self):
        bus = self.bus(retention=Retention(max_messages=2))
        positions = []
        bus.subscribe('t', lambda delivery: positions.append((delivery.offset, bus.position_of(delivery))))
        for i in range(10):
            publish(bus, 't', i, ts=i)
        bus.run_until_idle()

        self.assertEqual([offset for offset, _ in positions], list(range(10)))
        self.assertEqual(positions[-1][1], Position('t', 10))

    def test_idle_lanes_are_freed(self):
        bus = self.bus()
        subscription = bus.subscribe('t', Recorder().handler('s'))
        for i in range(200):
            publish(bus, 't', i, ts=i, key='k%d' % i)
        bus.run_until_idle()

        self.assertEqual(len(bus._consumers[0].lanes), 0)
        # and a lane that comes back is a new one with its place in the order
        publish(bus, 't', 1, ts=300, key='k1')
        self.assertEqual(bus.run_until_idle(), 1)
        subscription.cancel()

    def test_the_retention_is_validated(self):
        for bad in (0, -1, 1.5, True, '5'):
            with self.assertRaises(ValueError):
                Retention(max_messages=bad)

        with self.assertRaises(TypeError):
            InMemoryBus(retention=5)

        self.assertIsNone(Retention().max_messages)


class SerializingRetention(RetentionContract, unittest.TestCase):
    make_bus = staticmethod(make_serializing)


@nats_support.needs_server
class JetStreamRetention(RetentionContract, unittest.TestCase):
    make_bus = staticmethod(make_nats)

    def test_the_mirror_is_bounded_with_its_tokens(self):
        bus, _ = self.windowed(count=3000, window=100)
        bus.run_until_idle()  # reads what was published back
        inner = bus._inner

        self.assertEqual(len(inner._tokens['t']), len(inner._logs['t']))
        self.assertLessEqual(len(inner._logs['t']), 125)
        self.assertEqual(inner._tokens['t'], sorted(inner._tokens['t']))

    def test_a_bus_that_attaches_after_the_streams_limits_dropped_the_start_cannot_replay_it(self):
        jetstream = nats_support.jetstream
        log = jetstream.JetStreamLog(max_messages=WINDOW, drop_on_close=True)
        first = LogBus(log)
        self.addCleanup(first.close)
        receipts = []
        for i in range(20):  # read back between publishes: the stream's limit may not drop what was not read yet
            receipts.append(first.publish('t', b'%d' % i, type='t', logical_ts=i, producer_id='p'))
            first.run_until_idle()
        second = LogBus(jetstream.JetStreamLog(log.namespace, create=False))
        self.addCleanup(second.close)

        # the server kept the last WINDOW messages; the first bus's own mirror has all of them (it is not the stream)
        self.assertEqual(payloads(first, Start.EARLIEST), [b'%d' % i for i in range(20)])
        with self.assertRaises(PositionTooOld) as raised:
            second.subscribe('t', lambda delivery: None, start=Start.EARLIEST)
        self.assertEqual(raised.exception.oldest, second.oldest('t'))
        with self.assertRaises(PositionTooOld):
            second.subscribe('t', lambda delivery: None, start=receipts[3].position)
        self.assertEqual(payloads(second, second.oldest('t')), [b'%d' % i for i in range(20 - WINDOW, 20)])

    def test_limits_smaller_than_a_burst_are_a_clear_error_not_a_silent_loss(self):
        log = nats_support.jetstream.JetStreamLog(max_messages=WINDOW, drop_on_close=True)
        bus = LogBus(log)
        for i in range(20):  # twenty messages before the first read: the stream keeps five
            bus.publish('t', b'x', type='t', logical_ts=i, producer_id='p')

        with self.assertRaisesRegex(BusError, 'limits'):
            bus.run_until_idle()

        with self.assertRaises(BusError):  # and the bus stays failed, like for a refused message
            bus.step()
        with self.assertRaises(BusError):
            bus.close()

    def closing(self, bus):
        """Closes the bus at the end of the test, whether or not it failed (a failed bus raises its error at close)."""
        def close():
            try:
                bus.close()
            except BusError:
                pass

        self.addCleanup(close)
        return bus

    def test_entries_of_another_writer_that_the_limits_dropped_fail_the_reader_instead_of_replaying_less(self):
        jetstream = nats_support.jetstream
        log = jetstream.JetStreamLog(max_messages=WINDOW, drop_on_close=True)
        a = self.closing(LogBus(log))
        b = self.closing(LogBus(jetstream.JetStreamLog(log.namespace, create=False)))
        a.publish('t', b'a0', type='t', logical_ts=0, producer_id='a')
        a.run_until_idle()  # a has read its message: position 1
        for i in range(20):  # b writes and reads back each one; the stream keeps the last five
            b.publish('t', b'b%d' % i, type='t', logical_ts=i, producer_id='b')
            b.run_until_idle()
        seen = []

        # what a would replay from the start is a0 and b15 to b19: less than the log held, with no error
        with self.assertRaisesRegex(BusError, 'dropped entries.*limits'):
            a.subscribe('t', lambda delivery: seen.append(delivery.envelope.payload), start=Start.EARLIEST)

        self.assertEqual(seen, [])
        with self.assertRaises(BusError):  # and the bus stays failed
            a.run_until_idle()

    def test_an_own_message_that_other_writers_pushed_out_of_the_stream_names_the_limits(self):
        jetstream = nats_support.jetstream
        log = jetstream.JetStreamLog(max_messages=WINDOW, drop_on_close=True)
        first = self.closing(LogBus(log))
        other = self.closing(LogBus(jetstream.JetStreamLog(log.namespace, create=False)))
        first.publish('t', b'mine', type='t', logical_ts=0, producer_id='a')
        time.sleep(0.3)  # stored, not read back
        for i in range(20):
            other.publish('t', b'b%d' % i, type='t', logical_ts=i, producer_id='b')
            other.run_until_idle()
        started = time.monotonic()

        with self.assertRaisesRegex(BusError, 'limits'):
            first.run_until_idle()

        self.assertLess(time.monotonic() - started, 10)  # not the generic 30 s wait

    def test_a_stream_without_limits_is_replayed_whole_by_a_late_bus(self):
        jetstream = nats_support.jetstream
        log = jetstream.JetStreamLog(drop_on_close=True)
        first = LogBus(log)
        self.addCleanup(first.close)
        for i in range(20):
            first.publish('t', b'%d' % i, type='t', logical_ts=i, producer_id='p')
        first.run_until_idle()
        second = LogBus(jetstream.JetStreamLog(log.namespace, create=False))
        self.addCleanup(second.close)

        self.assertEqual(payloads(second, Start.EARLIEST), [b'%d' % i for i in range(20)])

    def test_the_stream_limits_are_validated(self):
        for name in ('max_messages', 'max_bytes'):
            for bad in (0, -1, True, 1.5, 'x'):
                with self.assertRaises(ValueError, msg='%s=%r' % (name, bad)):
                    nats_support.jetstream.JetStreamLog(**{name: bad})
        for bad in (0, -1, True, 'x', float('nan'), float('inf')):
            with self.assertRaises(ValueError, msg='max_age=%r' % (bad,)):
                nats_support.jetstream.JetStreamLog(max_age=bad)

        # good ones reach the server and make a stream
        nats_support.jetstream.JetStreamLog(max_messages=10, max_bytes=10_000, max_age=60.5, drop_on_close=True).close()

    def test_an_orphaned_stream_is_deleted_by_name(self):
        jetstream = nats_support.jetstream
        log = jetstream.JetStreamLog(drop_on_close=False)
        namespace = log.namespace
        log.close()  # a run that left its stream behind (what a crash does)

        self.assertTrue(jetstream.delete_stream(namespace))
        self.assertFalse(jetstream.delete_stream(namespace))

    def test_delete_stream_refuses_a_stream_that_is_not_a_runs(self):
        jetstream = nats_support.jetstream
        namespace = 'foreign' + uuid.uuid4().hex[:8]

        import nats
        from nats.js.api import StreamConfig

        async def make():
            nc = await nats.connect(jetstream.DEFAULT_URL)
            await nc.jetstream().add_stream(StreamConfig(name=namespace, subjects=['other.' + namespace]))
            await nc.close()

        async def remove():
            nc = await nats.connect(jetstream.DEFAULT_URL)
            await nc.jetstream().delete_stream(namespace)
            await nc.close()

        asyncio.run(make())
        self.addCleanup(lambda: asyncio.run(remove()))

        with self.assertRaisesRegex(BusError, 'not a run.s stream'):
            jetstream.delete_stream(namespace)

        # still there: a second refusal, not "False"
        with self.assertRaises(BusError):
            jetstream.delete_stream(namespace)

    def test_a_bus_attached_to_an_empty_stream_does_not_read_the_log_at_every_oldest_or_trim(self):
        jetstream = nats_support.jetstream
        log = jetstream.JetStreamLog(drop_on_close=True)
        bus = LogBus(log)
        self.addCleanup(bus.close)
        reads = []
        real = log.read_new
        log.read_new = lambda *args, **kwargs: (reads.append(args), real(*args, **kwargs))[1]

        for _ in range(5):
            bus.oldest('t')
            bus.trim(bus.oldest('t'))

        self.assertEqual(len(reads), 1)  # the attach read, once


if __name__ == '__main__':
    unittest.main()
