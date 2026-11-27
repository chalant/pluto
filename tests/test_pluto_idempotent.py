"""Idempotence of the phase protocol: every dedup point of `pluto/control/` and what a retried publish does.

A message is the same message however often it is delivered or published: its id (`phases.phase_msg_id`, `done_msg_id`,
`account_msg_id`, `orders_msg_id`) says what it is, a receiver ignores a copy (`phases.Handled`, the driver's done and batch ids,
`StrategyManager.receive_orders`), and a publisher that fails retries with the same id (`phases.publish_with_retry`). Each test
here puts one message in twice and says what would happen without the dedup of that point: they fail if it is taken out.
"""
import types
import unittest
from unittest import mock

import logbook
import pandas as pd

from pluto.bus import InMemoryBus, PublishFailed
from pluto.control import phases
from pluto.control.controller import controller, process_manager
from pluto.finance.metrics import consumer
from pluto.broker_service import events

from protos import broker_pb2, clock_pb2, phases_pb2

from tests.test_pluto_phase_participants import (
    TS, FakeBehavior, FakeBroker, FakeProcess, _calendar, order_batch)
from tests.test_pluto_phases import FakeParticipant

LOG = logbook.Logger('test')
TS_NS = TS.value
LATER = TS + pd.Timedelta(minutes=1)


def clock(ts=TS):
    return clock_pb2.ClockEvent(timestamp=phases.proto_timestamp(ts), event=clock_pb2.BAR)


class HandledTests(unittest.TestCase):
    def test_a_message_is_first_once(self):
        handled = phases.Handled()

        self.assertTrue(handled.first(10, 'a'))
        self.assertFalse(handled.first(10, 'a'))
        self.assertTrue(handled.first(10, 'b'))

    def test_a_message_of_an_older_tick_is_a_copy(self):
        handled = phases.Handled()

        self.assertTrue(handled.first(20, 'x'))
        self.assertFalse(handled.first(10, 'never seen, but the tick is closed'))

    def test_a_newer_tick_drops_the_ids_of_the_last(self):
        handled = phases.Handled()
        handled.first(10, 'a')

        self.assertTrue(handled.first(20, 'b'))
        self.assertEqual(handled._ids, {'b'})  # bounded by the messages of one tick

    def test_a_clock_time_shared_by_two_clock_events_keeps_both_sets_of_ids(self):
        handled = phases.Handled()
        handled.first(10, 'first event')
        handled.first(10, 'second event')

        self.assertFalse(handled.first(10, 'first event'))  # a late copy of the first, during the second


class FailingBus(InMemoryBus):
    """Refuses the first `failures` publishes to a topic with PublishFailed (after taking the message when `took`)."""

    def __init__(self, topic, failures, took=False):
        super().__init__()
        self.topic = topic
        self.failures = failures
        self.took = took
        self.attempts = []

    def publish(self, topic, payload, **kwargs):
        if topic != self.topic:
            return super().publish(topic, payload, **kwargs)

        self.attempts.append(kwargs['msg_id'])

        if self.failures > 0:
            self.failures -= 1

            if self.took:
                super().publish(topic, payload, **kwargs)

            raise PublishFailed('injected')

        return super().publish(topic, payload, **kwargs)


class PublishRetryTests(unittest.TestCase):
    def test_a_failed_publish_is_repeated_with_the_same_id(self):
        bus = FailingBus('t', 3)
        seen = []
        bus.subscribe('t', lambda d: seen.append(d.envelope.msg_id))

        phases.publish_with_retry(bus, 't', None, msg_id='the id', type='t', logical_ts=0, producer_id='p')
        bus.run_until_idle()

        self.assertEqual(bus.attempts, ['the id'] * 4)
        self.assertEqual(seen, ['the id'])

    def test_after_the_last_attempt_it_is_a_phase_error_that_names_the_message(self):
        bus = FailingBus('t', phases.PUBLISH_ATTEMPTS + 5)

        with self.assertRaisesRegex(phases.PhaseError, r'could not publish the id on t in 10 attempts: injected'):
            phases.publish_with_retry(bus, 't', None, msg_id='the id', type='t', logical_ts=0, producer_id='p')

        self.assertEqual(len(bus.attempts), phases.PUBLISH_ATTEMPTS)

    def test_another_error_is_not_retried(self):
        class Refusing(InMemoryBus):
            def publish(self, *args, **kwargs):
                raise ValueError('bad argument')

        with self.assertRaises(ValueError):
            phases.publish_with_retry(Refusing(), 't', None, msg_id='x', type='t', logical_ts=0, producer_id='p')


class DriverTests(unittest.TestCase):
    def setUp(self):
        self.bus = InMemoryBus()
        self.driver = phases.PhaseDriver(self.bus)

    def test_the_phase_event_is_retried_with_its_id_and_a_copy_left_by_an_ambiguous_failure_is_ignored(self):
        bus = FailingBus(phases.BOOK.topic, 2, took=True)  # taken twice, refused twice: three copies in the log
        driver = phases.PhaseDriver(bus)
        participant = FakeParticipant(bus, 'a', phases.BOOK)
        events_seen = []
        bus.subscribe(phases.BOOK.topic, lambda d: events_seen.append(d.envelope.msg_id))

        try:
            driver.drive(phases.BOOK, clock(), TS_NS, ('a',))
        except phases.PhaseError:
            pass  # the fake participant answers every copy: its dones are three, the same message

        self.assertEqual(len(set(bus.attempts)), 1)
        self.assertEqual(set(events_seen), {phases.phase_msg_id(phases.BOOK, TS_NS)})
        self.assertEqual(participant.calls, 3)

    def test_the_dones_of_copies_of_an_event_are_one_done(self):
        bus = FailingBus(phases.BOOK.topic, 2, took=True)
        driver = phases.PhaseDriver(bus)
        FakeParticipant(bus, 'a', phases.BOOK)

        driver.drive(phases.BOOK, clock(), TS_NS, ('a',))  # no 'said done twice': three answers, one message

    def test_two_events_of_a_phase_at_one_clock_time_are_two_messages(self):
        ids = []
        FakeParticipant(self.bus, 'a', phases.BOOK)
        self.bus.subscribe(phases.BOOK.topic, lambda d: ids.append(d.envelope.msg_id))

        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))
        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))

        self.assertEqual(len(set(ids)), 2)

    def test_a_late_copy_of_a_done_of_a_closed_tick_is_ignored_for_a_participant_that_answered(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)
        FakeParticipant(self.bus, 'a', phases.ACCOUNT)
        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))
        phases.publish_done(self.bus, phases.BOOK, TS_NS, 'a', answers=phases.phase_msg_id(phases.BOOK, TS_NS))

        self.driver.drive(phases.ACCOUNT, clock(LATER), LATER.value, ('a',))  # the copy of the book done arrives here

    def test_a_done_of_a_closed_tick_from_a_participant_never_heard_of_is_still_an_error(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)
        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))
        phases.publish_done(self.bus, phases.BOOK, TS_NS, 'stranger')

        with self.assertRaisesRegex(phases.PhaseError, 'stranger sent a done of phase.book'):
            self.driver.drive(phases.BOOK, clock(LATER), LATER.value, ('a',))

    def test_a_done_under_the_id_of_one_accepted_with_other_content_is_reported(self):
        FakeParticipant(self.bus, 'a', phases.TRADE)
        self.driver.drive(phases.TRADE, clock(), TS_NS, ('a',))

        # the same id as the accepted done (it answers the first event of the tick), another count
        phases.publish_done(
            self.bus, phases.TRADE, TS_NS, 'a', orders=5, batches=1, answers=phases.phase_msg_id(phases.TRADE, TS_NS))

        with self.assertRaisesRegex(phases.PhaseError, 'a said done of phase.trade twice with different content'):
            self.driver.drive(phases.TRADE, clock(), TS_NS, ('a',))

    def test_a_batch_published_twice_counts_once(self):
        batch = order_batch('a', TS, ['one'])

        for _ in range(2):
            self.bus.publish(
                phases.ORDERS_TOPIC, batch, type='OrderBatch', logical_ts=TS_NS, producer_id='a', key='a',
                msg_id=phases.orders_msg_id('a', TS_NS, 'event', 1))

        self.bus.run_until_idle()

        self.assertEqual(self.driver._batches[(TS_NS, 'a')], [1, 1])

    def test_a_second_batch_of_the_same_event_counts(self):
        batch = order_batch('a', TS, ['one'])

        for n in (1, 2):
            self.bus.publish(
                phases.ORDERS_TOPIC, batch, type='OrderBatch', logical_ts=TS_NS, producer_id='a', key='a',
                msg_id=phases.orders_msg_id('a', TS_NS, 'event', n))

        self.bus.run_until_idle()

        self.assertEqual(self.driver._batches[(TS_NS, 'a')], [2, 2])


class ParticipantTests(unittest.TestCase):
    """The participants and the manager on a bus with a fake book: a message published twice is handled once."""

    def setUp(self):
        patch = mock.patch.object(process_manager.service, 'BrokerService', FakeBroker)
        patch.start()
        self.addCleanup(patch.stop)

        self.bus = InMemoryBus()
        setup = types.SimpleNamespace(start=TS, capital=1000.0, data_frequency='minute', leverage=1.0)
        self.manager = process_manager.StrategyManager(None, setup, bus=self.bus)
        self.broker = self.manager.broker
        self.controller = controller.Controller(self.manager, None, FakeBehavior(), consumer.StreamMetrics(TS, 1000.0, None))
        self.process = FakeProcess('aaa')
        self.process.manager = self.manager
        self.manager.add_strategy(self.process)
        self.dones = []
        self.bus.subscribe(phases.DONES, lambda d: self.dones.append((d.envelope.msg_id, d.envelope.payload)))

    def publish_twice(self, topic, payload, msg_id, phase, key='', producer=phases.CONTROLLER_ID, ts=TS_NS):
        for _ in range(2):
            self.bus.publish(
                topic, payload, type=phases.message_type(payload), logical_ts=ts, producer_id=producer, phase=phase.rank,
                key=key, msg_id=msg_id)

        self.bus.run_until_idle()

    def calls(self, name):
        return [call for call in self.broker.calls if call[0] == name]

    def test_a_book_event_delivered_twice_writes_the_book_once(self):
        event = phases_pb2.SignalEvent(
            timestamp=phases.proto_timestamp(TS),
            signal=clock_pb2.Signal(timestamp=phases.proto_timestamp(TS), event=clock_pb2.MINUTE_END, calendar_name='XNYS'))

        self.publish_twice(phases.BOOK.topic, event, 'book event', phases.BOOK, key='XNYS')

        self.assertEqual(len(self.calls('set_time')), 1)
        self.assertEqual(len(self.calls('observe')), 1)  # observe twice would read the ledger twice (revalue compounds)
        self.assertEqual([msg_id for msg_id, _ in self.dones], [phases.done_msg_id(phases.BROKER_ID, phases.BOOK, TS_NS, 'book event')])

    def test_an_account_event_delivered_twice_sends_the_states_once(self):
        event = phases_pb2.AccountEvent(
            clock=clock(), last_sale_prices=[broker_pb2.AssetPrice(sid=1, price=10.0)])

        self.publish_twice(phases.ACCOUNT.topic, event, 'account event', phases.ACCOUNT)

        self.assertEqual(len(self.process.account_updates), 1)
        self.assertEqual(len(self.process.account_updates[0].last_sale_prices), 1)  # extended once, not twice
        self.assertEqual(len([m for m, d in self.dones if d.participant == phases.BROKER_ID]), 1)

    def test_a_broker_state_delivered_twice_is_applied_once(self):
        # the runtime's own copy: a state counts its fills, applying it twice would count them twice
        state = broker_pb2.BrokerState()

        self.publish_twice(
            phases.account_topic('aaa'), state, phases.account_msg_id('aaa', 'account event'), phases.ACCOUNT, key='aaa',
            producer=phases.BROKER_ID)

        self.assertEqual(len(self.process.account_updates), 1)

    def test_a_trade_event_delivered_twice_runs_the_strategy_once(self):
        self.bus.publish(
            phases.account_topic('aaa'), broker_pb2.BrokerState(), type='BrokerState', logical_ts=TS_NS,
            producer_id=phases.BROKER_ID, phase=phases.ACCOUNT.rank, key='aaa')
        self.manager.set_global_timestamp(TS)

        self.publish_twice(phases.TRADE.topic, clock(), 'trade event', phases.TRADE)

        self.assertEqual(self.process.clock_events, 1)
        self.assertEqual(self.manager.received_batches('aaa'), 1)
        self.assertEqual([d.batches for m, d in self.dones if d.phase == phases.TRADE.topic], [1])

    def test_a_submit_event_delivered_twice_submits_once(self):
        event = phases_pb2.SubmitEvent(clock=clock(), runtimes=[])

        self.publish_twice(phases.SUBMIT.topic, event, 'submit event', phases.SUBMIT)

        self.assertEqual(len(self.calls('submit')), 1)

    def test_a_batch_delivered_twice_reaches_the_book_once(self):
        self.manager.set_global_timestamp(TS)
        batch = order_batch('aaa', TS, ['order-1'])

        self.publish_twice(
            phases.ORDERS_TOPIC, batch, phases.orders_msg_id('aaa', TS_NS, 'trade event', 1), phases.TRADE, key='aaa',
            producer='aaa')

        self.assertEqual((self.manager.received_orders('aaa'), self.manager.received_batches('aaa')), (1, 1))


class ReceiveOrdersTests(unittest.TestCase):
    """`StrategyManager.receive_orders` and the copies of a batch, in and after its tick."""

    def setUp(self):
        patch = mock.patch.object(process_manager.service, 'BrokerService', FakeBroker)
        patch.start()
        self.addCleanup(patch.stop)

        setup = types.SimpleNamespace(start=TS, capital=1000.0, data_frequency='minute', leverage=1.0)
        self.manager = process_manager.StrategyManager(None, setup, bus=InMemoryBus())
        self.process = FakeProcess('aaa')
        self.process.manager = self.manager
        self.manager.add_strategy(self.process)

    def batch_id(self, ts=TS, event='trade event', n=1):
        return phases.orders_msg_id('aaa', ts.value, event, n)

    def test_a_copy_in_the_tick_is_ignored(self):
        self.manager.set_global_timestamp(TS)

        for _ in range(2):
            self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']), self.batch_id())

        self.assertEqual((self.manager.received_orders('aaa'), self.manager.received_batches('aaa')), (1, 1))

    def test_without_an_id_every_call_is_a_batch(self):
        self.manager.set_global_timestamp(TS)

        for _ in range(2):
            self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']))

        self.assertEqual(self.manager.received_batches('aaa'), 2)

    def test_a_copy_after_the_tick_closed_is_not_a_late_batch(self):
        self.manager.set_global_timestamp(TS)
        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']), self.batch_id())
        self.manager.clear()

        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']), self.batch_id())

        self.assertEqual(self.manager.take_late_batches(), [])

    def test_another_batch_after_the_tick_closed_is_late(self):
        self.manager.set_global_timestamp(TS)
        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']), self.batch_id())
        self.manager.clear()

        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']), self.batch_id(n=2))

        self.assertEqual(self.manager.take_late_batches(), [('aaa', TS.value, 1)])

    def test_a_copy_of_an_earlier_clock_time_is_not_late_but_a_batch_under_another_id_is(self):
        self.manager.set_global_timestamp(TS)
        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']), self.batch_id())
        self.manager.clear()
        self.manager.set_global_timestamp(LATER)
        self.manager.receive_orders('aaa', order_batch('aaa', LATER, ['p']), self.batch_id(LATER))
        self.manager.clear()

        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']), self.batch_id())  # a copy, two clears later
        self.assertEqual(self.manager.take_late_batches(), [])

        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['q']), 'aaa:7')
        self.assertEqual(self.manager.take_late_batches(), [('aaa', TS.value, 1)])

    def test_a_copy_of_the_first_of_two_clock_events_at_one_clock_time_is_not_the_second_s_batch(self):
        self.manager.set_global_timestamp(TS)
        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['first']), self.batch_id(event='first event'))
        self.manager.clear()
        self.manager.set_global_timestamp(TS)  # the next clock event shares the clock time

        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['first']), self.batch_id(event='first event'))
        self.manager.receive_orders('aaa', order_batch('aaa', TS, ['second']), self.batch_id(event='second event'))

        self.assertEqual((self.manager.received_orders('aaa'), self.manager.received_batches('aaa')), (1, 1))
        self.assertEqual([order.id for order in self.manager.new_orders], ['second'])


class StreamTests(unittest.TestCase):
    def test_an_event_of_the_stream_delivered_again_is_folded_once(self):
        # the book events do not travel on the bus (the broker service calls its subscribers), so the dedup is the
        # sequence number of the stream: the metrics consumer skips a sequence it folded, observation point included
        tracker = mock.Mock()
        metrics = consumer.StreamMetrics(TS, 1000.0, tracker)
        first = events.StreamEvent('aaa', 1, 'XNYS', events.SessionStarted(TS))
        observed = events.StreamEvent('aaa', 2, 'XNYS', events.Observed(clock_pb2.SESSION_START, TS))

        for event in (first, observed, first, observed):
            metrics(event)

        metrics.raise_if_failed()
        self.assertEqual(tracker.start_of_session.call_count, 1)
        self.assertEqual(metrics.replica.sequence, 2)


if __name__ == '__main__':
    unittest.main()
