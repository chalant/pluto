"""Idempotence of the phase protocol: every dedup point of `pluto/control/` and what a retried publish does.

A message is the same message however often it is delivered or published: its id (`phases.phase_msg_id`, `done_msg_id`,
`account_msg_id`, `orders_msg_id`) says what it is, a receiver ignores a copy (`phases.Handled`, the driver's done and batch
ids, `StrategyManager.receive_orders`), and a publisher that fails retries with the same id (`phases.publish_with_retry`). Each
test here puts one message in twice and says what would happen without the dedup of that point: they fail if it is taken out.

What orders the messages is the driver's step (one per drive, carried in the correlation id), never the clock: the clock times of
a run are not ordered (the minute clock goes back inside a session) and several clock events share one.
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
from tests.test_pluto_phases import FakeParticipant, event

LOG = logbook.Logger('test')
TS_NS = TS.value
LATER = TS + pd.Timedelta(minutes=1)
EARLIER = TS - pd.Timedelta(minutes=13)


def clock(ts=TS):
    return clock_pb2.ClockEvent(timestamp=phases.proto_timestamp(ts), event=clock_pb2.BAR)


class HandledTests(unittest.TestCase):
    def test_a_message_is_first_once(self):
        handled = phases.Handled()

        self.assertTrue(handled.first(1, 'a'))
        self.assertFalse(handled.first(1, 'a'))
        self.assertTrue(handled.first(1, 'b'))

    def test_a_message_of_an_earlier_step_is_a_copy(self):
        handled = phases.Handled()

        self.assertTrue(handled.first(5, 'x'))
        self.assertFalse(handled.first(4, 'never seen, but the drive is over'))

    def test_a_later_step_drops_the_ids_of_the_last(self):
        handled = phases.Handled()
        handled.first(1, 'a')

        self.assertTrue(handled.first(2, 'b'))
        self.assertEqual(handled._ids, {'b'})  # bounded by the messages of one drive

    def test_a_message_without_a_step_is_judged_by_its_id_in_the_step_in_progress(self):
        handled = phases.Handled()
        handled.first(3, 'a')

        self.assertTrue(handled.first(None, 'b'))
        self.assertFalse(handled.first(None, 'b'))
        self.assertFalse(handled.first(None, 'a'))

    def test_the_step_of_a_message_is_its_correlation_id(self):
        self.assertEqual(phases.step_of(event('x', step=12)), 12)
        self.assertIsNone(phases.step_of(event('x')))
        self.assertIsNone(phases.step_of(event('x')._replace(correlation_id='a request')))


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

    def test_a_done_must_say_which_event_it_answers(self):
        # without it the id would be shared by every event of the clock time and a second clock event's done would be a copy
        bus = InMemoryBus()

        with self.assertRaises(TypeError):
            phases.publish_done(bus, phases.BOOK, TS_NS, 'a')

        with self.assertRaises(TypeError):
            phases.report(bus, phases.BOOK, TS_NS, 'a', lambda: None, LOG)

    def test_a_done_that_cannot_be_published_is_logged_with_its_cause_not_raised(self):
        bus = FailingBus(phases.BOOK.done_topic, 100)

        with logbook.TestHandler() as handler:
            phases.report(bus, phases.BOOK, TS_NS, 'a', lambda: None, LOG, answers=event('the event'))

        self.assertTrue(any('a could not say done in phase.book' in r.message and 'could not publish a/done/the event' in r.message
                            for r in handler.records), [r.message for r in handler.records])


class DriverTests(unittest.TestCase):
    def setUp(self):
        self.bus = InMemoryBus()
        self.driver = phases.PhaseDriver(self.bus)

    def test_the_phase_event_is_retried_with_its_id_and_a_copy_left_by_an_ambiguous_failure_is_ignored(self):
        bus = FailingBus(phases.BOOK.topic, 2, took=True)  # taken twice, refused twice: three copies in the log
        driver = phases.PhaseDriver(bus)
        participant = FakeParticipant(bus, 'a', phases.BOOK)
        events_seen = []
        bus.subscribe(phases.BOOK.topic, lambda d: events_seen.append((d.envelope.msg_id, d.envelope.correlation_id)))

        driver.drive(phases.BOOK, clock(), TS_NS, ('a',))  # the fake answers every copy: the same done, three times

        self.assertEqual(len(set(bus.attempts)), 1)
        self.assertEqual(set(events_seen), {(phases.phase_msg_id(phases.BOOK, TS_NS, '', 1), '1')})
        self.assertEqual(participant.calls, 3)

    def test_two_drives_of_a_phase_at_one_clock_time_are_two_messages_with_their_steps(self):
        ids = []
        FakeParticipant(self.bus, 'a', phases.BOOK)
        self.bus.subscribe(phases.BOOK.topic, lambda d: ids.append((d.envelope.msg_id, d.envelope.correlation_id)))

        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))
        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))

        self.assertEqual([step for _, step in ids], ['1', '2'])
        self.assertEqual(len({msg_id for msg_id, _ in ids}), 2)

    def test_the_dones_carry_the_step_of_the_event_they_answer(self):
        dones = []
        FakeParticipant(self.bus, 'a', phases.BOOK)
        self.bus.subscribe(phases.DONES, lambda d: dones.append((d.envelope.msg_id, d.envelope.correlation_id)))

        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))

        self.assertEqual(dones, [(phases.done_msg_id('a', phases.phase_msg_id(phases.BOOK, TS_NS, '', 1)), '1')])

    def test_a_late_copy_of_a_done_of_an_earlier_drive_is_ignored(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)
        FakeParticipant(self.bus, 'a', phases.ACCOUNT)
        self.driver.drive(phases.BOOK, clock(), TS_NS, ('a',))
        book_event = event(phases.phase_msg_id(phases.BOOK, TS_NS, '', 1), step=1)
        phases.publish_done(self.bus, phases.BOOK, TS_NS, 'a', answers=book_event)

        self.driver.drive(phases.ACCOUNT, clock(), TS_NS, ('a',))  # the copy of the book done arrives here

    def test_the_clock_going_back_does_not_make_a_done_late(self):
        # the minute clock goes back inside a session: the done of a drive at an earlier clock time is not a copy
        FakeParticipant(self.bus, 'a', phases.BOOK)

        self.driver.drive(phases.BOOK, clock(LATER), LATER.value, ('a',))
        self.driver.drive(phases.BOOK, clock(TS), TS_NS, ('a',))
        self.driver.drive(phases.BOOK, clock(EARLIER), EARLIER.value, ('a',))

    def test_a_done_under_the_id_of_one_accepted_with_other_content_is_reported(self):
        driver = self.driver

        class Doubling(object):
            def __init__(self, bus):
                self.bus = bus
                bus.subscribe(phases.TRADE.topic, self.on_trade)

            def on_trade(self, delivery):
                for orders in (0, 5):
                    phases.publish_done(
                        self.bus, phases.TRADE, TS_NS, 'a', orders=orders, batches=1, answers=delivery.envelope)

        Doubling(self.bus)

        with self.assertRaisesRegex(phases.PhaseError, 'a said done of phase.trade twice with different content'):
            driver.drive(phases.TRADE, clock(), TS_NS, ('a',))

    def publish_batch(self, msg_id, step=None, ts=TS):
        self.bus.publish(
            phases.ORDERS_TOPIC, order_batch('a', ts, ['one']), type='OrderBatch', logical_ts=ts.value, producer_id='a',
            key='a', msg_id=msg_id, correlation_id=None if step is None else str(step))

    def test_a_batch_published_twice_counts_once(self):
        for _ in range(2):
            self.publish_batch(phases.orders_msg_id('a', 'event', 1))

        self.bus.run_until_idle()

        self.assertEqual(self.driver._batches[(TS_NS, 'a')], [1, 1])

    def test_a_second_batch_of_the_same_event_counts(self):
        for n in (1, 2):
            self.publish_batch(phases.orders_msg_id('a', 'event', n))

        self.bus.run_until_idle()

        self.assertEqual(self.driver._batches[(TS_NS, 'a')], [2, 2])

    def test_a_batch_of_another_drive_than_the_trade_phase_in_progress_is_not_counted(self):
        self.driver._step = 5  # the trade phase is the 5th drive

        self.publish_batch(phases.orders_msg_id('a', 'event of drive 4', 1), step=4)
        self.publish_batch(phases.orders_msg_id('a', 'event of drive 5', 1), step=5)
        self.bus.run_until_idle()

        self.assertEqual(self.driver._batches[(TS_NS, 'a')], [1, 1])


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

    def publish_twice(self, topic, payload, msg_id, phase, key='', producer=phases.CONTROLLER_ID, ts=TS_NS, step=1):
        for _ in range(2):
            self.bus.publish(
                topic, payload, type=phases.message_type(payload), logical_ts=ts, producer_id=producer, phase=phase.rank,
                key=key, msg_id=msg_id, correlation_id=str(step))

        self.bus.run_until_idle()

    def calls(self, name):
        return [call for call in self.broker.calls if call[0] == name]

    @staticmethod
    def book_event(ts=TS):
        return phases_pb2.SignalEvent(
            timestamp=phases.proto_timestamp(ts),
            signal=clock_pb2.Signal(timestamp=phases.proto_timestamp(ts), event=clock_pb2.MINUTE_END, calendar_name='XNYS'))

    def test_a_book_event_delivered_twice_writes_the_book_once(self):
        self.publish_twice(phases.BOOK.topic, self.book_event(), 'book event', phases.BOOK, key='XNYS')

        self.assertEqual(len(self.calls('set_time')), 1)
        self.assertEqual(len(self.calls('observe')), 1)  # observe twice would read the ledger twice (revalue compounds)
        self.assertEqual([msg_id for msg_id, _ in self.dones], [phases.done_msg_id(phases.BROKER_ID, 'book event')])

    def test_the_clock_going_back_does_not_make_a_book_event_a_copy(self):
        # the minute clock goes back inside a session (00:00, 20:58, 20:45, ...): the steps order the events, not the clock
        for step, ts in ((1, LATER), (2, TS), (3, EARLIER)):
            self.publish_twice(
                phases.BOOK.topic, self.book_event(ts), 'book event {}'.format(step), phases.BOOK, key='XNYS', ts=ts.value,
                step=step)

        self.assertEqual(len(self.calls('observe')), 3)
        self.assertEqual(len(self.dones), 3)

    def test_an_account_event_delivered_twice_sends_the_states_once(self):
        event_ = phases_pb2.AccountEvent(
            clock=clock(), last_sale_prices=[broker_pb2.AssetPrice(sid=1, price=10.0)])

        self.publish_twice(phases.ACCOUNT.topic, event_, 'account event', phases.ACCOUNT)

        self.assertEqual(len(self.process.account_updates), 1)
        self.assertEqual(len(self.process.account_updates[0].last_sale_prices), 1)  # extended once, not twice
        self.assertEqual(len([m for m, d in self.dones if d.participant == phases.BROKER_ID]), 1)

    def test_a_broker_state_delivered_twice_is_applied_once(self):
        # the runtime's own copy: a state counts its fills, applying it twice would count them twice
        self.publish_twice(
            phases.account_topic('aaa'), broker_pb2.BrokerState(), phases.account_msg_id('aaa', 'account event'),
            phases.ACCOUNT, key='aaa', producer=phases.BROKER_ID)

        self.assertEqual(len(self.process.account_updates), 1)

    def test_a_trade_event_delivered_twice_runs_the_strategy_once(self):
        self.bus.publish(
            phases.account_topic('aaa'), broker_pb2.BrokerState(), type='BrokerState', logical_ts=TS_NS,
            producer_id=phases.BROKER_ID, phase=phases.ACCOUNT.rank, key='aaa', correlation_id='1')
        self.manager.set_global_timestamp(TS)

        self.publish_twice(phases.TRADE.topic, clock(), 'trade event', phases.TRADE, step=2)

        self.assertEqual(self.process.clock_events, 1)
        self.assertEqual(self.manager.received_batches('aaa'), 1)
        self.assertEqual([d.batches for m, d in self.dones if d.phase == phases.TRADE.topic], [1])

    def test_the_batch_of_a_trade_event_has_its_id_and_step(self):
        batches = []
        self.bus.subscribe(phases.ORDERS_TOPIC, lambda d: batches.append((d.envelope.msg_id, d.envelope.correlation_id)))
        self.bus.publish(
            phases.account_topic('aaa'), broker_pb2.BrokerState(), type='BrokerState', logical_ts=TS_NS,
            producer_id=phases.BROKER_ID, phase=phases.ACCOUNT.rank, key='aaa', correlation_id='1')
        self.manager.set_global_timestamp(TS)

        self.bus.publish(
            phases.TRADE.topic, clock(), type='ClockEvent', logical_ts=TS_NS, producer_id=phases.CONTROLLER_ID,
            phase=phases.TRADE.rank, msg_id='trade event', correlation_id='2')
        self.bus.run_until_idle()

        self.assertEqual(batches, [(phases.orders_msg_id('aaa', 'trade event', 1), '2')])

    def test_a_submit_event_delivered_twice_submits_once(self):
        event_ = phases_pb2.SubmitEvent(clock=clock(), runtimes=[])

        self.publish_twice(phases.SUBMIT.topic, event_, 'submit event', phases.SUBMIT)

        self.assertEqual(len(self.calls('submit')), 1)

    def test_a_batch_delivered_twice_reaches_the_book_once(self):
        self.manager.set_global_timestamp(TS)

        self.publish_twice(
            phases.ORDERS_TOPIC, order_batch('aaa', TS, ['order-1']), phases.orders_msg_id('aaa', 'trade event', 1),
            phases.TRADE, key='aaa', producer='aaa', step=2)

        self.assertEqual((self.manager.received_orders('aaa'), self.manager.received_batches('aaa')), (1, 1))

    def test_a_copy_of_a_batch_that_differs_from_it_aborts_the_submit(self):
        self.manager.set_global_timestamp(TS)
        msg_id = phases.orders_msg_id('aaa', 'trade event', 1)

        for ids in (['order-1'], ['order-other']):
            self.bus.publish(
                phases.ORDERS_TOPIC, order_batch('aaa', TS, ids), type='OrderBatch', logical_ts=TS_NS, producer_id='aaa',
                phase=phases.TRADE.rank, key='aaa', msg_id=msg_id, correlation_id='2')

        self.bus.publish(
            phases.SUBMIT.topic, phases_pb2.SubmitEvent(
                clock=clock(), runtimes=[phases_pb2.RuntimeOrders(participant='aaa', orders=1, batches=1)]),
            type='SubmitEvent', logical_ts=TS_NS, producer_id=phases.CONTROLLER_ID, phase=phases.SUBMIT.rank)
        self.bus.run_until_idle()

        errors = [d.error for m, d in self.dones if d.phase == phases.SUBMIT.topic]
        self.assertEqual(len(errors), 1)
        self.assertIn('aaa sent two batches of orders under the id {} that differ'.format(msg_id), errors[0])
        self.assertEqual(self.calls('submit'), [])


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

    def receive(self, ts=TS, event_='trade event', n=1, step=None, orders=('o',)):
        self.manager.receive_orders('aaa', order_batch('aaa', ts, list(orders)), phases.orders_msg_id('aaa', event_, n), step)

    def test_a_copy_in_the_tick_is_ignored(self):
        self.manager.set_global_timestamp(TS)

        self.receive()
        self.receive()

        self.assertEqual((self.manager.received_orders('aaa'), self.manager.received_batches('aaa')), (1, 1))

    def test_without_an_id_every_call_is_a_batch(self):
        self.manager.set_global_timestamp(TS)

        for _ in range(2):
            self.manager.receive_orders('aaa', order_batch('aaa', TS, ['o']))

        self.assertEqual(self.manager.received_batches('aaa'), 2)

    def test_a_copy_after_the_tick_closed_is_not_a_late_batch(self):
        self.manager.set_global_timestamp(TS)
        self.receive()
        self.manager.clear()

        self.receive()

        self.assertEqual(self.manager.take_late_batches(), [])

    def test_another_batch_after_the_tick_closed_is_late(self):
        self.manager.set_global_timestamp(TS)
        self.receive()
        self.manager.clear()

        self.receive(n=2)

        self.assertEqual(self.manager.take_late_batches(), [('aaa', TS.value, 1)])

    def test_a_batch_never_taken_is_late_even_after_a_later_tick_was_taken(self):
        # the silent loss of a real late batch: its id has the form of a runtime's, it was never taken
        self.manager.set_global_timestamp(TS)
        self.receive(step=2)
        self.manager.clear()
        self.manager.set_global_timestamp(LATER)
        self.receive(LATER, 'next event', step=6)
        self.manager.clear()

        self.receive(n=3, step=2)  # a first-time batch of the older tick

        self.assertEqual(self.manager.take_late_batches(), [('aaa', TS.value, 1)])

    def test_a_copy_of_an_earlier_tick_inside_the_window_is_ignored(self):
        self.manager.set_global_timestamp(TS)
        self.receive(step=2)
        self.manager.clear()
        self.manager.set_global_timestamp(LATER)
        self.receive(LATER, 'next event', step=6)
        self.manager.clear()

        self.receive(step=2)  # a copy, two clears later

        self.assertEqual(self.manager.take_late_batches(), [])

    def test_a_copy_older_than_the_window_is_reported_not_dropped(self):
        self.manager.set_global_timestamp(TS)
        self.receive(step=2)
        self.manager.clear()
        self.manager.set_global_timestamp(LATER)
        self.receive(LATER, 'next event', step=2 + phases.COPY_WINDOW + 1)
        self.manager.clear()

        self.receive(step=2)

        self.assertEqual(self.manager.take_late_batches(), [('aaa', TS.value, 1)])

    def test_the_ids_kept_are_bounded_by_the_window(self):
        for step in range(1, 4 * phases.COPY_WINDOW):
            self.manager.set_global_timestamp(TS)
            self.receive(event_='event {}'.format(step), step=step)
            self.manager.clear()

        self.assertLessEqual(len(self.manager._taken), phases.COPY_WINDOW)

    def test_a_copy_of_the_first_of_two_clock_events_at_one_clock_time_is_not_the_second_s_batch(self):
        self.manager.set_global_timestamp(TS)
        self.receive(event_='first event', orders=('first',), step=2)
        self.manager.clear()
        self.manager.set_global_timestamp(TS)  # the next clock event shares the clock time

        self.receive(event_='first event', orders=('first',), step=2)
        self.receive(event_='second event', orders=('second',), step=6)

        self.assertEqual((self.manager.received_orders('aaa'), self.manager.received_batches('aaa')), (1, 1))
        self.assertEqual([order.id for order in self.manager.new_orders], ['second'])

    def test_a_copy_that_differs_is_recorded(self):
        self.manager.set_global_timestamp(TS)
        self.receive(orders=('o',))

        self.receive(orders=('other',))

        self.assertEqual(self.manager.take_conflicting_batches(), [('aaa', phases.orders_msg_id('aaa', 'trade event', 1))])
        self.assertEqual(self.manager.received_batches('aaa'), 1)
        self.assertEqual(self.manager.take_conflicting_batches(), [])


class StreamTests(unittest.TestCase):
    def test_an_event_of_the_stream_delivered_again_is_folded_once(self):
        # the book events do not travel on the bus (the broker service calls its subscribers), so the dedup is the
        # sequence number of the stream: the metrics consumer skips a sequence it folded, observation point included
        tracker = mock.Mock()
        metrics = consumer.StreamMetrics(TS, 1000.0, tracker)
        first = events.StreamEvent('aaa', 1, 'XNYS', events.SessionStarted(TS))
        observed = events.StreamEvent('aaa', 2, 'XNYS', events.Observed(clock_pb2.SESSION_START, TS))

        for event_ in (first, observed, first, observed):
            metrics(event_)

        metrics.raise_if_failed()
        self.assertEqual(tracker.start_of_session.call_count, 1)
        self.assertEqual(metrics.replica.sequence, 2)


if __name__ == '__main__':
    unittest.main()
