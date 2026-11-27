"""The phases of a run on the bus (pluto/control/phases.py): the lock-step of the controller on a bare bus with fake
participants, and the serialisation of the payloads."""
import time
import unittest

import logbook
import pandas as pd

from pluto.bus import InMemoryBus, SerializingBus
from pluto.control import phases

from protos import broker_pb2, clock_pb2, phases_pb2

LOG = logbook.Logger('test')
PhaseError = phases.PhaseError
TS = pd.Timestamp('2020-01-02 14:31', tz='UTC')
TS_NS = TS.value


def clock_event():
    return clock_pb2.ClockEvent(timestamp=phases.proto_timestamp(TS), event=clock_pb2.BAR)


class FakeParticipant(object):
    """Says done in a phase, with the work it is given."""

    def __init__(self, bus, name, phase, work=lambda: None):
        self.name = name
        self.calls = 0
        self._bus = bus
        self._phase = phase
        self._work = work

        bus.subscribe(phase.topic, self._on_phase)

    def _on_phase(self, delivery):
        self.calls += 1
        phases.report(self._bus, self._phase, delivery.envelope.logical_ts, self.name, self._work, LOG)


class PhaseDriverTests(unittest.TestCase):
    def setUp(self):
        self.bus = InMemoryBus()
        self.driver = phases.PhaseDriver(self.bus)

    def test_every_expected_participant_says_done(self):
        a = FakeParticipant(self.bus, 'a', phases.BOOK)
        b = FakeParticipant(self.bus, 'b', phases.BOOK)

        self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a', 'b'))

        self.assertEqual((a.calls, b.calls), (1, 1))

    def test_missing_done_aborts(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)

        with self.assertRaisesRegex(PhaseError, r'phase\.book at 2020-01-02 14:31:00\+00:00: no done from b'):
            self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a', 'b'))

    def test_nobody_listening_aborts(self):
        with self.assertRaisesRegex(PhaseError, 'no done from broker, metrics'):
            self.driver.drive(phases.TRADE, clock_event(), TS_NS, ('metrics', 'broker'))

    def test_failure_is_reported_once_and_not_retried(self):
        def work():
            raise ValueError('the strategy broke')

        failing = FakeParticipant(self.bus, 'a', phases.BOOK, work)

        # the bus retries a handler that raises: a participant reports its failure instead
        with self.assertRaisesRegex(PhaseError, 'a failed: ValueError: the strategy broke'):
            self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

        self.assertEqual(failing.calls, 1)
        self.assertEqual(self.bus.dead_letters(), ())

    def test_a_handler_the_bus_gives_up_on_aborts(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)

        def broken(delivery):
            raise RuntimeError('cannot decode')

        self.bus.subscribe(phases.BOOK.topic, broken)

        with self.assertRaisesRegex(PhaseError, 'gave up on a message of phase.book after 3 attempts: RuntimeError'):
            self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

    def test_done_of_another_phase_or_time_aborts(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)
        phases.publish_done(self.bus, phases.TRADE, TS_NS, 'late')
        phases.publish_done(self.bus, phases.BOOK, TS_NS - 1, 'old')

        with self.assertRaises(PhaseError) as raised:
            self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

        message = str(raised.exception)
        self.assertIn('late sent a done of phase.trade', message)
        self.assertIn('old sent a done of phase.book', message)

    def test_done_twice_aborts(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)
        FakeParticipant(self.bus, 'a', phases.BOOK)

        with self.assertRaisesRegex(PhaseError, 'a said done twice'):
            self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

    def test_done_of_an_unexpected_participant_is_accepted(self):
        # policy: only the expected participants are required; a participant that joined late may answer
        FakeParticipant(self.bus, 'a', phases.BOOK)
        extra = FakeParticipant(self.bus, 'extra', phases.BOOK)

        self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

        self.assertEqual(extra.calls, 1)

    def test_dones_of_a_phase_do_not_leak_into_the_next(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)

        self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

        with self.assertRaisesRegex(PhaseError, 'no done from a'):
            self.driver.drive(phases.ACCOUNT, clock_event(), TS_NS, ('a',))

    def test_a_done_is_published_on_the_done_topic_of_its_phase(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)
        book, trade = [], []
        self.bus.subscribe('phase.done.book', book.append)
        self.bus.subscribe('phase.done.trade', trade.append)

        self.driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

        self.assertEqual([d.envelope.payload.participant for d in book], ['a'])
        self.assertEqual(trade, [])

    def test_phases_are_in_the_order_of_the_calls(self):
        ranks = [phase.rank for phase in (
            phases.BOOK, phases.ACCOUNT, phases.TRADE, phases.SUBMIT)]

        self.assertEqual(ranks, sorted(set(ranks)))
        # a done comes right after the messages of its phase, before the next phase
        self.assertTrue(all(later - earlier > phases.DONE_AFTER for earlier, later in zip(ranks, ranks[1:])))

    def test_messages_of_a_phase_come_in_phase_order_on_the_bus(self):
        order = []

        for phase in (phases.SUBMIT, phases.TRADE, phases.BOOK):
            self.bus.subscribe(phase.topic, lambda delivery: order.append(delivery.envelope.topic))
            self.bus.publish(phase.topic, clock_event(), type='ClockEvent', logical_ts=TS_NS,
                             producer_id='controller', phase=phase.rank)

        self.bus.run_until_idle()

        self.assertEqual(order, ['phase.book', 'phase.trade', 'phase.submit'])


class RemoteBus(InMemoryBus):
    """An in-memory bus that looks networked to the driver: what a remote participant publishes is not in the bus
    until it is polled (``arrive`` queues the publication, one per poll that finds something)."""

    def __init__(self):
        super().__init__()
        self.arrivals = []
        self.polls = 0

    @property
    def networked(self):
        return True

    def arrive(self, publish):
        self.arrivals.append(publish)

    def poll(self, timeout=0.0):
        self.polls += 1

        if not self.arrivals:
            time.sleep(min(timeout, 0.005))
            return False

        self.arrivals.pop(0)()
        return True


class RemoteParticipant(object):
    """Answers a phase through the bus's arrivals: ``late`` polls after the event, or never."""

    def __init__(self, bus, name, phase, work=lambda: None, answers=True):
        self.name = name
        self.calls = 0
        self._bus = bus
        self._phase = phase
        self._work = work
        self._answers = answers

        bus.subscribe(phase.topic, self._on_phase)

    def _on_phase(self, delivery):
        self.calls += 1
        ts = delivery.envelope.logical_ts

        if self._answers:
            self._bus.arrive(lambda: phases.report(self._bus, self._phase, ts, self.name, self._work, LOG))


class NetworkedPhaseDriverTests(unittest.TestCase):
    """On a bus that other processes write to, the phase ends when the expected dones are in, not when it is idle."""

    def setUp(self):
        self.bus = RemoteBus()

    def test_waits_for_dones_that_arrive_after_the_bus_went_idle(self):
        participants = [RemoteParticipant(self.bus, name, phases.BOOK) for name in 'abc']
        driver = phases.PhaseDriver(self.bus, timeout=10)

        driver.drive(phases.BOOK, clock_event(), TS_NS, ('a', 'b', 'c'))

        self.assertEqual([p.calls for p in participants], [1, 1, 1])
        self.assertEqual(self.bus.arrivals, [])
        self.assertGreaterEqual(self.bus.polls, 3)

    def test_the_next_phase_does_not_see_the_dones_of_the_last(self):
        RemoteParticipant(self.bus, 'a', phases.BOOK)
        RemoteParticipant(self.bus, 'a', phases.ACCOUNT)
        driver = phases.PhaseDriver(self.bus, timeout=10)

        driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))
        driver.drive(phases.ACCOUNT, clock_event(), TS_NS, ('a',))

    def test_a_participant_that_never_answers_fails_the_phase_after_the_deadline_naming_it(self):
        RemoteParticipant(self.bus, 'a', phases.BOOK)
        RemoteParticipant(self.bus, 'b', phases.BOOK, answers=False)
        driver = phases.PhaseDriver(self.bus, timeout=0.2)
        started = time.monotonic()

        with self.assertRaisesRegex(PhaseError, r'phase\.book at .*: no done from b within 0\.2 s') as raised:
            driver.drive(phases.BOOK, clock_event(), TS_NS, ('a', 'b'))

        self.assertNotIn('no done from a', str(raised.exception))
        self.assertGreaterEqual(time.monotonic() - started, 0.2)

    def test_a_failure_ends_the_wait_at_once(self):
        def work():
            raise ValueError('the strategy broke')

        RemoteParticipant(self.bus, 'a', phases.BOOK, work)
        RemoteParticipant(self.bus, 'b', phases.BOOK, answers=False)
        driver = phases.PhaseDriver(self.bus, timeout=60)
        started = time.monotonic()

        with self.assertRaisesRegex(PhaseError, 'a failed: ValueError: the strategy broke'):
            driver.drive(phases.BOOK, clock_event(), TS_NS, ('a', 'b'))

        self.assertLess(time.monotonic() - started, 5)

    def test_a_done_of_another_phase_ends_the_wait_at_once(self):
        RemoteParticipant(self.bus, 'a', phases.BOOK, answers=False)
        self.bus.arrive(lambda: phases.publish_done(self.bus, phases.TRADE, TS_NS, 'late'))
        driver = phases.PhaseDriver(self.bus, timeout=60)
        started = time.monotonic()

        with self.assertRaisesRegex(PhaseError, 'late sent a done of phase.trade'):
            driver.drive(phases.BOOK, clock_event(), TS_NS, ('a',))

        self.assertLess(time.monotonic() - started, 5)

    def test_no_deadline_waits_for_a_slow_participant(self):
        RemoteParticipant(self.bus, 'a', phases.BOOK)

        def slow():
            time.sleep(0.05)

        RemoteParticipant(self.bus, 'b', phases.BOOK, slow)

        phases.PhaseDriver(self.bus, timeout=None).drive(phases.BOOK, clock_event(), TS_NS, ('a', 'b'))

    def test_a_message_that_is_already_in_the_bus_needs_no_poll(self):
        FakeParticipant(self.bus, 'a', phases.BOOK)

        phases.PhaseDriver(self.bus, timeout=10).drive(phases.BOOK, clock_event(), TS_NS, ('a',))

        self.assertEqual(self.bus.polls, 0)

    def trade_done_then_batch(self, orders=1, batches=1, batch_arrives=True):
        """Runtime 'a' says done in the trade phase (announcing a batch); the batch itself arrives after the done."""
        batch = broker_pb2.OrderBatch(ts=phases.proto_timestamp(TS))

        for _ in range(orders):
            batch.orders.add()

        RemoteParticipant(self.bus, 'a', phases.TRADE, answers=False)
        self.bus.arrive(lambda: phases.publish_done(self.bus, phases.TRADE, TS_NS, 'a', orders, batches))

        if batch_arrives:
            for _ in range(batches):
                self.bus.arrive(lambda: self.bus.publish(
                    phases.ORDERS_TOPIC, batch, type='OrderBatch', logical_ts=TS_NS, producer_id='a',
                    phase=phases.TRADE.rank, key='a'))

    def test_the_submit_waits_for_the_batch_a_done_announced(self):
        self.trade_done_then_batch(orders=2, batches=1)
        driver = phases.PhaseDriver(self.bus, timeout=10)

        driver.drive(phases.TRADE, clock_event(), TS_NS, ('a',))

        # the done is in, the batch is not: the driver goes on reading the log until it is
        announced = driver.announced_orders(TS_NS, ('a',))

        self.assertEqual([(r.participant, r.orders, r.batches) for r in announced], [('a', 2, 1)])
        self.assertEqual(self.bus.arrivals, [])

    def test_a_batch_that_never_comes_fails_naming_the_runtime(self):
        self.trade_done_then_batch(batch_arrives=False)
        driver = phases.PhaseDriver(self.bus, timeout=0.2)

        driver.drive(phases.TRADE, clock_event(), TS_NS, ('a',))

        with self.assertRaisesRegex(
                PhaseError, r'phase\.trade at .*: no batch of orders from a within 0\.2 s \(a announced 1 orders in 1 '
                            r'batches, the log has 0 in 0\)'):
            driver.announced_orders(TS_NS, ('a',))

    def test_a_runtime_that_announced_nothing_needs_no_batch(self):
        RemoteParticipant(self.bus, 'a', phases.TRADE)
        driver = phases.PhaseDriver(self.bus, timeout=0.2)

        driver.drive(phases.TRADE, clock_event(), TS_NS, ('a',))

        self.assertEqual([(r.orders, r.batches) for r in driver.announced_orders(TS_NS, ('a',))], [(0, 0)])


class InMemoryPhaseDriverTests(unittest.TestCase):
    def test_a_missing_done_fails_at_once_whatever_the_deadline(self):
        bus = InMemoryBus()
        FakeParticipant(bus, 'a', phases.BOOK)
        driver = phases.PhaseDriver(bus, timeout=60)
        started = time.monotonic()

        with self.assertRaisesRegex(PhaseError, r'no done from b$'):
            driver.drive(phases.BOOK, clock_event(), TS_NS, ('a', 'b'))

        self.assertLess(time.monotonic() - started, 5)

    def test_the_bus_says_nothing_can_arrive_from_outside(self):
        bus = SerializingBus(InMemoryBus(), phases.codec())

        self.assertFalse(bus.networked)
        self.assertFalse(bus.poll(10))

    def test_what_is_missing_is_not_waited_for_in_memory(self):
        bus = InMemoryBus()
        FakeParticipant(bus, 'a', phases.TRADE)
        driver = phases.PhaseDriver(bus, timeout=60)
        driver.drive(phases.TRADE, clock_event(), TS_NS, ('a',))
        started = time.monotonic()

        # a done that announced orders no batch of which is on the bus: the broker side reports it when it checks
        driver._accepted[phases.TRADE.topic]['a'] = phases_pb2.PhaseDone(
            phase=phases.TRADE.topic, participant='a', orders=1, batches=1)

        announced = driver.announced_orders(TS_NS, ('a',))

        self.assertEqual([(r.orders, r.batches) for r in announced], [(1, 1)])
        self.assertLess(time.monotonic() - started, 5)


class CodecTests(unittest.TestCase):
    def test_every_message_survives_a_serialising_bus(self):
        messages = {
            'ClockEvent': clock_event(),
            'BrokerState': broker_pb2.BrokerState(
                last_sale_prices=[broker_pb2.AssetPrice(sid=1, price=10.1)], not_running=1),
            'OrderBatch': broker_pb2.OrderBatch(ts=phases.proto_timestamp(TS), ended=True),
            'SignalEvent': phases_pb2.SignalEvent(
                timestamp=phases.proto_timestamp(TS),
                signal=clock_pb2.Signal(timestamp=phases.proto_timestamp(TS), event=clock_pb2.SESSION_END,
                                        calendar_name='XNYS'),
                last_sale_prices=[broker_pb2.AssetPrice(sid=2, price=0.1)]),
            'AccountEvent': phases_pb2.AccountEvent(clock=clock_event()),
            'PhaseDone': phases_pb2.PhaseDone(phase='phase.trade', participant='aaa', orders=3, error='e'),
            'SubmitEvent': phases_pb2.SubmitEvent(
                clock=clock_event(), runtimes=[phases_pb2.RuntimeOrders(participant='aaa', orders=2, batches=1)]),
        }
        self.assertEqual(set(messages), set(phases.message_types()))

        bus = SerializingBus(InMemoryBus(), phases.codec())
        received = []
        bus.subscribe('t', lambda delivery: received.append(delivery.envelope.payload))

        for name, message in messages.items():
            bus.publish('t', message, type=name, logical_ts=TS_NS, producer_id='p')

        bus.run_until_idle()

        self.assertEqual(received, list(messages.values()))
        self.assertTrue(all(a is not b for a, b in zip(received, messages.values())))

    def test_timestamps_to_the_nanosecond(self):
        ts = pd.Timestamp('2020-01-02 14:31:00.123456789', tz='UTC')

        self.assertEqual(phases.pandas_timestamp(phases.proto_timestamp(ts)), ts)
        self.assertEqual(phases.pandas_timestamp(phases.proto_timestamp(ts)).value, ts.value)


if __name__ == '__main__':
    unittest.main()
