"""The phases of a run on the bus (pluto/control/phases.py): the lock-step of the controller on a bare bus with fake
participants, and the serialisation of the payloads."""
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
            self.driver.drive(phases.METRICS, clock_event(), TS_NS, ('a',))

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
            phases.BOOK, phases.METRICS, phases.ACCOUNT, phases.TRADE, phases.SUBMIT)]

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
