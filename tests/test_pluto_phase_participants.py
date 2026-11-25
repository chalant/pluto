"""The participants of the phases (pluto/control/controller/participants.py) and the strategy manager on the bus, with a
fake book, a fake metrics tracker and fake runtimes: what each clock event does, in which order, and what a tick does
with the orders of several strategies."""
import types
import unittest
from unittest import mock

import pandas as pd
from zipline.assets import Equity, ExchangeInfo
from zipline.finance.order import Order

from pluto.bus import InMemoryBus
from pluto.coms.utils import conversions
from pluto.control import phases
from pluto.control.controller import controller, participants, process_manager
from pluto.control.processes import in_memory
from pluto.interface.utils import service_access

from protos import broker_pb2, clock_pb2, phases_pb2

TS = pd.Timestamp('2020-01-02 14:31', tz='UTC')
PRICES = {'XNYS': [(1, 10.0)], 'XLON': [(2, 20.0)]}


def _calendar(name):
    return types.SimpleNamespace(name=name)


class FakeBroker(object):
    """Takes the place of the ``BrokerService`` of the manager and records every write, in order, in ``calls``."""

    def __init__(self, *args):
        self.calls = []
        self.ledger = object()
        self.fail_submit = False

    def subscribe(self, callback):
        pass

    def set_strategy(self, strategy_id):
        pass

    def attach_market(self, market):
        pass

    def get_ledger(self, calendar_name):
        return self.ledger

    def set_time(self, calendar_name, dt):
        self.calls.append(('set_time', calendar_name, dt))

    def sync_last_sale_prices(self, calendar_name, prices, dt):
        self.calls.append(('sync_last_sale_prices', calendar_name, prices, dt))

    def match(self, calendar_name):
        self.calls.append(('match', calendar_name))

    def revalue(self, calendar_name):
        self.calls.append(('revalue', calendar_name))

    def start_session(self, calendar_name, dt):
        self.calls.append(('start_session', calendar_name, dt))

    def end_session(self, calendar_name, dt):
        self.calls.append(('end_session', calendar_name, dt))

    def submit(self, orders):
        if self.fail_submit:
            raise ValueError('the book refused the orders')

        self.calls.append(('submit', [order.id for order in orders]))


class FakeTracker(object):
    """Records the reads of the ledger in the calls of the broker, so that the order between the two is visible."""

    def __init__(self, broker):
        self._broker = broker

    def _read(self, name, dt, calendar_name, ledger):
        assert ledger is self._broker.ledger
        self._broker.calls.append((name, calendar_name, dt))

    def end_of_bar(self, dt, calendar_name, ledger):
        self._read('end_of_bar', dt, calendar_name, ledger)

    def start_of_session(self, dt, calendar_name, ledger):
        self._read('start_of_session', dt, calendar_name, ledger)

    def end_of_session(self, dt, calendar_name, ledger):
        self._read('end_of_session', dt, calendar_name, ledger)


class FakeBehavior(object):
    def get_last_sale_prices(self, ts, manager, signals):
        return {cal.name: PRICES[cal.name] for _, _, cal in signals}

    def ingest_watchlist_data(self, ts, manager, signals):
        pass

    def ingest_all_data(self, ts, manager, signals):
        pass

    def update_watchlist(self, ts, manager):
        pass


def _equity(sid=1):
    return Equity(
        sid,
        ExchangeInfo('XNYS', 'XNYS', 'US'),
        symbol='TEST{}'.format(sid),
        asset_name='TEST',
        start_date=pd.Timestamp('2004-01-02', tz='UTC'),
        end_date=pd.Timestamp('2014-12-31', tz='UTC'),
        first_traded=pd.Timestamp('2004-01-02', tz='UTC'),
        auto_close_date=pd.Timestamp('2015-01-01', tz='UTC'),
        tick_size=0.01,
        multiplier=1.0)


def order_batch(process_id, ts, ids):
    """The batch of a runtime: one order of the same asset for each id."""
    asset = _equity()

    return broker_pb2.OrderBatch(
        ts=phases.proto_timestamp(ts),
        orders=[conversions.to_proto_order(Order(process_id, ts, asset, 10, id_=i).to_dict()) for i in ids])


class FakeProcess(object):
    """A runtime: says what it does with the broker state and the clock event; by default it sends one order."""

    def __init__(self, process_id):
        self.process_id = process_id
        self.account_updates = []
        self.clock_events = 0
        self.fail_account = False
        self.sends = True
        # the clock time of the batch it sends: None for the time of the clock event
        self.batch_ts = None
        # the ids of the orders in its batch: None for one order
        self.order_ids = None

    def account_update(self, state):
        if self.fail_account:
            raise ValueError('cannot apply the state')

        self.account_updates.append(state)

    def clock_event(self, event):
        self.clock_events += 1

        if self.sends:
            ts = phases.pandas_timestamp(event.timestamp) if self.batch_ts is None else self.batch_ts
            ids = ['order-' + self.process_id] if self.order_ids is None else self.order_ids

            self.manager.runtime(self.process_id).send(phases.ORDERS_TOPIC, order_batch(self.process_id, ts, ids))


class FaultyBus(InMemoryBus):
    """An in-memory bus that loses the batches of orders of some producers, and delivers those of others twice."""

    def __init__(self):
        super().__init__()
        self.lost = set()
        self.doubled = set()

    def publish(self, topic, payload, **kwargs):
        if topic == phases.ORDERS_TOPIC and kwargs['key'] in self.lost:
            return None

        receipt = super().publish(topic, payload, **kwargs)

        if topic == phases.ORDERS_TOPIC and kwargs['key'] in self.doubled:
            super().publish(topic, payload, **kwargs)

        return receipt


class RunTests(unittest.TestCase):
    """A manager with a fake book and a controller driving it through the phases."""

    def setUp(self):
        patch = mock.patch.object(process_manager.service, 'BrokerService', FakeBroker)
        patch.start()
        self.addCleanup(patch.stop)

        self.bus = FaultyBus()
        setup = types.SimpleNamespace(start=TS, capital=1000.0, data_frequency='minute', leverage=1.0)
        self.manager = process_manager.StrategyManager(None, setup, bus=self.bus)
        self.broker = self.manager.broker
        self.controller = controller.Controller(
            self.manager, None, FakeTracker(self.broker), FakeBehavior())

    def add_process(self, process_id):
        process = FakeProcess(process_id)
        process.manager = self.manager
        self.manager.add_strategy(process)
        return process

    def tick(self, evt=clock_pb2.BAR, calendars=('XNYS',)):
        signals = [(TS, evt, _calendar(name)) for name in calendars]
        self.controller.clock_update(TS, evt, signals)

    def test_orders_are_submitted_in_the_order_the_strategies_were_added(self):
        # the bus delivers the batches by producer id; 'controller', the producer of the clock event, is in between
        for process_id in ('zzz', 'aaa', 'mmm'):
            self.add_process(process_id)

        self.tick()

        self.assertEqual(self.broker.calls[-1], ('submit', ['order-zzz', 'order-aaa', 'order-mmm']))

    def test_a_runtime_that_sends_no_batch_aborts_the_run(self):
        self.add_process('aaa').sends = False

        with self.assertRaisesRegex(phases.PhaseError, r'phase\.trade .*aaa failed: sent no batch of orders'):
            self.tick()

    def test_a_lost_batch_aborts_the_run_in_the_submit_phase(self):
        self.add_process('aaa')
        self.add_process('bbb')
        self.bus.lost.add('bbb')

        with self.assertRaisesRegex(
                phases.PhaseError, r'phase\.submit .*bbb sent 1 orders in 1 batches, the broker received 0 in 0'
        ) as raised:
            self.tick()

        self.assertNotIn('aaa sent', str(raised.exception))
        self.assertNotIn(('submit', ['order-aaa']), self.broker.calls)

    def test_a_batch_of_another_clock_event_aborts_the_run_in_the_submit_phase(self):
        process = self.add_process('aaa')
        process.batch_ts = TS - pd.Timedelta(minutes=1)

        with self.assertRaisesRegex(
                phases.PhaseError, r'phase\.submit .*aaa sent 1 orders in 1 batches, the broker received 0 in 0'):
            self.tick()

        self.assertEqual([call for call in self.broker.calls if call[0] == 'submit'], [])

    def test_an_empty_batch_of_another_clock_event_aborts_the_run(self):
        # nothing in it to count: the batch counts are what tell
        process = self.add_process('aaa')
        process.batch_ts = TS - pd.Timedelta(minutes=1)
        process.order_ids = []

        with self.assertRaisesRegex(
                phases.PhaseError, r'phase\.submit .*aaa sent 0 orders in 1 batches, the broker received 0 in 0'):
            self.tick()

    def test_a_batch_delivered_twice_aborts_the_run(self):
        process = self.add_process('aaa')
        process.order_ids = []
        self.bus.doubled.add('aaa')

        with self.assertRaisesRegex(
                phases.PhaseError, r'phase\.submit .*aaa sent 0 orders in 1 batches, the broker received 0 in 2'):
            self.tick()

    def test_a_batch_of_orders_delivered_twice_aborts_the_run(self):
        self.add_process('aaa')
        self.bus.doubled.add('aaa')

        with self.assertRaisesRegex(
                phases.PhaseError, r'phase\.submit .*aaa sent 1 orders in 1 batches, the broker received 2 in 2'):
            self.tick()

    def test_a_runtime_in_another_process_is_not_supported_yet(self):
        # PlaceOrders, SetWatchList and Stopped over gRPC are not on the bus: they say so instead of leaving the run
        # to abort on a missing done
        self.add_process('aaa')
        context = in_memory.FakeContext(service_access._metadata + (('session_id', 'aaa'),))

        for name in ('PlaceOrders', 'SetWatchList', 'Stopped'):
            with self.assertRaisesRegex(NotImplementedError, r'{}: .*networked bus'.format(name)):
                getattr(self.manager, name)(iter(()), context)

    def test_a_tick_with_two_signals_interleaves_the_book_and_the_metrics_per_signal(self):
        signals = [
            (TS, clock_pb2.MINUTE_END, _calendar('XNYS')),
            (TS, clock_pb2.SESSION_END, _calendar('XLON'))]

        self.controller.clock_update(TS, clock_pb2.MINUTE_END, signals)

        self.assertEqual(self.broker.calls, [
            ('set_time', 'XNYS', TS),
            ('sync_last_sale_prices', 'XNYS', [(1, 10.0)], TS),
            ('revalue', 'XNYS'),
            ('end_of_bar', 'XNYS', TS),
            ('set_time', 'XLON', TS),
            ('sync_last_sale_prices', 'XLON', [(2, 20.0)], TS),
            ('end_session', 'XLON', TS),
            ('revalue', 'XLON'),
            ('end_of_session', 'XLON', TS),
            ('submit', [])])

    def test_the_stale_batch_filter_on_the_bus_path(self):
        self.add_process('aaa')
        self.manager.set_global_timestamp(TS)
        stale = TS - pd.Timedelta(minutes=1)

        def send(ts, order_id, sid):
            """A batch of one order and a watchlist of one asset, for the clock event ``ts``."""
            self.bus.publish(
                phases.ORDERS_TOPIC,
                order_batch('aaa', ts, [order_id]),
                type='OrderBatch', logical_ts=ts.value, producer_id='aaa', phase=phases.TRADE.rank, key='aaa')
            self.bus.publish(
                phases.WATCHLIST_TOPIC,
                broker_pb2.WatchList(ts=phases.proto_timestamp(ts), assets=[conversions.to_proto_asset(_equity(sid))]),
                type='WatchList', logical_ts=ts.value, producer_id='aaa', phase=phases.TRADE.rank, key='aaa')
            self.bus.run_until_idle()

        # what a runtime sent for another clock event is ignored: its orders and its assets
        send(stale, 'old', 2)

        self.assertEqual(self.manager.new_orders, [])
        self.assertEqual(self.manager.watchlist, frozenset())

        send(TS, 'new', 1)

        self.assertEqual([order.id for order in self.manager.new_orders], ['new'])
        self.assertEqual([asset.sid for asset in self.manager.watchlist], [1])

    def test_a_failure_in_the_account_phase_aborts_the_run(self):
        self.add_process('aaa').fail_account = True

        with self.assertRaisesRegex(phases.PhaseError, r'phase\.account .*aaa failed: ValueError: cannot apply the state'):
            self.tick()

        self.assertEqual(self.bus.dead_letters(), ())

    def test_a_failure_in_the_submit_phase_aborts_the_run(self):
        self.add_process('aaa')
        self.broker.fail_submit = True

        with self.assertRaisesRegex(
                phases.PhaseError, r'phase\.submit .*broker failed: ValueError: the book refused the orders'):
            self.tick()

        self.assertEqual(self.bus.dead_letters(), ())


class ParticipantTests(unittest.TestCase):
    """The book and the metrics participants alone, on a bus, with a fake book and a fake tracker."""

    def setUp(self):
        patch = mock.patch.object(process_manager.service, 'BrokerService', FakeBroker)
        patch.start()
        self.addCleanup(patch.stop)

        self.bus = InMemoryBus()
        setup = types.SimpleNamespace(start=TS, capital=1000.0, data_frequency='minute', leverage=1.0)
        manager = process_manager.StrategyManager(None, setup, bus=self.bus)
        self.broker = manager.broker

        participants.BrokerParticipant(self.bus, manager)
        participants.MetricsParticipant(self.bus, manager, FakeTracker(self.broker))
        self.driver = phases.PhaseDriver(self.bus)

    def signal(self, evt, calendar_name='XNYS'):
        return phases_pb2.SignalEvent(
            timestamp=phases.proto_timestamp(TS),
            signal=clock_pb2.Signal(timestamp=phases.proto_timestamp(TS), event=evt, calendar_name=calendar_name),
            last_sale_prices=[broker_pb2.AssetPrice(sid=sid, price=price) for sid, price in PRICES[calendar_name]])

    def run_signal(self, evt):
        """The book phase and the metrics phase of a signal, as the controller drives them."""
        for phase, name in ((phases.BOOK, phases.BROKER_ID), (phases.METRICS, phases.METRICS_ID)):
            self.driver.drive(phase, self.signal(evt), TS.value, (name,), key='XNYS')

    def test_the_calls_of_each_clock_event(self):
        prices = [(1, 10.0)]
        expected = {
            clock_pb2.BAR: [
                ('set_time', 'XNYS', TS), ('sync_last_sale_prices', 'XNYS', prices, TS), ('match', 'XNYS')],
            clock_pb2.TRADE_END: [
                ('set_time', 'XNYS', TS), ('sync_last_sale_prices', 'XNYS', prices, TS), ('match', 'XNYS')],
            # the book is revalued right before the tracker reads it
            clock_pb2.MINUTE_END: [
                ('set_time', 'XNYS', TS), ('sync_last_sale_prices', 'XNYS', prices, TS), ('revalue', 'XNYS'),
                ('end_of_bar', 'XNYS', TS)],
            clock_pb2.SESSION_START: [
                ('set_time', 'XNYS', TS), ('start_session', 'XNYS', TS), ('revalue', 'XNYS'),
                ('start_of_session', 'XNYS', TS)],
            clock_pb2.BEFORE_TRADING_START: [
                ('set_time', 'XNYS', TS), ('sync_last_sale_prices', 'XNYS', prices, TS)],
            clock_pb2.SESSION_END: [
                ('set_time', 'XNYS', TS), ('sync_last_sale_prices', 'XNYS', prices, TS), ('end_session', 'XNYS', TS),
                ('revalue', 'XNYS'), ('end_of_session', 'XNYS', TS)],
        }
        self.assertEqual(set(expected), {
            clock_pb2.BAR, clock_pb2.TRADE_END, clock_pb2.MINUTE_END, clock_pb2.SESSION_START,
            clock_pb2.BEFORE_TRADING_START, clock_pb2.SESSION_END})

        for evt, calls in expected.items():
            with self.subTest(event=clock_pb2.EventType.Name(evt)):
                self.broker.calls.clear()

                self.run_signal(evt)

                self.assertEqual(self.broker.calls, calls)


if __name__ == '__main__':
    unittest.main()
