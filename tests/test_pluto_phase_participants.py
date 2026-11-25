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
from pluto.control.controller import controller, process_manager

from protos import broker_pb2, clock_pb2

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


def _equity():
    return Equity(
        1,
        ExchangeInfo('XNYS', 'XNYS', 'US'),
        symbol='TEST',
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

    def account_update(self, state):
        if self.fail_account:
            raise ValueError('cannot apply the state')

        self.account_updates.append(state)

    def clock_event(self, event):
        self.clock_events += 1
        self.send(event)

    def send(self, event):
        ts = phases.pandas_timestamp(event.timestamp)

        self.manager.bus.publish(
            phases.ORDERS_TOPIC,
            order_batch(self.process_id, ts, ['order-' + self.process_id]),
            type='OrderBatch',
            logical_ts=ts.value,
            producer_id=self.process_id,
            phase=phases.TRADE.rank,
            key=self.process_id)


class RunTests(unittest.TestCase):
    """A manager with a fake book and a controller driving it through the phases."""

    def setUp(self):
        patch = mock.patch.object(process_manager.service, 'BrokerService', FakeBroker)
        patch.start()
        self.addCleanup(patch.stop)

        setup = types.SimpleNamespace(start=TS, capital=1000.0, data_frequency='minute', leverage=1.0)
        self.manager = process_manager.StrategyManager(None, setup, bus=InMemoryBus())
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


if __name__ == '__main__':
    unittest.main()
