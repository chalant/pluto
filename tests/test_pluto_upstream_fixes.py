"""Behaviour fixes of zipline-reloaded that Pluto's own copies of the Zipline
modules (pluto/finance/position_tracker.py, ledger.py and
blotter/simulation_blotter.py) mirror. Each test names the upstream commit.
The positions are zipline's own (vendored) ones: Pluto's tracker creates them."""
import collections
import types
import unittest

import pandas as pd
from zipline import protocol
from zipline.assets import Equity, ExchangeInfo
from zipline.finance.cancel_policy import EODCancel, NeverCancel
from zipline.finance.execution import MarketOrder
from zipline.finance import position
from zipline.finance.order import ORDER_STATUS
from zipline.gens.sim_engine import SESSION_END

from pluto.finance import ledger, position_tracker
from pluto.finance.blotter import simulation_blotter

StockDividend = collections.namedtuple('StockDividend', 'asset payment_asset ratio pay_date')

DT = pd.Timestamp('2014-01-02', tz='UTC')


def _equity(sid=1, symbol='TEST'):
    return Equity(sid, ExchangeInfo('TEST', 'TEST', 'US'), symbol=symbol)


def _tracker(*positions):
    tracker = position_tracker.PositionTracker(protocol.Positions())
    for pos in positions:
        tracker.positions[pos.asset] = pos
    return tracker


class SplitTests(unittest.TestCase):
    """zipline-reloaded 62cb4a13 (reverse splits)."""

    def test_long_fraction_is_paid(self):
        asset = _equity()
        pos = position.Position(asset, amount=90.0, cost_basis=1.0)
        cash = pos.handle_split(asset, 100.0)

        self.assertEqual(pos.amount, 0.0)
        self.assertEqual(cash, 90.0)

    def test_short_is_truncated_toward_zero(self):
        # np.floor made short 90 become short 1 with cash credited
        asset = _equity()
        pos = position.Position(asset, amount=-90.0, cost_basis=1.0)
        cash = pos.handle_split(asset, 100.0)

        self.assertEqual(pos.amount, 0.0)
        self.assertEqual(cash, -90.0)

    def test_position_left_without_shares_is_removed(self):
        asset = _equity()
        other = _equity(2, 'OTHER')
        tracker = _tracker(
            position.Position(asset, amount=-90.0, cost_basis=1.0),
            position.Position(other, amount=10.0, cost_basis=1.0))

        cash = tracker.handle_splits([(asset, 100.0)])

        self.assertEqual(cash, -90.0)
        self.assertNotIn(asset, tracker.positions)
        self.assertIn(other, tracker.positions)

    def test_cash_of_either_sign_is_booked(self):
        flows = []
        fake = types.SimpleNamespace(
            _position_tracker=types.SimpleNamespace(handle_splits=lambda splits: -90.0),
            _cash_flow=flows.append)

        ledger.Ledger.process_splits(fake, [])

        self.assertEqual(flows, [-90.0])


class StockDividendTests(unittest.TestCase):
    """zipline-reloaded 604c64f3 and a571cf5a (60dfa7ba and f1649741 are in the
    vendored position)."""

    def setUp(self):
        self.asset = _equity()
        self.pay_date = pd.Timestamp('2014-01-10', tz='UTC')

    def _dividend(self, ratio):
        return StockDividend(self.asset, self.asset, ratio, self.pay_date)

    def test_paid_once_and_stats_refreshed(self):
        tracker = _tracker(position.Position(self.asset, amount=100))
        tracker.earn_dividends([], [self._dividend(0.5)])

        tracker.pay_dividends(self.pay_date)
        self.assertEqual(tracker.positions[self.asset].amount, 150.0)

        tracker._dirty_stats = False
        tracker.pay_dividends(self.pay_date)
        self.assertEqual(tracker.positions[self.asset].amount, 150.0)
        self.assertFalse(tracker._dirty_stats)

        tracker.earn_dividends([], [self._dividend(0.5)])
        tracker.pay_dividends(self.pay_date)
        self.assertTrue(tracker._dirty_stats)
        self.assertEqual(tracker.positions[self.asset].amount, 225.0)

    def test_dividend_of_nothing_is_skipped(self):
        tracker = _tracker(position.Position(self.asset, amount=0.0))
        tracker.earn_dividends([], [self._dividend(0.5)])
        self.assertEqual(tracker._unpaid_stock_dividends, {})


class BlotterTests(unittest.TestCase):
    """zipline-reloaded 62cb4a13 and a85e787b (orders and splits), 698b7dbb
    (daily cancel policy)."""

    def setUp(self):
        self.asset = _equity()

    def _blotter(self, cancel_policy=None):
        blotter = simulation_blotter.SimulationBlotter(None, cancel_policy)
        blotter.set_date(DT)
        return blotter

    def test_split_drops_an_order_left_without_shares(self):
        blotter = self._blotter()
        order_id = blotter.order(self.asset, 100, MarketOrder(), 'session')
        order = blotter.orders[order_id]

        blotter.process_splits([(self.asset, 1000.0)])

        self.assertNotIn(self.asset, blotter.open_orders)
        self.assertNotIn(order, blotter.new_orders)
        # the cleanup of an auto closed asset no longer trips on the order
        blotter.cancel_all_orders_for_asset(self.asset)

    def test_split_keeps_the_outstanding_part_of_a_partly_filled_order(self):
        blotter = self._blotter()
        order_id = blotter.order(self.asset, 100, MarketOrder(), 'session')
        order = blotter.orders[order_id]
        order.filled = 50

        blotter.process_splits([(self.asset, 2.0)])

        self.assertEqual(list(blotter.open_orders[self.asset]), [order])

    def test_daily_cancel_policy_cancels_the_oldest_of_several_orders(self):
        blotter = self._blotter(EODCancel(warn_on_cancel=False))
        first = blotter.order(self.asset, 100, MarketOrder(), 'session')
        blotter.execute_daily_cancel_policy(SESSION_END)
        # a single order stays
        self.assertEqual(blotter.orders[first].status, ORDER_STATUS.OPEN)

        second = blotter.order(self.asset, 50, MarketOrder(), 'session')
        blotter.execute_daily_cancel_policy(SESSION_END)

        self.assertEqual(blotter.orders[first].status, ORDER_STATUS.CANCELLED)
        self.assertEqual(blotter.orders[second].status, ORDER_STATUS.OPEN)
        # relayed with the new orders
        self.assertIn(blotter.orders[first], blotter.new_orders)

    def test_never_cancel_cancels_nothing(self):
        blotter = self._blotter(NeverCancel())
        blotter.order(self.asset, 100, MarketOrder(), 'session')
        second = blotter.order(self.asset, 50, MarketOrder(), 'session')
        blotter.execute_daily_cancel_policy(SESSION_END)
        self.assertEqual(len(blotter.open_orders[self.asset]), 2)
        self.assertEqual(blotter.orders[second].status, ORDER_STATUS.OPEN)
