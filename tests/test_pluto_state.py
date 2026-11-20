"""Saved state and monitor packets keep fractional amounts and 64-bit values.

The state is written and read by the real objects: a strategy's
MetricsTracker (pluto.finance.metrics.tracker) over a Ledger and a
PositionTracker, as the controllable builds them; get_state serialises
through tracker_state.proto / ledger_state.proto / protocol.proto, and a
fresh tracker restores it. Every value must come back bit for bit: the
amounts are fractional and the prices, cash and commissions are not
representable in 32 bits, so a 32-bit field or an integer amount would
change them.
"""
import unittest

import numpy as np
import pandas as pd

from trading_calendars import get_calendar

from zipline import protocol
from zipline.assets import Equity, ExchangeInfo
from zipline.data.data_portal import DataPortal
from zipline.finance.order import Order
from zipline.finance.transaction import Transaction

from pluto.coms.utils import conversions
from pluto.finance import ledger, position_tracker
from pluto.finance.metrics import tracker

from protos import controller_pb2
from protos import ledger_state_pb2
from protos import tracker_state_pb2

CAPITAL = 123456.78
START = pd.Timestamp('2014-01-02', tz='UTC')
SESSION = pd.Timestamp('2014-01-03', tz='UTC')
DT = pd.Timestamp('2014-01-03 21:00', tz='UTC')

ACCOUNT_FIELDS = (
    'settled_cash', 'accrued_interest', 'buying_power', 'equity_with_loan',
    'total_positions_value', 'total_positions_exposure', 'regt_equity',
    'regt_margin', 'initial_margin_requirement',
    'maintenance_margin_requirement', 'available_funds', 'excess_liquidity',
    'cushion', 'day_trades_remaining', 'leverage', 'net_leverage',
    'net_liquidation', 'gross_leverage')

PORTFOLIO_FIELDS = (
    'cash_flow', 'starting_cash', 'portfolio_value', 'pnl', 'returns', 'cash',
    'positions_value', 'positions_exposure', 'start_date')

ORDER_FIELDS = (
    'id', 'session_id', 'dt', 'sid', 'amount', 'filled', 'commission',
    'stop', 'limit', 'status')


def _equity(sid, symbol):
    return Equity(
        sid,
        ExchangeInfo('NASDAQ', 'NASDAQ', 'US'),
        symbol=symbol,
        asset_name=symbol,
        start_date=pd.Timestamp('2004-01-02', tz='UTC'),
        end_date=pd.Timestamp('2014-12-31', tz='UTC'),
        first_traded=pd.Timestamp('2004-01-02', tz='UTC'),
        auto_close_date=pd.Timestamp('2015-01-01', tz='UTC'),
        tick_size=0.01,
        multiplier=1.0)


AAPL = _equity(7, 'AAPL')
MSFT = _equity(8, 'MSFT')


def _metrics_tracker():
    # as Controllable.initialize builds it
    pos_tracker = position_tracker.PositionTracker(protocol.Positions())
    return tracker.MetricsTracker(
        ledger.Ledger(
            START,
            protocol.Portfolio(START, CAPITAL),
            protocol.Account(),
            pos_tracker),
        CAPITAL,
        'daily',
        'daily',
        START)


def _not_32_bit(value):
    return float(np.float32(value)) != value


class StateRoundTripTest(unittest.TestCase):
    def setUp(self):
        calendar = get_calendar('XNYS')
        self.data_portal = DataPortal(
            asset_finder=None,
            trading_calendar=calendar,
            first_trading_day=START)

    def _trade(self, metrics_tracker):
        metrics_tracker.handle_market_open(SESSION, self.data_portal)

        fills = [
            Transaction(AAPL, 2.5, DT, 543.2109876, 'o1'),
            Transaction(MSFT, 0.125, DT, 37.123456789, 'o2'),
            Transaction(MSFT, -0.0625, DT, 38.987654321, 'o3'),
        ]
        for txn in fills:
            metrics_tracker.process_transaction(txn)

        # last sale prices of the session
        positions = metrics_tracker.ledger.position_tracker
        positions.update_position(
            AAPL, last_sale_price=545.6789012, last_sale_date=DT)
        positions.update_position(
            MSFT, last_sale_price=38.765432109, last_sale_date=DT)

        metrics_tracker.process_commission(
            {'asset': AAPL, 'order': None, 'cost': 1.0123456789})

        # an open order with a fractional amount and a limit
        order = Order('session', DT, MSFT, 1.75, limit=38.123456789)
        order.filled = 0.3333
        order.commission = 0.0123456789
        metrics_tracker.process_order(order)

    def test_fractional_position_round_trip(self):
        original = _metrics_tracker()
        self._trade(original)

        state = original.get_state(DT)

        restored = _metrics_tracker()
        restored.restore_state(state)

        # positions (position tracker)
        before = original.ledger.position_tracker.positions
        after = restored.ledger.position_tracker.positions
        self.assertEqual(list(before), list(after))
        self.assertEqual(before[AAPL].amount, 2.5)
        self.assertEqual(before[MSFT].amount, 0.0625)
        for asset in before:
            b, a = before[asset], after[asset]
            for field in ('amount', 'cost_basis', 'last_sale_price'):
                self.assertEqual(
                    getattr(b, field).hex(), float(getattr(a, field)).hex(),
                    (asset, field))
            self.assertTrue(_not_32_bit(b.cost_basis), asset)
            self.assertTrue(_not_32_bit(b.last_sale_price), asset)
            self.assertEqual(b.last_sale_date, a.last_sale_date)

        # portfolio (with its positions) and account
        p_before = original.portfolio
        p_after = restored.portfolio
        for field in PORTFOLIO_FIELDS:
            self.assertEqual(
                getattr(p_before, field), getattr(p_after, field), field)
        self.assertTrue(_not_32_bit(p_before.cash))
        self.assertTrue(_not_32_bit(p_before.portfolio_value))
        self.assertEqual(list(p_before.positions), list(p_after.positions))
        for asset, position in p_before.positions.items():
            other = p_after.positions[asset]
            for field in ('amount', 'cost_basis', 'last_sale_price',
                          'last_sale_date'):
                self.assertEqual(
                    getattr(position, field), getattr(other, field),
                    (asset, field))

        a_before = original.account
        a_after = restored.account
        for field in ACCOUNT_FIELDS:
            self.assertEqual(
                getattr(a_before, field), getattr(a_after, field), field)

        # statistics recomputed from the restored positions
        s_before = original.ledger.position_tracker.stats
        s_after = restored.ledger.position_tracker.stats
        for field in ('gross_exposure', 'gross_value', 'long_exposure',
                      'long_value', 'net_exposure', 'net_value',
                      'short_exposure', 'short_value', 'longs_count',
                      'shorts_count'):
            self.assertEqual(
                getattr(s_before, field), getattr(s_after, field), field)

        # orders
        o_before = original.ledger.orders()
        o_after = restored.ledger.orders()
        self.assertEqual(len(o_before), 1)
        self.assertEqual(len(o_after), 1)
        for field in ORDER_FIELDS:
            self.assertEqual(o_before[0][field], o_after[0][field], field)

        # and the restored tracker writes the same state again
        self.assertEqual(restored.get_state(DT), state)


# LedgerState(portfolio=Portfolio(cash=1000.5, portfolio_value=1234.5,
# starting_cash=1000.5), session_count=3) serialised by the modules of master
# dd2e9623 (32-bit float fields, no version)
OLD_LEDGER_STATE = b'\n\x0f\x15\x00 zD\x1d\x00P\x9aD5\x00 zD8\x03'


class OldStateTest(unittest.TestCase):
    """State saved with the 32-bit modules is refused with a clear error
    instead of being read as zeros."""

    def test_old_values_would_be_lost(self):
        # why the version check: the widened fields do not parse
        state = ledger_state_pb2.LedgerState.FromString(OLD_LEDGER_STATE)
        self.assertEqual(state.session_count, 3)
        self.assertEqual(state.portfolio.cash, 0.0)
        self.assertEqual(state.version, 0)

    def test_ledger_refuses_old_state(self):
        with self.assertRaisesRegex(ValueError, 'format version 0'):
            _metrics_tracker().ledger.restore_state(OLD_LEDGER_STATE)

    def test_tracker_refuses_old_state(self):
        state = tracker_state_pb2.TrackerState(
            first_open_session=conversions.to_proto_timestamp(SESSION),
            ledger_state=OLD_LEDGER_STATE,
            last_checkpoint=conversions.to_proto_timestamp(DT))
        with self.assertRaisesRegex(ValueError, 'format version 0'):
            _metrics_tracker().restore_state(state.SerializeToString())



class PerformancePacketTest(unittest.TestCase):
    """The monitor packet (controller.proto PerformancePacket, metrics.proto)
    keeps fractional positions and 64-bit values."""

    def test_round_trip(self):
        values = {
            'amount': 2.5, 'cost_basis': 543.2109876,
            'last_sale_price': 545.6789012}
        txn = Transaction(AAPL, 2.5, DT, 543.2109876, 'o1')
        cumulative = {
            'period_open': START, 'period_close': DT,
            'returns': 0.0123456789, 'pnl': 1524.123456789,
            'capital_used': -1358.0274690, 'starting_exposure': 0.0,
            'ending_exposure': 1364.197253, 'starting_value': 0.0,
            'ending_value': 1364.197253, 'starting_cash': CAPITAL,
            'ending_cash': 122098.7525310, 'portfolio_value': 123462.9497840,
            'longs_count': 1, 'shorts_count': 0, 'long_value': 1364.197253,
            'short_value': 0.0, 'long_exposure': 1364.197253,
            'short_exposure': 0.0, 'gross_leverage': 0.011049292,
            'net_leverage': 0.011049292}
        daily = {
            'orders': [], 'transactions': [txn.to_dict()],
            'positions': [dict(values, sid=AAPL)],
            'period_open': SESSION, 'period_close': DT,
            'capital_used': -1358.0274690, 'starting_exposure': 0.0,
            'ending_exposure': 1364.197253, 'starting_value': 0.0,
            'starting_cash': CAPITAL, 'returns': 0.0123456789,
            'pnl': 1524.123456789}
        risk = {
            'algorithm_period_return': 0.0123456789, 'max_leverage': 0.011049292,
            'trading_days': 2, 'period_label': '2014-01',
            'excess_return': 0.0123456789, 'treasury_period_return': 0.0}
        packet = {
            'cumulative_perf': cumulative, 'daily_perf': daily,
            'cumulative_risk_metrics': risk}

        data = conversions.to_proto_performance_packet(packet).SerializeToString()
        proto = controller_pb2.PerformancePacket()
        proto.ParseFromString(data)
        result = conversions.from_proto_performance_packet(proto)

        for key, value in cumulative.items():
            self.assertEqual(result['cumulative_perf'][key], value, key)
        for key, value in risk.items():
            self.assertEqual(result['cumulative_risk_metrics'][key], value, key)
        for key in ('period_open', 'period_close', 'capital_used',
                    'starting_exposure', 'ending_exposure', 'starting_value',
                    'starting_cash', 'returns', 'pnl'):
            self.assertEqual(result['daily_perf'][key], daily[key], key)

        [result_position] = result['daily_perf']['positions']
        self.assertEqual(result_position['sid'], AAPL)
        for key, value in values.items():
            self.assertEqual(result_position[key], value, key)
        self.assertTrue(_not_32_bit(values['cost_basis']))
        self.assertTrue(_not_32_bit(values['last_sale_price']))

        [result_txn] = result['daily_perf']['transactions']
        self.assertEqual(result_txn.amount, 2.5)
        self.assertEqual(result_txn.price, 543.2109876)
