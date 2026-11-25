"""Pluto's order ids are deterministic (CR7): the id of the strategy session
(its process id) followed by the sequence of the orders placed by the blotter.
Zipline's own blotter keeps its random ids (the oracle leaves ids out)."""
import unittest

import pandas as pd
from zipline.assets import Equity, ExchangeInfo
from zipline.finance.execution import MarketOrder

from pluto.finance.blotter import simulation_blotter

DT = pd.Timestamp('2014-01-02', tz='UTC')
SESSION = 'ab12' * 8  # a process id: md5 hexdigest


def _equity(sid=1, symbol='TEST'):
    return Equity(sid, ExchangeInfo('TEST', 'TEST', 'US'), symbol=symbol)


def _blotter():
    blotter = simulation_blotter.SimulationBlotter(None)
    blotter.set_date(DT)
    return blotter


class OrderIdTests(unittest.TestCase):
    def setUp(self):
        self.asset = _equity()

    def _place(self, blotter, amount=100, session_id=SESSION, **kwargs):
        return blotter.order(self.asset, amount, MarketOrder(), session_id, **kwargs)

    def test_session_id_and_sequence(self):
        blotter = _blotter()

        self.assertEqual(self._place(blotter), SESSION + '00000001')
        self.assertEqual(self._place(blotter), SESSION + '00000002')
        self.assertEqual(self._place(blotter, -5), SESSION + '00000003')

    def test_same_orders_same_ids(self):
        first, second = _blotter(), _blotter()

        self.assertEqual(
            [self._place(first, amount) for amount in (10, -20, 30)],
            [self._place(second, amount) for amount in (10, -20, 30)])

    def test_sessions_do_not_share_ids(self):
        other = 'cd34' * 8

        self.assertNotEqual(
            self._place(_blotter()),
            self._place(_blotter(), session_id=other))

    def test_an_order_of_no_shares_takes_no_number(self):
        blotter = _blotter()

        self.assertIsNone(self._place(blotter, 0))
        self.assertEqual(self._place(blotter), SESSION + '00000001')

    def test_a_given_id_is_kept(self):
        blotter = _blotter()

        self.assertEqual(self._place(blotter, order_id='given'), 'given')
        self.assertEqual(self._place(blotter), SESSION + '00000001')

    def test_the_id_is_hexadecimal(self):
        # zipline's Order.__hash__ reads the id as a hexadecimal number
        blotter = _blotter()
        order = blotter.orders[self._place(blotter)]

        self.assertIsInstance(hash(order), int)
        self.assertEqual(len(order.id), 40)
