"""Pluto's order ids are deterministic (CR7): a namespace made from the strategy
session (its process id) followed by the sequence of the orders placed by the
blotter. Zipline's own blotter keeps its random ids (the oracle leaves ids out)."""
import hashlib
import unittest

import pandas as pd
from zipline.assets import Equity, ExchangeInfo
from zipline.finance.execution import MarketOrder

from pluto.control.processes import processes
from pluto.finance.blotter import simulation_blotter

DT = pd.Timestamp('2014-01-02', tz='UTC')
SESSION = processes.make_process_id('strategy', 'run')
NAMESPACE = hashlib.md5(SESSION.encode()).hexdigest()[:16]


def _equity(sid=1, symbol='TEST'):
    return Equity(sid, ExchangeInfo('TEST', 'TEST', 'US'), symbol=symbol)


def _blotter(**kwargs):
    blotter = simulation_blotter.SimulationBlotter(None, **kwargs)
    blotter.set_date(DT)
    return blotter


class OrderIdTests(unittest.TestCase):
    def setUp(self):
        self.asset = _equity()

    def _place(self, blotter, amount=100, session_id=SESSION, **kwargs):
        return blotter.order(self.asset, amount, MarketOrder(), session_id, **kwargs)

    def test_namespace_and_sequence(self):
        blotter = _blotter()

        self.assertEqual(self._place(blotter), NAMESPACE + '00000001')
        self.assertEqual(self._place(blotter), NAMESPACE + '00000002')
        self.assertEqual(self._place(blotter, -5), NAMESPACE + '00000003')

    def test_same_orders_same_ids(self):
        first, second = _blotter(), _blotter()

        self.assertEqual(
            [self._place(first, amount) for amount in (10, -20, 30)],
            [self._place(second, amount) for amount in (10, -20, 30)])

    def test_runs_do_not_share_ids(self):
        other = processes.make_process_id('strategy', 'another run')

        self.assertNotEqual(
            self._place(_blotter()),
            self._place(_blotter(), session_id=other))

    def test_a_strategy_and_a_run_are_not_confused(self):
        self.assertNotEqual(
            processes.make_process_id('ab', 'c'),
            processes.make_process_id('a', 'bc'))

    def test_an_order_of_no_shares_takes_no_number(self):
        blotter = _blotter()

        self.assertIsNone(self._place(blotter, 0))
        self.assertEqual(self._place(blotter), NAMESPACE + '00000001')

    def test_a_given_id_is_kept(self):
        blotter = _blotter()

        self.assertEqual(self._place(blotter, order_id='given'), 'given')
        self.assertEqual(self._place(blotter), NAMESPACE + '00000001')

    def test_a_rebuilt_blotter_goes_on_from_the_checkpointed_sequence(self):
        blotter = _blotter()
        self._place(blotter)
        self._place(blotter)
        self.assertEqual(blotter.order_sequence, 3)

        rebuilt = _blotter(start_sequence=blotter.order_sequence)

        self.assertEqual(self._place(rebuilt), NAMESPACE + '00000003')

    def test_the_id_is_compact_and_alphanumeric(self):
        blotter = _blotter()
        order = blotter.orders[self._place(blotter)]

        self.assertTrue(order.id.isalnum())
        self.assertEqual(len(order.id), 24)

    def test_orders_hash_by_any_id(self):
        blotter = _blotter()
        order = blotter.orders[self._place(blotter, order_id='not hexadecimal')]

        self.assertEqual(hash(order), hash('not hexadecimal'))
