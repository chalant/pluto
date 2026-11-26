"""The strategy runtime's view of its own portfolio (``context.portfolio``, ``context.account``).

The runtime has its own ledger, revalued lazily: each revaluation compounds ``returns`` and ``pnl``, so what a strategy
reads depends on where the ledger was revalued. At the end of a session and at the end of a minute the runtime revalues
it, where the performance packet it used to compute read the portfolio (Zipline's ``handle_market_close`` reads it there
too). Tests:

- the strategy of tests/runtime_portfolio_strategy.py records what it sees of its portfolio and account; the records are
  equal, bit for bit, to the ones captured on ``origin/master`` 6a6941d6 (before the packet was deleted), stored in
  tests/resources/runtime_portfolio_records.json (the runtime's view is not exactly Zipline's: it shows e.g. a pnl that
  differs from Zipline's at the 7th digit, so Zipline is not the reference here);
- the runtime ledger is up to date at the end of every session of a run;
- ``minute_end`` revalues a dirty ledger (the bundle runs are daily: a unit test).
"""
import itertools
import json
import os
import types
import unittest
from unittest import mock

import pandas as pd

from zipline import protocol

from pluto.control.controllable import controllable
from pluto.finance import ledger, position_tracker

from tests import runtime_portfolio_strategy as strategy
from tests.test_pluto_examples import _run
from tests.test_pluto_zipline_oracle import ZiplinePlutoFixture

RUNS = itertools.count()
REFERENCE = os.path.join(os.path.dirname(__file__), 'resources', 'runtime_portfolio_records.json')


class RuntimePortfolioTests(ZiplinePlutoFixture):
    def run_pluto(self):
        args = strategy._test_args()
        strategy.RECORDS.clear()

        # a strategy script is installed once per root directory: each run has its own name
        alias = 'runtime_portfolio_{}'.format(next(RUNS))
        _run(self._pluto_tempdir, alias, self._environ,
             modules={alias: (args, strategy.__spec__.origin)})

        return list(strategy.RECORDS)

    def test_the_strategy_sees_the_portfolio_it_saw_before(self):
        with open(REFERENCE) as f:
            reference = json.load(f)

        records = self.run_pluto()

        self.assertEqual(len(reference), 15)
        self.assertEqual([[float(value).hex() for value in record] for record in records], reference)

    def test_the_runtime_ledger_is_up_to_date_at_the_end_of_a_session(self):
        dirty = []
        original = controllable.Controllable.session_end

        def session_end(self, dt):
            result = original(self, dt)
            dirty.append(self._metrics_tracker.ledger._dirty_portfolio)
            return result

        with mock.patch.object(controllable.Controllable, 'session_end', session_end):
            self.run_pluto()

        self.assertGreater(len(dirty), 40)
        self.assertEqual(set(dirty), {False})


class MinuteEndTests(unittest.TestCase):
    def test_minute_end_revalues_the_runtime_ledger(self):
        start = pd.Timestamp('2014-01-02', tz='UTC')
        dt = pd.Timestamp('2014-01-03 15:00', tz='UTC')
        book = ledger.Ledger(
            start, protocol.Portfolio(start, 1000.0), protocol.Account(),
            position_tracker.PositionTracker(protocol.Positions()))
        # what an account update does to the runtime's ledger
        book._dirty_portfolio = True
        runtime = types.SimpleNamespace(_metrics_tracker=types.SimpleNamespace(ledger=book), _end_dt=dt)

        ended = controllable.Controllable.minute_end(runtime, dt)

        self.assertTrue(ended)
        self.assertFalse(book._dirty_portfolio)
        self.assertFalse(controllable.Controllable.minute_end(runtime, dt - pd.Timedelta(minutes=1)))


if __name__ == '__main__':
    unittest.main()
