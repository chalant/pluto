"""Oracle: each example runs through the fork's plain Zipline path
(zipline.examples.run_example -> run_algorithm -> AlgorithmSimulator) and through
Pluto's in-memory controller/controllable path (tests.test_pluto_examples._run),
in the same process and on the same test bundle; the daily result frames must
agree exactly (rtol=0, atol=0) on every column both produce.

Compared (pluto.test_utils.test_utils.compare_column): the numeric columns,
period_open/period_close/period_label, positions, orders and transactions
(order ids and session ids are left out; lists are compared order-insensitively,
with the same keys). Columns produced by only one side are listed and checked
against the known set below.
"""
import tarfile
import tempfile
import unittest

import contextlib2 as ctx
import pandas as pd
from nose_parameterized import parameterized
from trading_calendars import calendar_utils

from directory import utils as tdr

from zipline import examples as zipline_examples
from zipline.testing import test_resource_path, core
from zipline.utils import cache
from zipline.utils.paths import update_modified_time

from pluto import examples as pluto_examples
from pluto.data.bundles import test as tb
from pluto.setup import directory_setup as dsp
from pluto.test_utils import test_utils

from tests.test_pluto_examples import _run

# exact: Pluto equals the fork's Zipline bit for bit on the five examples
# (measured 2026-11-20, ALIGN_PROGRESS.md)
RTOL = 0.0
ATOL = 0.0

# zipline's perf frame has these and Pluto's tracker does not:
# recorded variables: the zipline examples call record(); the Pluto ports of
# the examples have their record() calls commented out, and the controller
# side tracker has no recorded-variables channel.
ZIPLINE_ONLY = {
    'buy_and_hold': set(),
    'buyapple': {'AAPL'},
    'dual_ema_talib': {'AAPL', 'short_ema', 'long_ema', 'buy', 'sell'},
    'dual_moving_average': {'AAPL', 'short_mavg', 'long_mavg'},
    'momentum_pipeline': {'universe_size'},
}


def compare_frames(zipline_perf, pluto_perf, rtol=RTOL, atol=ATOL):
    """Returns (errors, zipline_only_columns, pluto_only_columns)."""
    errors = []

    if len(zipline_perf) != len(pluto_perf):
        return (['row count zipline {} vs pluto {}'.format(len(zipline_perf), len(pluto_perf))],
                set(), set())

    if not (zipline_perf.index == pluto_perf.index).all():
        errors.append('index (period_close) differs')

    common = sorted(set(zipline_perf.columns) & set(pluto_perf.columns))
    for col in common:
        error = test_utils.compare_column(col, zipline_perf[col], pluto_perf[col], rtol, atol)
        if error:
            errors.append(error)

    return (errors,
            set(zipline_perf.columns) - set(pluto_perf.columns),
            set(pluto_perf.columns) - set(zipline_perf.columns))


class ZiplinePlutoFixture(unittest.TestCase):
    """Pluto test bundle ingested in a temp root, zipline example data
    extracted in another; shared with tests/test_pluto_scenarios.py."""

    @classmethod
    def setUpClass(cls):
        cls._exit_stack = stack = ctx.ExitStack()

        root_dir = tdr.TempDirectory(dsp.create_directory(tempfile.mkdtemp(prefix='pluto_tmp_')))
        stack.enter_context(tb.TestBundle(root_dir)).ingest()
        cls._pluto_tempdir = stack.enter_context(root_dir)

        cls._zpl_tempdir = dir_ = stack.enter_context(core.tmp_dir())
        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(dir_.path)

        # as zipline's tests/test_examples.py: the market data files must look
        # fresh, otherwise zipline's loader tries to download them.
        for name in ('SPY_benchmark.csv', 'treasury_curves.csv'):
            update_modified_time(dir_.getpath('example_data/root/data/' + name))

        cls._environ = {'ZIPLINE_ROOT': dir_.getpath('example_data/root')}
        cls._expected_perf = cache.dataframe_cache(
            dir_.getpath('example_data/expected_perf/%s' % pd.__version__.replace('.', '-')),
            serialization='pickle')

        calendar_utils._default_calendar_aliases['YAHOO'] = 'XNYS'
        stack.callback(calendar_utils._default_calendar_aliases.pop, 'YAHOO')

    @classmethod
    def tearDownClass(cls):
        cls._exit_stack.close()


class PlutoZiplineOracleTests(ZiplinePlutoFixture):
    @parameterized.expand(sorted(pluto_examples.EXAMPLE_MODULES))
    def test_zipline_and_pluto_agree(self, example_name):
        zipline_perf = zipline_examples.run_example(example_name, environ=self._environ)

        tracker = _run(
            self._pluto_tempdir,
            example_name,
            self._expected_perf[example_name],
            self._environ)
        pluto_perf = tracker.frame()

        errors, zipline_only, pluto_only = compare_frames(zipline_perf, pluto_perf)

        print('\n[oracle] {}: {} rows, {} common columns compared; zipline only: {}; pluto only: {}'.format(
            example_name, len(zipline_perf),
            len(set(zipline_perf.columns) & set(pluto_perf.columns)),
            sorted(zipline_only), sorted(pluto_only)))

        self.assertEqual(errors, [])
        self.assertEqual(zipline_only, ZIPLINE_ONLY[example_name])
