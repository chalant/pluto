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
import contextlib as ctx
import tarfile
import tempfile
import unittest
from importlib import import_module

from parameterized import parameterized

from directory import utils as tdr

from zipline import examples as zipline_examples
from zipline.testing import core

from pluto import examples as pluto_examples
from pluto.data import benchmark as bm
from pluto.data.bundles import test as tb
from pluto.trading_calendars import calendar_utils as cu
from pluto.setup import directory_setup as dsp
from pluto.test_utils import test_utils

from tests.test_pluto_examples import _run, ZIPLINE_ONLY

# exact: Pluto equals the fork's Zipline bit for bit on the five examples
# (measured 2026-11-20, ALIGN_PROGRESS.md)
RTOL = 0.0
ATOL = 0.0


def zipline_example_modules():
    """zipline's example modules by name. load_example_modules() leaves
    dual_ema_talib out under numpy 2; it imports where TA-Lib does (it does in
    the environment of this port), so it is added here."""
    modules = dict(zipline_examples.load_example_modules())
    if 'dual_ema_talib' not in modules:
        modules['dual_ema_talib'] = import_module('zipline.examples.dual_ema_talib')
    return modules


def benchmark_returns(environ):
    """The benchmark returns Pluto reads (``ZiplineBenchmark``), in the form
    zipline's run_algorithm takes them (time-zone naive index): both sides then
    work on the very same numbers (the checked-in file of zipline and the one in
    the example data tarball differ in the last digit)."""
    returns = bm.ZiplineBenchmark(environ=environ).read()
    return returns.tz_localize(None)


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
        with tarfile.open(core.test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(dir_.path, filter='data')

        cls._environ = {'ZIPLINE_ROOT': dir_.getpath('example_data/root')}
        cu.register_alias('YAHOO', 'XNYS')
        stack.callback(cu.deregister_alias, 'YAHOO')

    @classmethod
    def tearDownClass(cls):
        cls._exit_stack.close()


class PlutoZiplineOracleTests(ZiplinePlutoFixture):
    @parameterized.expand(sorted(pluto_examples.EXAMPLE_MODULES))
    def test_zipline_and_pluto_agree(self, example_name):
        zipline_perf = zipline_examples.run_example(
            zipline_example_modules(),
            example_name,
            environ=self._environ,
            benchmark_returns=benchmark_returns(self._environ))

        tracker = _run(
            self._pluto_tempdir,
            example_name,
            self._environ)
        pluto_perf = tracker.frame()

        errors, zipline_only, pluto_only = compare_frames(zipline_perf, pluto_perf)

        print('\n[oracle] {}: {} rows, {} common columns compared; zipline only: {}; pluto only: {}'.format(
            example_name, len(zipline_perf),
            len(set(zipline_perf.columns) & set(pluto_perf.columns)),
            sorted(zipline_only), sorted(pluto_only)))

        self.assertEqual(errors, [])
        self.assertEqual(zipline_only, ZIPLINE_ONLY[example_name])
