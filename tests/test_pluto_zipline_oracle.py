"""Oracle: each example runs through the fork's plain Zipline path
(zipline.examples.run_example -> run_algorithm -> AlgorithmSimulator) and through
Pluto's in-memory controller/controllable path (tests.test_pluto_examples._run),
in the same process and on the same test bundle; the daily result frames must
agree on every column both produce, with the tolerances of
tests/test_pluto_examples.py (rtol=0.001, atol=0.02).

Compared: the numeric columns, period_open/period_close/period_label, positions,
orders and transactions (order ids and session ids are left out; lists are
compared order-insensitively). Columns produced by only one side are listed and
checked against the known set below.

period_open is compared at session level: zipline stamps the execution open
(e.g. 14:31 UTC), Pluto's trackers stamp the session label (midnight); the
execution-open computation is commented out in pluto/finance/metrics/tracker.py,
controller_tracker.py and the test tracker. Reported, not reconciled.
"""
import tarfile
import tempfile
import unittest
from os import path

import contextlib2 as ctx
import numpy as np
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

from tests.test_pluto_examples import _run

RTOL = 0.001
ATOL = 0.02

# zipline's perf frame has these and Pluto's tracker does not:
# - 'sharpe': computed by Pluto's tracker too (present), listed for safety
# - recorded variables: the zipline examples call record(); the Pluto ports of
#   the examples have their record() calls commented out, and the controller
#   side tracker has no recorded-variables channel.
ZIPLINE_ONLY = {
    'buy_and_hold': set(),
    'buyapple': {'AAPL'},
    'dual_ema_talib': {'AAPL', 'short_ema', 'long_ema', 'buy', 'sell'},
    'dual_moving_average': {'AAPL', 'short_mavg', 'long_mavg'},
    'momentum_pipeline': {'universe_size'},
}

# identifiers: random uuids (id, order_id) and the strategy session id that
# Pluto appends to its orders ('' for zipline's own TradingAlgorithm)
_ID_FIELDS = {'id', 'order_id', 'session_id'}


def _sid(value):
    return int(getattr(value, 'sid', value))


def _normalize(items):
    """List of order/transaction/position dicts -> sorted list of plain dicts
    (assets as sids, identifiers dropped)."""
    out = []
    for item in items:
        d = {}
        for k, v in item.items():
            if k in _ID_FIELDS:
                continue
            d[k] = _sid(v) if k in ('sid', 'asset') else v
        out.append(d)
    return sorted(out, key=lambda d: (d.get('sid', d.get('asset')),
                                      str(d.get('dt')),
                                      float(d.get('amount') or 0)))


def _values_equal(a, b):
    if isinstance(a, (float, np.floating)) or isinstance(b, (float, np.floating)):
        if a is None or b is None:
            return a is None and b is None
        return bool(np.isclose(np.nan_to_num(float(b)), float(a), rtol=RTOL, atol=ATOL))
    return a == b


def _lists_equal(zl, pl):
    zl, pl = _normalize(zl), _normalize(pl)
    if len(zl) != len(pl):
        return False
    for a, b in zip(zl, pl):
        if set(a) != set(b):
            return False
        if not all(_values_equal(b[k], a[k]) for k in a):
            return False
    return True


def compare_frames(zipline_perf, pluto_perf):
    """Returns (errors, zipline_only_columns, pluto_only_columns)."""
    errors = []

    if len(zipline_perf) != len(pluto_perf):
        return (['row count zipline {} vs pluto {}'.format(len(zipline_perf), len(pluto_perf))],
                set(), set())

    if not (zipline_perf.index == pluto_perf.index).all():
        errors.append('index (period_close) differs')

    common = sorted(set(zipline_perf.columns) & set(pluto_perf.columns))
    for col in common:
        z, p = zipline_perf[col], pluto_perf[col]
        if col in ('positions', 'orders', 'transactions'):
            bad = [i for i, (a, b) in enumerate(zip(z, p)) if not _lists_equal(a, b)]
            if bad:
                errors.append('{}: {} rows differ, first row {}: zipline={} pluto={}'.format(
                    col, len(bad), bad[0], _normalize(z.iloc[bad[0]]), _normalize(p.iloc[bad[0]])))
            continue
        if col in ('period_open', 'period_close', 'period_label'):
            def label(v):
                if col == 'period_label':
                    return str(v)
                ts = pd.Timestamp(v)
                return str(ts.normalize() if col == 'period_open' else ts)
            zs = [label(v) for v in z]
            ps = [label(v) for v in p]
            bad = [i for i, (a, b) in enumerate(zip(zs, ps)) if a != b]
            if bad:
                errors.append('{}: {} rows differ, first row {}: zipline={} pluto={}'.format(
                    col, len(bad), bad[0], zs[bad[0]], ps[bad[0]]))
            continue
        try:
            np.testing.assert_allclose(
                np.nan_to_num(z.values.astype(float)),
                p.values.astype(float),
                rtol=RTOL,
                atol=ATOL,
                equal_nan=False)
        except AssertionError as e:
            errors.append('{}: {}'.format(col, str(e).strip().splitlines()[:6]))

    return (errors,
            set(zipline_perf.columns) - set(pluto_perf.columns),
            set(pluto_perf.columns) - set(zipline_perf.columns))


class PlutoZiplineOracleTests(unittest.TestCase):
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
