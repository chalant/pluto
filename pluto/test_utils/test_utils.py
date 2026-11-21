import pandas as pd

from numpy import testing
import numpy as np

import empyrical as ep

from pluto.setup import directory_setup

from pluto.control.loop import simulation_loop
from pluto.control.processes import processes
from pluto.control.controller import controller
from pluto.control.controller import behavior as bv

from pluto.finance import metrics as mtr

# Relative tolerance per example for the comparison with the stored expected
# results (tests/resources/example_data.tar.gz); atol is 0 and every other
# example is compared exactly. Pluto equals the fork's Zipline bit for bit on
# all five examples (tests/test_pluto_zipline_oracle.py). For
# momentum_pipeline both differ from the stored results by floating-point
# summation noise, measured 2026-11-20 (ALIGN_PROGRESS.md): largest relative
# difference 1.06e-12 (alpha), 6.4e-13 (pnl), largest absolute 1.3e-10
# (ending_cash, portfolio_value ~1e5).
STORED_RTOL = {'momentum_pipeline': 2e-12}

# columns holding lists of dicts, compared after _normalize
LIST_COLUMNS = ('positions', 'orders', 'transactions')

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


def _values_equal(desired, actual, rtol=0.0, atol=0.0):
    if isinstance(desired, (float, np.floating)) or isinstance(actual, (float, np.floating)):
        if desired is None or actual is None:
            return desired is None and actual is None
        return bool(np.isclose(float(actual), float(desired), rtol=rtol, atol=atol, equal_nan=True))
    return desired == actual


def _lists_equal(desired, actual, rtol=0.0, atol=0.0):
    desired, actual = _normalize(desired), _normalize(actual)
    if len(desired) != len(actual):
        return False
    for a, b in zip(desired, actual):
        if set(a) != set(b):
            return False
        if not all(_values_equal(a[k], b[k], rtol, atol) for k in a):
            return False
    return True


def compare_column(col, desired, actual, rtol=0.0, atol=0.0):
    """Compares one column of two daily result frames (pandas.Series of the
    same length). Returns None if they agree, else an error message.

    Numbers: numpy.testing.assert_allclose(nan_to_num(desired), actual, rtol,
    atol); rtol=atol=0 is exact equality. NaN in desired counts as 0 (Pluto's
    trackers report 0 where zipline reports NaN, e.g. the first day's
    volatility). LIST_COLUMNS: element by element after _normalize, same keys,
    numeric fields with rtol/atol. Anything else (timestamps, labels,
    booleans): exact equality.
    """
    if col in LIST_COLUMNS:
        bad = [i for i, (a, b) in enumerate(zip(desired, actual))
               if not _lists_equal(a, b, rtol, atol)]
        if bad:
            return '{}: {} rows differ, first row {}: desired={} actual={}'.format(
                col, len(bad), bad[0], _normalize(desired.iloc[bad[0]]), _normalize(actual.iloc[bad[0]]))
        return None
    try:
        des = desired.values.astype(float)
        act = actual.values.astype(float)
    except (TypeError, ValueError):
        bad = [i for i, (a, b) in enumerate(zip(desired, actual)) if not a == b]
        if bad:
            return '{}: {} rows differ, first row {}: desired={!r} actual={!r}'.format(
                col, len(bad), bad[0], desired.iloc[bad[0]], actual.iloc[bad[0]])
        return None
    try:
        testing.assert_allclose(np.nan_to_num(des), act, rtol=rtol, atol=atol, equal_nan=False)
    except AssertionError as e:
        return '{}: {}'.format(col, str(e).strip().splitlines()[:6])
    return None


def stored_with_utc_timestamps(frame):
    """A copy of a stored expected frame (pickled with pandas 0.22) in which the
    Timestamps inside the positions, orders and transactions dicts are UTC.

    Under pandas 2 the 0.22 pickles of a time-zone aware Timestamp load as a
    naive Timestamp with the same UTC wall time; the Timestamps of the frame's
    own columns and index keep their zone.
    """
    def fix(value):
        if isinstance(value, pd.Timestamp) and value.tzinfo is None:
            return value.tz_localize('UTC')
        return value

    frame = frame.copy()
    for col in LIST_COLUMNS:
        if col in frame:
            # element by element: numpy would turn equal-length lists into a 2-d array
            fixed = np.empty(len(frame), dtype=object)
            for i, items in enumerate(frame[col]):
                fixed[i] = [{k: fix(v) for k, v in item.items()} for item in items]
            frame[col] = fixed
    return frame


class TestMetricsTracker(object):
    def __init__(
            self,
            name,
            benchmark_source,
            trading_sessions,
            expected_df,
            capital_base,
            trading_calendar):

        self._name = name
        self._benchmark_source = benchmark_source
        self._trading_sessions = trading_sessions
        self._trading_calendar = trading_calendar

        # None: nothing stored to compare with (scenario runs)
        self._expected_df = expected_df

        self._errors = 0
        self._tests = 0

        self._i = 0

        self._daily_perfs = []

        self._benchmark_source = benchmark_source

        self._session_idx = 0

        # arr = np.empty(len(trading_sessions))
        # arr[:] = np.nan
        arr = np.zeros(len(trading_sessions))

        self._algo_daily_returns = arr

        benchmark_returns = benchmark_source.daily_returns_series

        self._benchmark_period_return = (np.cumprod(1 + benchmark_returns.values) - 1)
        self._benchmark_volatility = (benchmark_returns.expanding(2).std(ddof=1) * np.sqrt(252)).values
        self._benchmark_returns = benchmark_returns.values

        self._cumulative_returns = []

        self._metrics = mtr.pluto_metrics()
        self._first_session = trading_sessions[0]
        self._first_open_session = None
        self._capital_base = capital_base

        self._initialized = False

        self._num_sessions = len(trading_sessions)

    @staticmethod
    def _check_is_nan(value):
        if np.isnan(value):
            return 0.0
        return value

    def start_of_session(self, ts, calendar_name, ledger):
        # the daily packet's period_open: the session's execution open, as
        # zipline's MetricsTracker.handle_market_open
        calendar = self._trading_calendar
        self._first_open_session = calendar.session_first_minute(
            ts.tz_localize(None) if ts.tzinfo is not None else ts)

        if not self._initialized:
            for metric in self._metrics:
                metric.initialization(
                    first_session=ts,
                    ledger=ledger)
            self._initialized = True

        for metric in self._metrics:
            metric.start_of_session(
                dt=ts,
                session=ts,
                ledger=ledger)

    def end_of_bar(self, ts, calendar_name, ledger):
        daily_perfs = self._daily_perfs
        # append until end...

        first_session = self._first_session

        perf = {
            'period_start': first_session,
            'period_end': ts,
            'capital_base': self._capital_base,
            'daily_perf': {
                'period_open': self._first_open_session,
                'period_close': ts,
            },
            'cumulative_perf': {
                'period_open': first_session,
                'period_close': ts,
            },
            'progress': None,
            'cumulative_risk_metrics': {},
        }

        for metric in self._metrics:
            metric.end_of_session(
                dt=ts,
                packet=perf,
                ledger=ledger)

        self._cumulative_returns.append(ledger.returns)

        daily_perf = perf.get('daily_perf', None)
        daily_perf.update(perf['cumulative_risk_metrics'])

        cum_perf = perf['cumulative_perf']
        cum_perf.pop('period_close')
        cum_perf.pop('period_open')
        cum_perf.pop('capital_used')
        cum_perf.pop('starting_exposure')
        cum_perf.pop('ending_exposure')
        cum_perf.pop('starting_value')
        cum_perf.pop('starting_cash')
        cum_perf.pop('returns')
        cum_perf.pop('pnl')

        daily_perf.update(cum_perf)

        session_idx = self._session_idx

        self._algo_daily_returns[session_idx] = daily_perf['returns']

        daily_perf['benchmark_period_return'] = self._benchmark_period_return[session_idx]
        daily_perf['benchmark_volatility'] = self._check_is_nan(self._benchmark_volatility[session_idx])

        self._session_idx += 1

        algo_returns = self._algo_daily_returns[:session_idx + 1]

        a, b = ep.alpha_beta_aligned(
            algo_returns,
            self._benchmark_returns[:session_idx + 1],
        )

        daily_perf['algo_volatility'] = self._check_is_nan(ep.annual_volatility(algo_returns))
        daily_perf['alpha'] = self._check_is_nan(a)
        daily_perf['beta'] = self._check_is_nan(b)

        daily_perf['sharpe'] = self._check_is_nan(ep.sharpe_ratio(algo_returns))
        daily_perf['sortino'] = self._check_is_nan(ep.sortino_ratio(algo_returns))
        daily_perf['max_drawdown'] = ep.max_drawdown(algo_returns)

        daily_perfs.append(daily_perf)

    def end_of_session(self, ts, calendar_name, ledger):
        if self._session_idx < self._num_sessions:
            self.end_of_bar(ts, calendar_name, ledger)

    def frame(self):
        """The daily packets as a DataFrame indexed by period_close."""
        daily_perfs = self._daily_perfs

        daily_dts = pd.DatetimeIndex([p['period_close'] for p in daily_perfs], tz='UTC')

        return pd.DataFrame(daily_perfs, index=daily_dts)

    def validate(self):
        """Compares every column that the stored expected results and this run
        both have (compare_column, rtol from STORED_RTOL, atol 0). The stored
        frames are read with stored_with_utc_timestamps."""
        expected_perf = self._expected_df
        if expected_perf is None:
            return

        daily_stats = self.frame()
        rtol = STORED_RTOL.get(self._name, 0.0)

        expected_perf = stored_with_utc_timestamps(expected_perf)

        common = sorted(set(expected_perf.columns) & set(daily_stats.columns))
        errors = []
        if len(expected_perf) != len(daily_stats):
            errors.append('row count stored {} vs pluto {}'.format(len(expected_perf), len(daily_stats)))
        else:
            for col in common:
                error = compare_column(col, expected_perf[col], daily_stats[col], rtol=rtol)
                if error:
                    errors.append(error)

        if errors:
            raise AssertionError('{} failed {} out of {} columns:\n{}'.format(
                self._name,
                len(errors),
                len(common),
                '\n'.join(errors)))


class StrategyTester(object):
    def __init__(self, process, universe, name, process_manager):
        self._process = process
        self._universe = universe
        self._name = name
        self._process_manager = process_manager

    def run(self,
            loop: simulation_loop.SimulationLoop,
            expected_perf,
            benchmark_source,
            trading_sessions,
            market,
            controller_behavior: bv.AbstractControllerBehavior,
            capital_base,
            trading_calendar):

        tracker = TestMetricsTracker(
            self._name,
            benchmark_source,
            trading_sessions,
            expected_perf,
            capital_base,
            trading_calendar)

        for _ in loop.start(controller.Controller(
                self._process_manager,
                market,
                tracker,
                controller_behavior)):
            pass

        # validate results
        tracker.validate()

        return tracker


class TestClient(object):
    def __init__(self, directory, controllable):
        """

        Parameters
        ----------
        directory: directory.directory.Directory

        """

        self._controllable = controllable

        self._directory = directory

    def add_strategy(self, stp, name, source_path):
        # the controllable reads the script from <root>/strategies/<name>.py
        directory_setup.add_strategy_script(self._directory, name, source_path)

        pi = processes.ProcessInitializer(self._controllable)
        init_response = pi.initialize(stp, name)

        # todo: if initial requirements aren't met, raise an error...

        stp.fractional = init_response.fractional
        return pi.start(stp)
