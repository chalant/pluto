import pandas as pd

from numpy import testing
import numpy as np

import empyrical as ep

from dev import strategy_store

from pluto import examples

from pluto.control.loop import simulation_loop
from pluto.control.processes import processes
from pluto.control.controller import controller
from pluto.control.controller import behavior as bv

from pluto.finance import metrics as mtr


class TestMetricsTracker(object):
    def __init__(
            self,
            name,
            benchmark_source,
            trading_sessions,
            expected_df,
            capital_base):

        self._name = name
        self._benchmark_source = benchmark_source
        self._trading_sessions = trading_sessions

        self._expected_df = expected_df[examples._cols_to_check]

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
        self._first_open_session = ts

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
        expected_perf = self._expected_df

        daily_stats = self.frame()[examples._cols_to_check]

        errors = 0
        tests = 0
        idx = 0

        # for i, j in zip(expected_perf['capital_used'], daily_stats['capital_used']):
        #     if abs(round(i, 2) - round(j, 2)) > 0:
        #         print(idx, i, j, len(self._benchmark_returns), daily_dts[idx], daily_stats['capital_used'][idx - 1],
        #               expected_perf['capital_used'][idx + 1], expected_perf['capital_used'][idx - 1])
        #
        #     idx += 1

        for l, r in zip(expected_perf.items(), daily_stats.items()):
            tests += 1

            des = l[1]
            act = r[1]

            try:
                testing.assert_allclose(
                    np.nan_to_num(des.values),
                    act.values,
                    rtol=0.001,
                    atol=0.02,
                    equal_nan=False)

            except AssertionError as e:
                errors += 1

                print('Name: {}\nError: {}'.format(l[0], e))

            except TypeError:
                # try:
                #     pd.testing.assert_series_equal(act, des, check_less_precise=3)
                # except AssertionError as e:
                #     logging.warning('\nName: {}\nError: {}'.format(l[0], e))
                pass

        if errors > 0:
            raise AssertionError('{} failed {} out of {}'.format(
                self._name,
                errors,
                tests))


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
            capital_base):

        tracker = TestMetricsTracker(
            self._name,
            benchmark_source,
            trading_sessions,
            expected_perf,
            capital_base)

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

        self._store = strategy_store.StrategyStore(directory)
        self._directory = directory

    def add_strategy(self, stp, name, source_path):
        self._store.add_strategy(name, source_path)

        pi = processes.ProcessInitializer(self._controllable)
        init_response = pi.initialize(stp, name)

        # todo: if initial requirements aren't met, raise an error...

        stp.fractional = init_response.fractional
        return pi.start(stp)
