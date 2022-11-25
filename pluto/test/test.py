import logging

from sqlalchemy import create_engine

import pandas as pd

from numpy import testing
import numpy as np

import empyrical as ep

from dev import strategy_store, events

from pluto import examples
from pluto.sources import benchmark_source as bs

from pluto.utils import stream

from pluto.control.loop import simulation_loop

from pluto.control.processes import processes
from pluto.control.processes import in_memory
from pluto.control.controller import controller

from pluto.coms.utils import conversions

from pluto.strategies import strategies
from pluto.strategies import schema

from pluto.interface import monitor

from pluto.data.universes import core

from protos import controller_pb2


class TestWatcher(monitor.AbstractWatcher):
    def __init__(self,
                 process_id,
                 expected_df,
                 name,
                 trading_sessions,
                 benchmark_source,
                 error_callback):
        super(TestWatcher, self).__init__(process_id)

        self._expected_df = expected_df[examples._cols_to_check]

        self._errors = 0
        self._tests = 0

        self._i = 0

        self._daily_perfs = []
        self._name = name

        self._error_callback = error_callback
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

    @staticmethod
    def _check_is_nan(value):
        if np.isnan(value):
            return 0.0
        return value

    def performance_update(self, packet):
        # todo: need benchmark returns etc.
        #  note: metrics could be determined by the setup
        #  script, then computed by the controller.
        #  the strategies only track returns, portfolio data etc.

        daily_perfs = self._daily_perfs
        # append until end...

        perf = controller_pb2.PerformancePacket()
        perf.ParseFromString(packet.packet)

        perf = conversions.from_proto_performance_packet(perf)

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

        if packet.end:
            # compare values to expected values.
            expected_perf = self._expected_df

            daily_dts = pd.DatetimeIndex(
                [p['period_close'] for p in daily_perfs], tz='UTC'
            )

            daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)[examples._cols_to_check]

            errors = 0
            tests = 0
            idx = 0

            for i, j in zip(expected_perf['capital_used'], daily_stats['capital_used']):
                if abs(round(i, 2) - round(j, 2)) > 0:
                    print(idx, i, j, len(self._benchmark_returns), daily_dts[idx], daily_stats['capital_used'][idx-1], expected_perf['capital_used'][idx+1], expected_perf['capital_used'][idx-1])

                idx += 1

            for l, r in zip(expected_perf.iteritems(), daily_stats.iteritems()):
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
                self._error_callback('{} failed {} out of {}'.format(self._name, errors, tests))


class StrategyTester(object):
    def __init__(self, process, universe, name, process_manager):
        self._process = process
        self._universe = universe
        self._name = name
        self._process_manager = process_manager

    def run(self, loop, expected_perf, benchmark_source, trading_sessions):
        def on_error(message):
            raise AssertionError(message)

        watcher = TestWatcher(
            self._process.process_id,
            expected_perf,
            self._name,
            trading_sessions,
            benchmark_source,
            on_error)

        self._process.watch(watcher)

        # # subscribe to clock events
        # events.subscribe('clock.update', self._process.clock_update)

        # todo: add metrics tracker

        loop.start(controller.Controller(
            self._process_manager))


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
