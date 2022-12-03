import unittest
import contextlib2 as ctx
from os import path, environ
import tarfile
import logging
from concurrent import futures

from nose_parameterized import parameterized

import pandas as pd

from numpy import testing
import numpy as np

from trading_calendars import calendar_utils

from directory import utils as tdr


from zipline.testing import test_resource_path, core
from zipline.data import data_portal
from zipline.utils import cache
from zipline.assets.assets import Equity, Future
from zipline.finance.execution import (
    StopOrder,
    LimitOrder,
    StopLimitOrder,
    MarketOrder)
from zipline.finance import asset_restrictions

from pluto import examples
from pluto.test import test

from pluto.sources import benchmark_source as source

from pluto.finance import ledger
from pluto.finance.blotter import simulation_blotter
from pluto.finance.broker import broker_setup as bks

from pluto.control.processes import in_memory
from pluto.control.controller import process_manager
from pluto.control.controllable import running_behavior
from pluto.control.loop import simulation_loop

from pluto.setup import run_setup
from pluto.setup import directory_setup as ds
from pluto.setup import engine_setup


from pluto.data.bundles import test as tb
from pluto.data.universes import core
from pluto.data import caching

from pluto.trading_calendars import calendar_utils as cu


def _build_data_frame(mon, process_id):
    daily_perfs = []

    for perf in mon.watch(process_id):
        daily_perf = perf.get('daily_perf', None)
        daily_perf.update(perf['cumulative_risk_metrics']),
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
        daily_perfs.append(daily_perf)

    daily_dts = pd.DatetimeIndex(
        [p['period_close'] for p in daily_perfs], tz='UTC'
    )

    daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)
    return daily_stats


def _run(dir_, example_name, expected_perf, environ):
    args, path_ = examples.EXAMPLE_MODULES[example_name]

    # setup strategy...

    stp = run_setup.RunSetup('test')

    stp.environ = environ

    # stp.broker = 'none'
    stp.trading_universe = 'test'
    stp.capital = args['capital_base']

    stp.leverage = args['max_leverage']
    stp.look_back = args['look_back']
    stp.data_frequency = args['data_frequency']
    stp.start = args['start']
    stp.end = args['end']

    stp.clear_perf = True
    stp.remote_cache = False

    fts = bks.FractionalTradeSetup()
    ts = bks.TradeSetup()
    bs = bks.BrokerSetup()

    fts.asset_types = {Equity.__name__}
    fts.order_types = {MarketOrder.__name__}
    fts.minimal_fraction = 3
    fts.minimum_purchase = 1.0

    ts.asset_types = {
        Equity.__name__,
        Future.__name__}

    ts.order_types = {
        StopOrder.__name__,
        LimitOrder.__name__,
        StopLimitOrder.__name__,
        MarketOrder.__name__}

    bs.fractional = True

    stp.fractional_trade_setup = fts
    stp.trade_setup = ts

    if 'commission_models' in args:
        stp.commission_models = args['commission_models']

    if 'slippage_models' in args:
        stp.slippage_models = args['slippage_models']

    stp.tax_models = {}

    eng_stp = engine_setup.EngineSetup()
    eng_stp.slippage_models = stp.commission_models

    eng_stp.slippage_models = {
        Equity: stp.slippage_models['equity'],
        Future: stp.slippage_models['futures']
    }

    eng_stp.commission_models = {
        Equity: stp.commission_models['equity'],
        Future: stp.commission_models['futures']
    }

    uni = universes.get_universe(
        stp.trading_universe,
        dir_,
        caching.InMemoryCache(),
        stp.environ
    )

    manager = process_manager.StrategyManager(
        uni.asset_finder,
        stp.start,
        stp.capital,
        stp.emission_rate
    )

    restrictions = asset_restrictions.NoRestrictions()

    loop = simulation_loop.SimulationLoop(
        stp.start,
        stp.end,
        stp.data_frequency
    )

    calendar_name = 'XNYS'

    calendar = cu.get_calendar_in_range(
        calendar_name,
        stp.start,
        stp.end)

    last_session = calendar.last_session

    dp = data_portal.DataPortal(
        asset_finder=uni.asset_finder,
        trading_calendar=calendar,
        first_trading_day=calendar.first_session,
        equity_minute_reader=uni.equity_minute_bar_reader,
        equity_daily_reader=uni.equity_daily_bar_reader,
        adjustment_reader=uni.adjustment_reader,
        last_available_session=last_session,
        last_available_minute=calendar.minutes_for_session(last_session)[-1]
    )

    manager.add_data_portal(
        calendar_name,
        dp)

    manager.add_bar_data(
        'daily',
        dp,
        restrictions)

    manager.add_blotter(
        calendar_name,
        simulation_blotter.SimulationBlotter(
            eng_stp))

    # manager.add_metrics_tracker(
    #     calendar_name,
    #     'daily'
    # )

    loop.add_clock(calendar_name)

    client = test.TestClient(
        dir_,
        in_memory.InMemoryControllableStub(
            dir_,
            'backtesting',
            manager,
            running_behavior.Blocking()))

    process = client.add_strategy(
        stp,
        example_name,
        path_
    )

    manager.add_strategy(process)

    tester = test.StrategyTester(
        process,
        uni,
        example_name,
        manager)

    sessions = calendar.all_sessions

    benchmark_source = source.SimulationBenchmarkSource(
        calendar,
        calendar.all_sessions,
        uni.benchmark,
        150)

    tester.run(
        loop,
        expected_perf,
        benchmark_source,
        sessions)

    # capital = 1e7

    # max_leverage = args['max_leverage']
    # session_id = client.setup(
    #     response.strategy_id,
    #     args['start'],
    #     args['end'],
    #     capital,
    #     max_leverage,
    #     args['look_back'])

    # client.run(session_id, ratio, max_leverage)

    # expected_perf = expected_perf[example_name]
    # daily_stats = _build_data_frame(mon, session_id)
    #
    # _compare_df(
    #     expected_perf[examples._cols_to_check],
    #     daily_stats[examples._cols_to_check])


def _compare_df(desired, actual):
    """

    Parameters
    ----------
    desired: pandas.DataFrame
    actual: pandas.DataFrame

    Returns
    -------

    """

    errors = 0
    tests = 0

    for l, r in zip(desired.iteritems(), actual.iteritems()):
        tests += 1
        act = r[1]
        des = l[1]

        try:
            testing.assert_allclose(
                np.nan_to_num(des.values),
                act.values,
                rtol=0.1,
                atol=0.2,
                equal_nan=False)
        except AssertionError as e:
            errors += 1
            print('Name: {}\nError: {}'.format(l[0], e))
        except TypeError:
            try:
                pd.testing.assert_series_equal(des, act, check_less_precise=3)
            except AssertionError as e:
                logging.warning('\nName: {}\nError: {}'.format(l[0], e))

    if errors > 0:
        raise AssertionError('failed {} out of {}'.format(errors, tests))


class PlutoExamplesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._exit_stack = stack = ctx.ExitStack()
        cls._thread_pool = stack.enter_context(
            futures.ThreadPoolExecutor(10))

        root_dir = tdr.TempDirectory(ds.create_directory(path.expanduser('~/pluto_tmp')))

        # ingest test bundle
        # test_bundle = stack.enter_context(tb.ClickHouseTestBundle(
        #     root_dir,
        #     caching.InMemoryCache()
        # ))

        test_bundle = stack.enter_context(tb.TestBundle(root_dir))

        # ingest the test bundle
        test_bundle.ingest()

        cls._pluto_tempdir = stack.enter_context(root_dir)

        # bundles.register('test', lambda *args: None)

        cls._zpl_tempdir = dir_ = stack.enter_context(
            core.tmp_dir(path=path.expanduser('~/tmp/zipline')))

        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(dir_.path)

        # environ.setdefault('ZIPLINE_ROOT', dir_.getpath('example_data/root'))

        cls._environ = {'ZIPLINE_ROOT': dir_.getpath('example_data/root')}

        cls._expected_perf = cache.dataframe_cache(
            dir_.getpath(
                'example_data/expected_perf/%s' %
                pd.__version__.replace('.', '-'),
            ),
            serialization='pickle',
        )

        cls._framework_url = '[::]:50051'

        calendar_utils._default_calendar_aliases['YAHOO'] = 'XNYS'

    @classmethod
    def tearDownClass(cls) -> None:
        calendar_utils._default_calendar_aliases.pop('YAHOO')
        cls._exit_stack.close()

    @parameterized.expand(sorted(examples.EXAMPLE_MODULES))
    def test_simulation(self, example_name):
        # monitor for performance callbacks...

        # process, uni = _create_process(
        #     _STP,
        #     example_name,
        #     in_memory.InMemoryControllableStub(dir_, 'backtesting'))

        # print("Example ", example_name)

        _run(self._pluto_tempdir,
             example_name,
             self._expected_perf[example_name],
             self._environ)

    # @parameterized.expand(sorted(examples.EXAMPLE_MODULES))
    # def test_live_simulation(self, example_name):
    #     thread_pool = self._thread_pool
    #     framework_url = self._framework_url
    #
    #     client = test.InMemoryTestClient(
    #         self._pluto_tempdir,
    #         framework_url,
    #         mode_utils.LiveSimulationModeFactory(
    #             framework_url,
    #             factory.LiveSimulationMarketFactory(
    #                 blotter_factory.MultiSimulationBlotterFactory()),
    #             thread_pool),
    #         loop_utils.SimpleSimulationLoopFactory())
    #
    #     _run(client, example_name, self._expected_perf)
