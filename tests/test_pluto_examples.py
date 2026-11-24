import unittest
import contextlib as ctx
from os import path, environ
import tarfile
import tempfile
from concurrent import futures

from parameterized import parameterized

import pandas as pd

from directory import utils as tdr

from zipline.testing import core
from zipline.data import data_portal
from zipline.utils import cache
from zipline.assets.assets import Equity, Future
from zipline.finance.execution import (StopOrder, LimitOrder, StopLimitOrder, MarketOrder)
from zipline.finance import asset_restrictions

from pluto import examples
from pluto.test_utils import test_utils

from pluto.sources import benchmark_source as source

from pluto.finance.blotter import simulation_blotter
from pluto.finance.broker import broker_setup as bks
from pluto.finance.markets import market_simulation

from pluto.control.processes import in_memory
from pluto.control.controller import process_manager
from pluto.control.controller import simulation_behavior
from pluto.control.controllable import running_behavior
from pluto.control.loop import simulation_loop

from pluto.data import caching
from pluto.data.bundles import test as tb
from pluto.data.universes import core as uni_core

from pluto.trading_calendars import calendar_utils as cu

from pluto.setup import run_setup
from pluto.setup import directory_setup as dsp
from pluto.setup import engine_setup

# the stored expected results are the set generated on this stack by zipline's own
# run path (tests/resources/add_expected_perf.py), stored in the example data
# under the pandas version in use (dashes: 2-3-3 in the pinned environment)
NEW_PANDAS_VERSION = pd.__version__.replace('.', '-')

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


def _run(dir_, example_name, environ_, modules=examples.EXAMPLE_MODULES):
    """modules: name -> (_test_args(), path of the strategy file); the
    examples by default, tests/pluto_scenarios for the scenario tests."""
    args, path_ = modules[example_name]

    # setup strategy...

    stp = run_setup.RunSetup('test')

    stp.environ = environ_

    # stp.broker = 'none'
    stp.trading_universe = 'test'
    stp.capital = args['capital_base']

    stp.leverage = args['max_leverage']
    stp.max_leverage = args['max_leverage']
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

    uni = uni_core.get_universe(
        stp.trading_universe,
        dir_,
        caching.InMemoryCache(),
        stp.environ
    )

    manager = process_manager.StrategyManager(uni.asset_finder, stp)

    restrictions = asset_restrictions.NoRestrictions()

    market = market_simulation.MarketSimulation(eng_stp, stp.data_frequency)
    controller_behavior = simulation_behavior.SimulationControllerBehavior(stp.data_frequency)

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
        last_available_minute=calendar.session_last_minute(last_session)
    )

    market.add_bar_data(dp, restrictions)

    controller_behavior.add_data_portal(calendar_name, dp)

    manager.add_data_portal(calendar_name, dp)
    manager.add_ledger(calendar_name)
    manager.add_blotter(
        calendar_name,
        simulation_blotter.SimulationBlotter(
            eng_stp))

    loop.add_clock(calendar_name)

    client = test_utils.TestClient(
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

    tester = test_utils.StrategyTester(
        process,
        uni,
        example_name,
        manager)

    sessions = calendar.sessions

    benchmark_source = source.SimulationBenchmarkSource(
        calendar,
        calendar.sessions,
        uni.benchmark,
        150)

    return tester.run(
        loop,
        benchmark_source,
        sessions,
        market,
        controller_behavior,
        stp.capital,
        calendar)

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
    Exact comparison, column by column (test_utils.compare_column, rtol=0,
    atol=0, non-numeric columns included).

    Parameters
    ----------
    desired: pandas.DataFrame
    actual: pandas.DataFrame

    Returns
    -------

    """

    errors = [e for e in (test_utils.compare_column(col, desired[col], actual[col])
                          for col in desired.columns) if e]

    if errors:
        raise AssertionError('failed {} out of {}:\n{}'.format(
            len(errors), len(desired.columns), '\n'.join(errors)))


class PlutoExamplesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._exit_stack = stack = ctx.ExitStack()
        cls._thread_pool = stack.enter_context(
            futures.ThreadPoolExecutor(10))

        # fresh directories each run: a fixed path (~/pluto_tmp) kept the
        # strategy store of an interrupted run and the next run failed with
        # FileExistsError: Strategy of name ... already exists
        root_dir = tdr.TempDirectory(dsp.create_directory(tempfile.mkdtemp(prefix='pluto_tmp_')))

        # ingest test bundle
        test_bundle = stack.enter_context(tb.TestBundle(root_dir))

        # ingest the test bundle
        test_bundle.ingest()

        cls._pluto_tempdir = stack.enter_context(root_dir)

        # bundles.register('test', lambda *args: None)

        cls._zpl_tempdir = dir_ = stack.enter_context(core.tmp_dir())

        with tarfile.open(core.test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(dir_.path, filter='data')

        # environ.setdefault('ZIPLINE_ROOT', dir_.getpath('example_data/root'))

        cls._environ = {'ZIPLINE_ROOT': dir_.getpath('example_data/root')}

        # the set generated on this stack by zipline's own run path
        # (tests/resources/add_expected_perf.py), the only stored reference;
        # the key is the pandas version in use (2-3-3 in the pinned environment)
        cls._new_expected_perf = cache.dataframe_cache(
            dir_.getpath('example_data/expected_perf/%s' % NEW_PANDAS_VERSION),
            serialization='pickle',
        )
        cls._frames = {}

        cls._framework_url = '[::]:50051'

        cu.register_alias('YAHOO', 'XNYS')

    @classmethod
    def tearDownClass(cls) -> None:
        cu.deregister_alias('YAHOO')
        cls._exit_stack.close()

    def _pluto_frame(self, example_name):
        """The daily result frame of Pluto's run of an example (run once per
        class: the run is deterministic)."""
        frames = type(self)._frames
        if example_name not in frames:
            frames[example_name] = _run(
                self._pluto_tempdir, example_name, self._environ).frame()
        return frames[example_name]

    def check_new_expected(self, example_name):
        """Pluto against the results generated on this stack by zipline's own
        run path (tests/resources/add_expected_perf.py): exact on every column
        both have; the columns that only zipline has are its recorded variables."""
        desired = self._new_expected_perf[example_name]
        actual = self._pluto_frame(example_name)

        self.assertTrue((desired.index == actual.index).all())
        errors = test_utils.frame_errors(desired, actual)
        self.assertEqual(errors, [])
        self.assertEqual(set(actual.columns) - set(desired.columns), set())
        self.assertEqual(set(desired.columns) - set(actual.columns),
                         ZIPLINE_ONLY[example_name])

    @parameterized.expand(sorted(examples.EXAMPLE_MODULES))
    def test_simulation(self, example_name):
        self.check_new_expected(example_name)
