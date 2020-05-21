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

from zipline.testing import test_resource_path, core
from zipline.data import bundles
from zipline.utils import cache

from pluto.interface import directory
from pluto import examples
from pluto.test import test
from pluto.control.modes import utils as mode_utils
from pluto.control.loop import utils as loop_utils
from pluto.control.modes.market import factory
from pluto.control.modes.market import blotter_factory


def _build_data_frame(test_client, session_id):
    daily_perfs = []
    for perf in test_client.watch(session_id):
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


def _run(client, example_name, expected_perf):
    response = client.add_strategy(example_name)
    args, path_ = examples.EXAMPLE_MODULES[example_name]

    with open(path_, 'r') as f:
        # save the strategy as byte in the file system
        client.save(response.strategy_id, f.read().encode('utf-8'))

    capital = 1e7
    ratio = args['capital_base'] / capital

    max_leverage = args['max_leverage']
    session_id = client.setup(
        response.strategy_id,
        args['start'],
        args['end'],
        capital,
        max_leverage,
        args['look_back'])

    client.run(session_id, ratio, max_leverage)

    expected_perf = expected_perf[example_name]
    daily_stats = _build_data_frame(client, session_id)

    _compare_df(
        expected_perf[examples._cols_to_check],
        daily_stats[examples._cols_to_check])


def _compare_df(desired, actual):
    '''

    Parameters
    ----------
    desired: pandas.DataFrame
    actual: pandas.DataFrame

    Returns
    -------

    '''
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
    def setUp(self) -> None:
        self._exit_stack = stack = ctx.ExitStack()
        self._thread_pool = stack.enter_context(
            futures.ThreadPoolExecutor(10))
        self._pluto_tempdir = stack.enter_context(
            directory.get_directory('test'))

        bundles.register('test', lambda *args: None)

        self._zpl_tempdir = dir_ = stack.enter_context(
            core.tmp_dir(path=path.expanduser('~/tmp/zipline')))

        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(dir_.path)

        environ.setdefault('ZIPLINE_ROOT', dir_.getpath('example_data/root'))

        self._expected_perf = cache.dataframe_cache(
            dir_.getpath(
                'example_data/expected_perf/%s' %
                pd.__version__.replace('.', '-'),
            ),
            serialization='pickle',
        )

        self._framework_url = '[::]:50051'

    def tearDown(self) -> None:
        bundles.unregister('test')
        self._exit_stack.close()

    @parameterized.expand(sorted(examples.EXAMPLE_MODULES))
    def test_simulation(self, example_name):
        thread_pool = self._thread_pool
        framework_url = self._framework_url

        client = test.InMemoryTestClient(
            self._pluto_tempdir,
            framework_url,
            mode_utils.SimulationModeFactory(
                thread_pool,
                framework_url),
            loop_utils.SimpleSimulationLoopFactory(),
            thread_pool)

        _run(client, example_name, self._expected_perf)

    @parameterized.expand(sorted(examples.EXAMPLE_MODULES))
    def test_live_simulation(self, example_name):
        thread_pool = self._thread_pool
        framework_url = self._framework_url

        client = test.InMemoryTestClient(
            self._pluto_tempdir,
            framework_url,
            mode_utils.LiveSimulationModeFactory(
                framework_url,
                factory.LiveSimulationMarketFactory(
                    blotter_factory.MultiSimulationBlotterFactory()),
                thread_pool),
            loop_utils.SimpleSimulationLoopFactory(),
            thread_pool)

        _run(client, example_name, self._expected_perf)
