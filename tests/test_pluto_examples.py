import unittest
import contextlib2 as ctx
from os import path, environ
import tarfile

from nose_parameterized import parameterized
import pandas as pd
from pandas import testing

from zipline.testing import test_resource_path, core
from zipline.data import bundles
from zipline.utils import cache
from zipline.testing import predicates

from pluto.interface import directory
from pluto import examples
from pluto.test import test
from pluto.control.modes.processes import in_memory


class PlutoExamplesTests(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        cls._exit_stack = stack = ctx.ExitStack()
        cls._pluto_tempdir = stack.enter_context(
            directory.get_directory('test'))

        bundles.register('test', lambda *args: None)

        cls._zpl_tempdir = dir_ = stack.enter_context(
            core.tmp_dir(path=path.expanduser('~/tmp/zipline')))

        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(dir_.path)

        environ.setdefault('ZIPLINE_ROOT', dir_.getpath('example_data/root'))

        cls._expected_perf = cache.dataframe_cache(
            dir_.getpath(
                'example_data/expected_perf/%s' %
                pd.__version__.replace('.', '-'),
            ),
            serialization='pickle',
        )

    @classmethod
    def tearDown(cls) -> None:
        bundles.unregister('test')
        cls._exit_stack.close()

    @parameterized.expand(sorted(examples.EXAMPLE_MODULES))
    def test_simulation(self, example_name):
        client = test.TestClient(
            in_memory.InMemoryProcessFactory(),
            self._pluto_tempdir)

        response = client.add_strategy(example_name)
        args, path_ = examples.EXAMPLE_MODULES[example_name]

        with open(path_, 'r') as f:
            #save the strategy as byte in the file system
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

        daily_perfs = []
        for perf in client.watch(session_id):
            daily_perf = perf.get('daily_perf', None)
            daily_perf.update(perf['cumulative_risk_metrics'])
            daily_perf.update(perf['cumulative_perf'])
            daily_perfs.append(daily_perf)

        daily_dts = pd.DatetimeIndex(
            [p['period_close'] for p in daily_perfs], tz='UTC'
        )

        daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)
        expected_perf = self._expected_perf[example_name]

        a = daily_stats[examples._cols_to_check]
        b = expected_perf[examples._cols_to_check]

        # print(a.iloc[:, 2])
        # print(b.iloc[:, 2])

        testing.assert_frame_equal(
            a,
            b,
            check_dtype=False,
            check_less_precise=1)