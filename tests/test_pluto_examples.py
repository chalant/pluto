import unittest
import contextlib2 as ctx
from os import path
import tarfile

from parametrized import parametrized
import pandas as pd

from zipline.testing import test_resource_path, core
from zipline.data import bundles
from zipline.utils import cache
from zipline.testing import predicates

from pluto.interface import directory
from pluto import examples, test
from pluto.control.modes.processes import in_memory

from protos import interface_pb2_grpc as itf


class PlutoExamplesTests(unittest.TestCase):
    def setUp(self) -> None:
        self._exit_stack = stack = ctx.ExitStack()
        self._pluto_tempdir = stack.enter_context(
            directory.get_directory('test'))

        bundles.register('test', lambda *args: None)

        self._zpl_tempdir = dir_ = stack.enter_context(
            core.tmp_dir(path.expanduser('~/tmp/zipline')))

        self._expected_perf = cache.dataframe_cache(
            dir_.getpath(
                'example_data/expected_perf/%s' %
                pd.__version__.replace('.', '-'),
            ),
            serialization='pickle',
        )

        #todo: we need a monitor implementation

        self._client = test.test.TestClient(
            in_memory.InMemoryProcessFactory(itf.MonitorServicer()),
            self._pluto_tempdir)

        with tarfile.open(test_resource_path('example_data.tar.gz')) as tar:
            tar.extractall(dir_.path)

    def tearDown(self) -> None:
        bundles.unregister('test')
        self._exit_stack.close()

    @parametrized(sorted(examples.EXAMPLE_MODULES))
    def test_simulation(self, example_name):
        client = self._client
        response = client.add_strategy(example_name)
        args, path_ = examples.EXAMPLE_MODULES[example_name]

        with open(path_, 'r') as f:
            #save the strategy as byte in the file system
            client.save(response.strategy_id, f.read().encode('utf-8'))

        capital = 1e7
        max_leverage = args['max_leverage']
        session_id = client.setup(
            response.strategy_id,
            args['start'],
            args['end'],
            capital,
            max_leverage,
            args['look_back'])

        ratio = capital / args['capital_base']

        client.run(session_id, ratio, max_leverage)

        daily_perfs = []
        for perf in client.watch(session_id):
            daily_perf = perf.get('daily_perf', None)
            daily_perf.update(
                daily_perf.pop('recorded_vars')
            )
            daily_perf.update(
                perf['cumulative_risk_metrics'])
            daily_perfs.append(daily_perf)

        daily_dts = pd.DatetimeIndex(
            [p['period_close'] for p in daily_perfs], tz='UTC'
        )

        daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)
        expected_perf = self._expected_perf[example_name]
        predicates.assert_equal(
            daily_stats[examples._cols_to_check],
            expected_perf[examples._cols_to_check],
            check_dtype=False,
        )