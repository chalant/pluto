import logging

from sqlalchemy import create_engine

import pandas as pd

from numpy import testing
import numpy as np

from pluto import examples

from pluto.coms.utils import conversions

from pluto.utils import stream

from pluto.control.processes import in_memory
from pluto.control.loop import simulation_loop

from pluto.strategies import strategies

from pluto.interface import monitor

from protos import controller_pb2


class TestWatcher(monitor.AbstractWatcher):
    def __init__(self, process_id, expected_df):
        super(TestWatcher, self).__init__(process_id)

        self._expected_df = expected_df[examples._cols_to_check]

        self._errors = 0
        self._tests = 0

        self._i = 0

        self._daily_perfs = []

    def performance_update(self, packet):
        # append until end...
        if not packet.end:
            daily_perfs = self._daily_perfs

            perf = controller_pb2.PerformancePacket()
            perf.ParseFromString(packet.packet)

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

        else:
            # compare values to expected values.

            daily_perfs = self._daily_perfs
            expected_perf = self._expected_df

            daily_dts = pd.DatetimeIndex(
                [p['period_close'] for p in daily_perfs], tz='UTC'
            )

            daily_stats = pd.DataFrame(daily_perfs, index=daily_dts)

            errors = 0
            tests = 0

            for l, r in zip(expected_perf.iteritems(), daily_stats.iteritems()):

                tests += 1

                des = l[1]
                act = r[1]

                try:
                    testing.assert_allclose(
                        np.nan_to_num(des.values),
                        act.values,
                        rtol=0.1,
                        atol=0.2,
                        equal_nan=False)

                except AssertionError as e:
                    errors += 1

                    print('Name: {}\nError: {}'.format(des, e))

                except TypeError:
                    try:
                        pd.testing.assert_series_equal(act, des, check_less_precise=3)
                    except AssertionError as e:
                        logging.warning('\nName: {}\nError: {}'.format(des, e))

                if self._errors > 0:
                    raise AssertionError('failed {} out of {}'.format(errors, tests))


class StrategyTester(object):
    def __init__(self, process):
        self._process = process

    def run(self, stp, universe, expected_perf):
        loop = simulation_loop.SimulationLoop(
            stp.start,
            stp.end,
            self._process,
            stp.data_frequency)

        loop.start(
            universe.exchanges,
            TestWatcher(
                self._process.process_id,
                expected_perf))


class TestClient(object):
    def __init__(self,
                 directory,
                 process):
        """

        Parameters
        ----------
        directory: directory.directory.Directory
        process: pluto.control.processes.processes.Process

        """

        self._strategy_dir = dir_ = directory.get_directory('strategies')
        self._strategy_db_engine = create_engine(
            'sqlite://' + dir_.get_file('metadata', 'sqlite').path)

        self._process = process

    def add_strategy(self, name, source_path):
        with self._strategy_db_engine.connect() as connection:
            strategies.create_strategy(
                connection,
                strategies.StrategyCreator(
                    self._strategy_dir.path,
                    source_path),
                name)

        return StrategyTester(self._process)
