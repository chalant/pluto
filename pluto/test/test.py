import logging

from sqlalchemy import create_engine

import pandas as pd

from numpy import testing
import numpy as np

from dev import strategy_store, events

from pluto import examples

from pluto.utils import stream

from pluto.control.loop import simulation_loop

from pluto.control.processes import processes
from pluto.control.processes import in_memory

from pluto.coms.utils import conversions

from pluto.strategies import strategies
from pluto.strategies import schema

from pluto.interface import monitor

from pluto.universes import universes

from protos import controller_pb2


class TestWatcher(monitor.AbstractWatcher):
    def __init__(self, process_id, expected_df, name, error_callback):
        super(TestWatcher, self).__init__(process_id)

        self._expected_df = expected_df[examples._cols_to_check]

        self._errors = 0
        self._tests = 0

        self._i = 0

        self._daily_perfs = []
        self._name = name

        self._error_callback = error_callback

    def performance_update(self, packet):

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
    def __init__(self, process, universe, name):
        self._process = process
        self._universe = universe
        self._name = name

    def run(self, stp, expected_perf):
        loop = simulation_loop.SimulationLoop(
            stp.start,
            stp.end,
            stp.data_frequency)

        def on_error(message):
            raise AssertionError(message)

        watcher = TestWatcher(
            self._process.process_id,
            expected_perf,
            self._name,
            on_error)

        self._process.watch(watcher)

        # subscribe to clock events
        events.subscribe('clock.update', self._process.clock_update)

        # make a watch request
        # self._process.watch()

        loop.start(self._universe.exchanges)


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

        uni = universes.get_universe(stp.trading_universe)

        stp.fractional = init_response.fractional

        return StrategyTester(pi.start(stp), uni, name)
