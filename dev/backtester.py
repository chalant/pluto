import yaml

from directory import directory as drt
from directory import utils as du

from pluto.interface import monitor

from pluto.control.controllable import server
from pluto.control.loop import simulation_loop
from pluto.control.processes import in_memory
from pluto.control.processes import processes

from pluto.universes import universes

from pluto.setup import run_setup
from pluto.setup import directory_setup as drp


# def load_backtest_setup():
#     with open(drp.setup_dir.get_file("setup", "yaml").path) as f:
#         stp = yaml.load(f, Loader=yaml.FullLoader)


class BackTester(object):
    def __init__(self, controllable):
        # note end date is always the most recent data point

        self._stg_stp = None
        self._process = None

    def _setup(self, setup, controllable):
        pi = processes.ProcessInitializer(controllable)

        strategy_setup = pi.initialize(setup)

        r_stp = run_setup.load_run_setup()

        # todo: normally, we should use the strategy setup to match with
        #  with the available universes, if the requested setup does not exist,
        #  raise an error!

        # todo: create RunSetup message... (we use the setup file...)

        # todo: raise an error when the universe doesn't match the strategy setup requirements

        # todo: save the setups into the data-base, could be useful for deployment etc.

        uni = universes.get_universe(r_stp.trading_universe)

        return pi.start(r_stp), uni

    def run(self, setup, watcher, directory):
        # get calendar of the universe

        mon = monitor.Monitor()

        process, uni = self._setup(setup, mon, directory)

        # subscribe watcher to monitor...
        mon.watch(watcher)

        loop = simulation_loop.SimulationLoop(
            setup.start,
            setup.end,
            process,
            setup.frequency)

        loop.start(uni.exchanges)
