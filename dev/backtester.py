import yaml

from dev import events

from pluto.interface import monitor

from pluto.control.loop import simulation_loop
from pluto.control.processes import in_memory
from pluto.control.processes import processes

from pluto.strategies import strategies
from pluto.strategies import schema

from pluto.universes import universes

from pluto.setup import run_setup

events.add_event('backtesting.started')
events.add_event('backtesting.ended')


class BackTester(object):
    def __init__(self, controllable, directory):
        # note end date is always the most recent data point

        self._stg_stp = None
        self._process = None
        self._controllable = controllable

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

    def run(self, stp, strategy, loader, watcher):
        # get calendar of the universe

        pi = processes.ProcessInitializer(self._controllable)
        init_response = pi.initialize(stp, strategy.name)

        loader.load_strategy(strategy)

        # todo: if initial requirements aren't met, raise an error...

        stp.fractional = init_response.fractional

        uni = universes.get_universe(stp.trading_universe)

        process = pi.start(stp)

        loop = simulation_loop.SimulationLoop(
            stp.start,
            stp.end,
            stp.frequency)

        # subscribe process to clock events
        events.subscribe('clock.update', process.clock_update)

        # subscribe to monitor events
        process.watch(watcher)

        events.post_event('backtesting.started', None)

        # run loop on these specific exchanges
        loop.start(uni.exchanges)

