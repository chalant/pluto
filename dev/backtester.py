import yaml

from zipline.assets import Equity, Future
from zipline.data import data_portal
from zipline.finance import asset_restrictions

from dev import events

from pluto.interface import monitor

from pluto.control.loop import simulation_loop
from pluto.control.processes import in_memory
from pluto.control.processes import processes
from pluto.control.controller import controller
from pluto.control.controller import process_manager
from pluto.trading_calendars import calendar_utils as cu
from pluto.finance.blotter import simulation_blotter
from pluto.finance import ledger

from pluto.strategies import strategies
from pluto.strategies import schema

from pluto.data.universes import core

from pluto.setup import run_setup
from pluto.setup import engine_setup

events.add_event('backtesting.started')
events.add_event('backtesting.ended')


class BackTester(object):
    def __init__(
            self,
            controllable,
            directory,
            process_manager_):

        # note end date is always the most recent data point

        self._stg_stp = None
        self._process = None
        self._controllable = controllable
        self._directory = directory

        self._process_manager = process_manager_

    def _setup(self, setup, controllable):
        pi = processes.ProcessInitializer(controllable)

        strategy_setup = pi.initialize(setup,)

        r_stp = run_setup.load_run_setup()

        # todo: normally, we should use the strategy setup to match with
        #  with the available universes, if the requested setup does not exist,
        #  raise an error!

        # todo: create RunSetup message... (we use the setup file...)

        # todo: raise an error when the universe doesn't match the strategy setup requirements

        # todo: save the setups into the data-base, could be useful for deployment etc.

        uni = universes.get_universe(
            r_stp.trading_universe,
            self._directory)

        return pi.start(r_stp), uni

    def run(self, stp, strategy, loader, watcher):
        # get calendar of the universe

        # events.post_event('backtesting.started', None)Z
        self._run(stp, watcher, strategy)

    def _run(self, stp, watcher, strategy):
        uni = universes.get_universe(
            stp.trading_universe,
            self._directory)

        controllable = in_memory.InMemoryControllableStub(
            self._directory,
            'backtesting',
            pm,
            running_behavior.Blocking())

        pi = processes.ProcessInitializer(controllable)
        stg_stp = pi.initialize(stp, strategy.strategy_name)

        restrictions = asset_restrictions.NoRestrictions()

        # todo we need the directory

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

        for bc in uni.base_calendars:
            # todo: create bar data using setup and add it...
            calendar = cu.get_calendar_in_range(
                bc,
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

            pm.add_bar_data(
                stg_stp.data_frequency,
                dp,
                restrictions)

            # add blotter for the base calendar...

            pm.add_blotter(
                bc,
                simulation_blotter.SimulationBlotter(
                    eng_stp
                )
            )

            pm.add_data_portal(bc, dp)

        ctl = controller.Controller(pm)

        process = pi.start(stp)

        loop = simulation_loop.SimulationLoop(
            stp.start,
            stp.end,
            stp.data_frequency)

        # subscribe process to clock events
        # events.subscribe('clock.update', process.clock_update)

        # subscribe to monitor events
        process.watch(watcher)

        events.post_event('backtesting.started', None)

        # run loop on these specific exchanges
        loop.start(ctl)

