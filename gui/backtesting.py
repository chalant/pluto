from PyQt6 import QtWidgets

import threading

from zipline.assets import Equity, Future
from zipline.data import data_portal
from zipline.finance import asset_restrictions

from pluto.control.loop import simulation_loop
from pluto.control.processes import in_memory
from pluto.control.processes import processes
from pluto.control.controller import controller
from pluto.control.controller import process_manager
from pluto.control.controllable import running_behavior
from pluto.control.setup import setup as ctp
from pluto.control.setup import account
from pluto.control import script_handler

from pluto.trading_calendars import calendar_utils as cu
from pluto.finance.blotter import simulation_blotter
from pluto.finance import ledger
from pluto.data.universes import universes
from pluto.setup import run_setup
from pluto.setup import engine_setup

from dev import strategy_store
from dev import events

from gui import ui_main_window
from gui import load_dialog
from gui import new_dialog
from gui.plotting import plotting, plotting_control
from gui import setup
from gui import controls

events.add_event('backtesting.started')
events.add_event('backtesting.ended')


class BackTesting(QtWidgets.QMainWindow):
    def __init__(
            self,
            directory,
            script_name):
        """

        Parameters
        ----------
        directory: directory.utils.DirectoryInterface
        """

        super(BackTesting, self).__init__()

        self._directory = directory
        self._script_name = script_name

        self._ui = ui = ui_main_window.Ui_MainWindow()
        self._strategy_store = store = strategy_store.StrategyStore(directory)

        ui.setupUi(self)

        self._backtesting_control = bct = controls.BacktestingControl(
            ui.toolBar)

        ui.toolBar.addWidget(bct)

        bct.setEnabled(False)

        ui.actionNew.triggered.connect(self._new)
        ui.actionOpen.triggered.connect(self._load)

        # ui.deploy_button.clicked.connect(self._deploy)
        # ui.backtest_button.clicked.connect(self._run)

        # self._layout = QtWidgets.QVBoxLayout()
        #
        # ui.centralwidget.setLayout(self._layout)

        self._plotting = plotting.PlottingHandler(
            ui.centralwidget,
            ui.verticalLayout_2)

        # self._layout.addWidget(self._plotting)

        self._template_path = tp = ""  # todo

        self._new_dialog = new_dialog.NewStrategy(store, tp)
        self._load_dialog = load_dialog.LoadStrategy(store)

        # setup view
        self._setup = stp = setup.Setup(
            ui.setup_dock
        )

        self._bt_setup = setup.BacktestSetup(
            stp,
            'backtest',
            directory)

        self._plotting_control = plotting_control.PlottingControl(stp)

        ui.setup_dock.setWidget(stp)

        stp.add_setup(self._bt_setup)
        stp.add_setup(self._plotting_control)

        stp.setEnabled(False)

        events.subscribe('strategy.loaded', self._load_strategy)
        events.subscribe('strategy.new', self._new_strategy)

        events.subscribe('backtesting.start', self._start_backtesting)
        events.subscribe('backtesting.stop', self._stop_backtesting)

        events.subscribe('backtesting.started', self._backtesting_started)
        events.subscribe('backtesting.ended', self._backtesting_ended)

        self._loader = None
        self._watcher = None

    def _new(self):
        ui = self._ui

        ui.actionOpen.setEnabled(False)
        ui.actionNew.setEnabled(False)

        self._new_dialog.show()

    def _load(self):
        """
        display a file window to select a strategy

        """
        ui = self._ui

        ui.actionOpen.setEnabled(False)
        ui.actionNew.setEnabled(False)

        self._load_dialog.show()

    def _start_backtesting(self, event_data):
        """
        run the algorithm with the specified parameters (universe, start_dt...)
        note: the strategy always run until the most recent available datapoint.
        """

        # todo: when

        def start():
            dir_ = self._directory
            stp = self._bt_setup.run_setup

            stg_store = ctp.StrategyStore(self._directory)

            # todo: create account class
            script_stp = ctp.Setup(
                stg_store,
                account.AccountSetup(
                    stp.capital
                )
            )

            scripts_dir = dir_.get_directory('launchers')

            handler = script_handler.load(scripts_dir.get_file(self._script_name))

            # run setup function of the script
            handler.setup(script_stp)

            uni = universes.get_universe(
                stp.trading_universe,
                self._directory)

            self._process_manager = pm = process_manager.StrategyManager(
                uni.asset_finder,
                stp.start,
                stp.capital,
                stp.emission_rate
            )

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

            loop = simulation_loop.SimulationLoop(
                stp.start,
                stp.end,
                stp.data_frequency)

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

                # todo: each strategy can be watched individually

                # create strategy processes
                for s, loader in stg_store.strategies:
                    controllable = in_memory.InMemoryControllableStub(
                        self._directory,
                        'backtesting',
                        pm,
                        running_behavior.NonBlocking())

                    pi = processes.ProcessInitializer(controllable)

                    stg_stp = pi.initialize(
                        stp,
                        loader.strategy_name)

                    pm.add_bar_data(
                        stg_stp.data_frequency,
                        dp,
                        restrictions)

                    process = pi.start(stp)
                    pm.add_process(process)

                # add blotter for the base calendar...
                pm.add_blotter(
                    bc,
                    simulation_blotter.SimulationBlotter(
                        eng_stp
                    )
                )

                pm.add_data_portal(bc, dp)

                loop.add_clock(bc)

            # todo: we need a plotting manager...
            # todo: we need a plotting setup...

            # todo: need emission rate from plotting control ?

            self._plotting.set_plotting_setup(
                data_frequency=stp.data_frequency,
                plotting_range=(stp.start, stp.end),
                emission_rate='daily'
            )

            ctl = controller.Controller(pm)

            # subscribe process to clock events
            # events.subscribe('clock.update', process.clock_update)

            # todo: watch the controller instead (implement watch method)

            # subscribe to monitor events
            ctl.watch(self._plotting)

            events.post_event('backtesting.started', None)
            # run loop on these specific exchanges

            loop.start(ctl)

            # self._backtester.run(
            #     stp,
            #     self._strategy,
            #     self._strategy_store.load_strategy(self._strategy),
            #     self._plotting
            # )

        thread = threading.Thread(target=start)
        # todo: disable run button when the load dialog box
        #  is active

        thread.start()

    def _stop_backtesting(self, data):
        pass

    def _backtesting_started(self, data):
        ui = self._ui

        self._plotting.reset()

        ui.actionOpen.setEnabled(False)
        ui.actionNew.setEnabled(False)

    def _backtesting_ended(self, data):
        ui = self._ui

        ui.actionOpen.setEnabled(True)
        ui.actionNew.setEnabled(True)

    def _deploy(self):
        """
        deploys the currently running strategy into the local directory.
        the strategy is assigned a unique id (adds the strategy to the metadata file)
        the strategy gets attributed a unique id...
        there is also a mapping between the id and the universe.
        a strategy can run on multiple universes...

        """
        self._deployer.deploy(self._load_path)

    def _load_strategy(self, meta):
        # todo: load parameters associated with the
        #  strategy

        # todo: set up plotting axes...

        stp = self._bt_setup

        ui = self._ui

        # get loader
        self._strategy = meta

        stp.load_setup(meta)

        self._setup.setEnabled(True)

        ui.actionOpen.setEnabled(True)
        ui.actionNew.setEnabled(True)

        self._backtesting_control.setEnabled(True)

    def _new_strategy(self, meta):
        self._load_strategy(meta)



