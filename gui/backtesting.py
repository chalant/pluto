from PyQt6 import QtWidgets, QtCore, QtGui

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
from pluto.sources import benchmark_source

from pluto.trading_calendars import calendar_utils as cu
from pluto.finance.blotter import simulation_blotter
from pluto.setup import engine_setup

from dev import strategy_store
from dev import events

from gui import ui_main_window
from gui import load_dialog
from gui import new_dialog
from gui import setup
from gui import controls
from gui import metric
from gui import qtlogger

events.add_event('backtesting.started')
events.add_event('backtesting.ended')


class Backtester(QtCore.QThread):
    started = QtCore.pyqtSignal()
    stopped = QtCore.pyqtSignal()

    def __init__(self, parent, script_name, dir_,
                 run_setup, metrics_view, universe):
        super(Backtester, self).__init__(parent)

        self._directory = dir_
        self._universe = universe
        self._run_setup = run_setup
        self._metrics_view = metrics_view
        self._script_name = script_name
        self._stopped = False

        events.subscribe('backtesting.stop', self.stop)

    def run(self) -> None:
        def start():
            try:
                self._stopped = False

                dir_ = self._directory
                stp = self._run_setup
                metrics_view = self._metrics_view

                metrics_view.reset()

                stg_store = ctp.StrategyStore(self._directory)

                # todo: create account class
                script_stp = ctp.Setup(
                    stg_store,
                    account.AccountSetup(
                        stp.capital,
                        stp.leverage))

                scripts_dir = dir_.get_directory('launchers')

                try:
                    handler = script_handler.load(
                        scripts_dir.get_file(self._script_name, 'py').path,
                        stp,
                        stg_store)
                    # run setup function of the script
                    handler.setup(script_stp)

                except FileNotFoundError or ValueError:
                    return

                self.started.emit()

                # todo: calendars are determined by strategy setup...
                #  (domain etc.)

                # todo: the universe contains all the available data
                #  strategies that request data not in the universe will crash

                uni = self._universe
                stp.environ = uni.environ

                self._process_manager = pm = process_manager.StrategyManager(uni.asset_finder, stp)

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

                    # todo: build trading domain of each strategy
                    #  this will be used to determine which clocks to instanciate

                    # todo: pass the capital in the process initialize method
                    # create strategy processes
                    for s, loader in stg_store.strategies:
                        controllable = in_memory.InMemoryControllableStub(
                            self._directory,
                            'backtesting',
                            pm,
                            running_behavior.NonBlocking())

                        pi = processes.ProcessInitializer(controllable)

                        # todo: need a better way for setting up the strategies

                        stg_stp = pi.initialize(
                            stp,
                            loader.strategy_name)

                        pm.add_bar_data(
                            stg_stp.data_frequency,
                            dp,
                            restrictions)

                        pm.add_strategy(pi.start(ctp.StrategySetup(s, stp)))

                    # add blotter for the base calendar...
                    pm.add_blotter(bc, simulation_blotter.SimulationBlotter(eng_stp))
                    pm.add_data_portal(bc, dp)
                    pm.add_ledger(bc)

                    # metrics_view.set_benchmark_source(
                    #     bc,
                    #     benchmark_source.SimulationBenchmarkSource(
                    #         calendar,
                    #         calendar.all_sessions,
                    #         uni.benchmark,
                    #         150))

                    loop.add_clock(bc)

                # todo: we need a plotting manager...
                # todo: we need a plotting setup...

                # todo: need emission rate from plotting control ?
                metrics_view.initialize(stp.start, stp.end)

                # todo: check if the stop button was clicked at each iteration

                for _ in loop.start(controller.Controller(pm, metrics_view)):
                    if self._stopped:
                        break

                self.stopped.emit()

            except Exception as e:
                print(e)
                return

        thread = threading.Thread(target=start)
        thread.start()

    def stop(self, data):
        self._stopped = True


class BackTesting(QtWidgets.QMainWindow):
    def __init__(self, directory, script_name):
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

        self._backtesting_control = bct = controls.BacktestingControl(ui.toolBar)

        ui.toolBar.addWidget(bct)

        bct.setEnabled(False)

        ui.actionNew.triggered.connect(self._new)
        ui.actionOpen.triggered.connect(self._load)

        # ui.deploy_button.clicked.connect(self._deploy)
        # ui.backtest_button.clicked.connect(self._run)

        self._template_path = tp = ""  # todo

        self._new_dialog = new_dialog.NewStrategy(store, tp)
        self._load_dialog = load_dialog.LoadStrategy(store)

        # setup view
        self._setup = stp = setup.Setup(ui.setup_dock)

        self._universe = None
        events.subscribe('universe.loaded', self._set_universe)

        self._bt_setup = setup.BacktestSetup(stp, 'backtest', directory)

        self._metrics_view = metric.BacktestingMetrics(ui.centralwidget)

        self._logger = qtlogger.Log(ui.console_output)

        ui.setup_dock.setWidget(stp)
        # console_layout = QtWidgets.QHBoxLayout()
        # ui.console_output.setLayout(console_layout)
        ui.console_output.setWidget(self._logger)

        stp.add_setup(self._bt_setup)
        # stp.add_setup(self._plotting_control)

        # stp.setEnabled(False)

        events.subscribe('strategy.loaded', self._load_strategy)
        events.subscribe('strategy.new', self._new_strategy)

        events.subscribe('backtesting.start', self._start_backtesting)
        events.subscribe('backtesting.stop', self._stop_backtesting)

        self._backtester = bt = Backtester(
            self,
            self._script_name,
            self._directory,
            self._bt_setup.run_setup,
            self._metrics_view,
            self._universe)

        bt.stopped.connect(self._backtesting_ended)
        bt.started.connect(self._backtesting_started)

    @property
    def logger(self):
        return self._logger

    def closeEvent(self, a0: QtGui.QCloseEvent) -> None:
        self._backtester.stop(None)

        a0.accept()

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

    def _set_universe(self, uni):
        self._universe = uni
        self._backtesting_control.setEnabled(True)

    def _start_backtesting(self, event_data):
        """
        run the algorithm with the specified parameters (universe, start_dt...)
        note: the strategy always run until the most recent available datapoint.
        """

        # ui.start()
        self._logger.reset()

        thread = self._backtester
        thread.run()

    def _stop_backtesting(self, data):
        self._backtester.stop(data)

    def _backtesting_started(self):
        ui = self._backtesting_control

        # self._metrics_view.reset()
        # self._plotting.reset()

        # ui.actionOpen.setEnabled(False)
        # ui.actionNew.setEnabled(False)

        ui.start()

    def _backtesting_ended(self):
        ui = self._backtesting_control

        # ui.actionOpen.setEnabled(True)
        # ui.actionNew.setEnabled(True)
        ui.stop()

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
