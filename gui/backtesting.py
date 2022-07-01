from PyQt6 import QtWidgets

from dev import backtester
from dev import strategy_store
from dev import events

from gui import ui_main_window
from gui import load_dialog
from gui import new_dialog
from gui.plotting import plotting, plotting_control
from gui import setup
from gui import controls


class BackTesting(QtWidgets.QMainWindow):
    def __init__(
            self,
            directory,
            controllable):
        """

        Parameters
        ----------
        directory: directory.utils.DirectoryInterface
        controllable:
        """

        super(BackTesting, self).__init__()

        self._backtester = backtester.BackTester(
            controllable,
            directory)

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

    def _start_backtesting(self, data):
        """
        run the algorithm with the specified parameters (universe, start_dt...)
        note: the strategy always run until the most recent available datapoint.
        """

        # todo: disable run button when the load dialog box
        #  is active

        stp = self._bt_setup.run_setup

        # todo: we need a plotting manager...
        # todo: we need a plotting setup...


        # todo: need emission rate from plotting control ?

        self._plotting.set_plotting_setup(
            data_frequency=stp.data_frequency,
            plotting_range=(stp.start, stp.end),
            emission_rate='daily'
        )

        self._backtester.run(
            stp,
            self._strategy,
            self._strategy_store.load_strategy(self._strategy),
            self._plotting
        )

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



