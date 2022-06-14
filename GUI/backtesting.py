from PyQt6 import QtWidgets

from dev import backtester
from dev import strategy_store
from dev import events

from GUI import ui_backtesting
from GUI import load_dialog
from GUI import new_dialog
from GUI import plotting
from GUI import setup


# handles setup for the backtesting.
class Setup(object):
    def __init__(self, view, setup_model):
        """
        view:: GUI.ui_backtesting.Ui_Backtesting
        """
        self._view = view
        self_setup_model = setup_model


class BackTesting(QtWidgets.QWidget):
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

        self._ui = ui = ui_backtesting.Ui_Backtesting()
        self._strategy_store = store = strategy_store.StrategyStore(directory)

        ui.setupUi(self)

        ui.load_button.clicked.connect(self._load)
        ui.new_button.clicked.connect(self._new)
        ui.deploy_button.clicked.connect(self._deploy)
        ui.backtest_button.clicked.connect(self._run)

        self._template_path = tp = ""  # todo

        self._new_dialog = new_dialog.NewStrategy(store, tp)
        self._load_dialog = load_dialog.LoadStrategy(store)

        # setup view
        self._setup = stp = setup.Setup(
            ui.settings_frame,
            'backtest')

        stp.setEnabled(False)

        events.subscribe('strategy.loaded', self._load_strategy)
        events.subscribe('strategy.new', self._new_strategy)

        events.subscribe('backtesting.started', self._backtesting_started)
        events.subscribe('backtesting.ended', self._backtesting_ended)

        self._loader = None
        self._watcher = None

    def _new(self):
        ui = self._ui

        ui.load_button.setEnabled(False)
        ui.new_button.setEnabled(False)

        self._new_dialog.show()

    def _load(self):
        """
        display a file window to select a strategy

        """
        ui = self._ui

        ui.load_button.setEnabled(False)
        ui.new_button.setEnabled(False)

        self._load_dialog.show()

    def _run(self):
        """
        run the algorithm with the specified parameters (universe, start_dt...)
        note: the strategy always run until the most recent available datapoint.
        """
        # todo: disable run button when the load dialog box
        #  is active

        stp = self._setup.run_setup

        if stp.data_frequency == 'minute':
            plotter = plotting.MinutePerformancePlotter()
        else:
            plotter = plotting.DailyPerformancePlotter()

        self._backtester.run(
            stp,
            self._strategy,
            self._strategy_store.load_strategy(self._strategy),
            plotter
        )

    def _backtesting_started(self):
        # todo: disable backtesting button
        ui = self._ui

        ui.load_button.setEnabled(False)
        ui.new_button.setEnabled(False)
        ui.backtest_button.setEnabled(False)

    def _backtesting_ended(self):
        # todo: enable backtesting button

        ui = self._ui

        ui.load_button.setEnabled(True)
        ui.new_button.setEnabled(True)
        ui.backtest_button.setEnabled(True)

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
        # todo: enable run button

        stp = self._setup

        ui = self._ui

        # get loader
        self._strategy = meta

        stp.load_setup(meta)

        ui.backtest_button.setEnabled(True)
        self._setup.setEnabled(True)

        ui.load_button.setEnabled(True)
        ui.new_button.setEnabled(True)

    def _new_strategy(self, meta):
        self._load_strategy(meta)

    def show(self) -> None:
        self._setup.show()
        super(BackTesting, self).show()


