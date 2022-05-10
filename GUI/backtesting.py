from PyQt6 import QtWidgets

from dev import backtester

from pluto import strategies

from GUI import ui_backtesting


# handles setup for the backtesting.
class Setup(object):
    def __init__(self, view, setup_model):
        """
        view:: GUI.ui_backtesting.Ui_Backtesting
        """
        self._view = view
        self_setup_model = setup_model


class BackTestingController(QtWidgets.QWidget):
    def __init__(self, model, backtester_):
        super(BackTestingController, self).__init__()

        self._model = model
        self._ui = ui = ui_backtesting.Ui_Backtesting()
        ui.setupUi(self)

        ui.load_button.clicked.connect(self._load)
        ui.deploy_button.clicked.connect(self._deploy)
        ui.backtest_button.clicked.connect(self._run)

        self._load_path = ""
        self._backtester = backtester_

    def _new(self):
        '''
        create a new strategy... this will create a file (based on template) and
        generate a session id ( a session groups a strategy with its parameters)
        generates a strategy id which is unique...

        once we deploy a strategy, we deploy it into a session where it will run
        with the specified parameters...
        a session also has an id that is a combination of all parameters using md5 hashing
        and concatenating the parameters strings.

        we can always overwrite the strategy parameters

        in development, we always run strategies by there names, and we
        can change the parameters as much as we want... once we've deployed a
        strategy it is "frozen" into a session. And in live mode, we run strategies
        by session.

        '''
        pass

    def _load(self):
        """
        display a file window to select a strategy

        """
        print("Running!")

    def _run(self):
        """
        run the algorithm with the specified parameters (universe, start_dt...)
        note: the strategy always run until the most recent available datapoint.
        """
        print("Running!")

    def _deploy(self):
        """
        deploys the currently running strategy into the local directory.
        the strategy is assigned a unique id (adds the strategy to the metadata file)
        the strategy gets attributed a unique id...
        there is also a mapping between the id and the universe.
        a strategy can run on multiple universes...

        """
        self._model.deploy(self._load_path)


