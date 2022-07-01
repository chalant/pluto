from PyQt6 import QtWidgets

from gui import ui_backtesting_controls
from dev import events

events.add_event('backtesting.start')
events.add_event('backtesting.pause')
events.add_event('backtesting.stop')


class BacktestingControl(QtWidgets.QWidget):
    def __init__(self, container):
        super(BacktestingControl, self).__init__(container)

        self._ui = ui = ui_backtesting_controls.Ui_backtesting_controls()
        ui.setupUi(self)

        ui.backtesting_button.clicked.connect(self._start)
        ui.backtesting_stop_button.clicked.connect(self._stop)

        ui.backtesting_stop_button.setEnabled(False)

        # events.subscribe('strategy.loaded')
        # events.subscribe('strategy.new')

        events.subscribe('backtesting.started', self._started)
        events.subscribe('backtesting.ended', self._ended)

    def _start(self):
        events.post_event('backtesting.start', None)

    def _stop(self):
        events.post_event('backtesting.stop', None)

    def _started(self, data):
        ui = self._ui

        ui.backtesting_button.setEnabled(False)
        ui.backtesting_stop_button.setEnabled(True)

    def _ended(self, data):
        ui = self._ui

        ui.backtesting_button.setEnabled(True)
        ui.backtesting_stop_button.setEnabled(False)
