from PyQt6 import QtWidgets, QtGui, QtCore

from dev import events

from gui import ui_plotting_control

_plotters = [
    'Performance',
    'Leverage',
    'Alpha',
    'Beta'
]

_index = {
    'Performance': 0,
    'Leverage': 1,
    'Alpha': 2,
    'Beta': 3
}


# todo: emission rate option...
#  note this should trigger down sampling if we from minute to daily.

class PlottingControl(QtWidgets.QWidget):
    def __init__(self, container):
        super(PlottingControl, self).__init__(container)

        self._ui = ui = ui_plotting_control.Ui_plotting_control()
        ui.setupUi(self)

        self._not_displayed = _plotters.copy()
        self._selected = 0

        ui.add_plotter.clicked.connect(self._display_menu)

        events.subscribe('plotter.closed', self._plotter_closed)

        # set default chart to performance.
        self._inner_plotter_selected("Performance")

    def _display_menu(self):

        menu = QtWidgets.QMenu(self)

        for p in self._not_displayed:
            if p:
                menu.addAction(p)

        menu.triggered.connect(self._plotter_selected)

        add_plotter = self._ui.add_plotter

        menu.setFixedWidth(add_plotter.width())

        menu.exec(self.mapToGlobal(add_plotter.pos() + QtCore.QPoint(0, add_plotter.height())))

    def _inner_plotter_selected(self, name):
        events.post_event('plotter.add', name)

        self._not_displayed[_index[name]] = ''

        self._selected += 1

        if self._selected == len(_plotters):
            self._ui.add_plotter.setEnabled(False)

    def _plotter_selected(self, a0: QtGui.QAction):
        self._inner_plotter_selected(a0.text())

    def _plotter_closed(self, name):
        # put back plotter in the slot...
        self._not_displayed[_index[name]] = name

        self._ui.add_plotter.setEnabled(True)






