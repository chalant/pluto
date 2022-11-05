from PyQt6 import QtWidgets, QtCore
from PyQt6.QtCore import Qt

from dev import events

events.add_event('plotter.closed')
events.add_event('plotter.add')

# todo: we the list of fields (note: by default, we plot the returns and the benchmark returns)
# todo: we need to be able to add plotters... (combo box?)


class SplitterView(QtWidgets.QWidget):
    def __init__(self, parent):
        super(SplitterView, self).__init__(parent)

        self._data_handlers = {}

        layout = QtWidgets.QHBoxLayout()

        self.setLayout(layout)

        self._splitter = QtWidgets.QSplitter(
            parent=self,
            orientation=Qt.Orientation.Vertical)

        layout.addWidget(self._splitter)

        self._plotting_range = (QtCore.QDateTime.currentDateTime().addYears(-1), QtCore.QDateTime.currentDateTime())

    def reset(self):
        for plotter in self._data_handlers.values():
            plotter.reset()

    def start_of_session(self, dt, ledger):
        dt2 = QtCore.QDateTime(dt.to_pydatetime()).toMSecsSinceEpoch()

        for handler in self._data_handlers.values():
            handler.end_of_session(dt2, ledger)

    def end_of_bar(self, dt, ledger):
        dt2 = QtCore.QDateTime(dt.to_pydatetime()).toMSecsSinceEpoch()

        for handler in self._data_handlers.values():
            handler.end_of_bar(dt2, ledger)

    def end_of_session(self, dt, ledger):
        dt2 = QtCore.QDateTime(dt.to_pydatetime()).toMSecsSinceEpoch()

        for handler in self._data_handlers.values():
            handler.end_of_session(dt2, ledger)

    def _add_component(self, name, component):
        dock = QtWidgets.QDockWidget(self)
        dock.setWidget(component)

        dock.setWindowTitle(name)

        dock.setFeatures(dock.features().NoDockWidgetFeatures)

        self._data_handlers[name] = component

        self._splitter.addWidget(dock)

        dock.show()

    def add_component(self, name, component):
        self._add_component(name, component)
