from PyQt6 import QtWidgets, QtCore
from PyQt6.QtCore import Qt


class DockList(QtWidgets.QWidget):
    def __init__(self, parent):
        super(DockList, self).__init__(parent)
        layout = QtWidgets.QHBoxLayout()

        self.setLayout(layout)

        self._splitter = QtWidgets.QSplitter(
            parent=self,
            orientation=Qt.Orientation.Vertical)

        layout.addWidget(self._splitter)

        self._plotting_range = (QtCore.QDateTime.currentDateTime().addYears(-1), QtCore.QDateTime.currentDateTime())

    def _add_component(self, component):
        dock = QtWidgets.QDockWidget(self)
        dock.setWidget(component)

        dock.setFeatures(dock.features().NoDockWidgetFeatures)

        self._splitter.addWidget(dock)

        dock.show()

        return dock

    def addWidget(self, component):
        return self._add_component(component)


class DockColumn(QtWidgets.QWidget):
    def __init__(self, parent):
        super(DockColumn, self).__init__(parent)

        layout = QtWidgets.QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.setLayout(layout)

        self._splitter = splitter = QtWidgets.QSplitter(
            parent=self,
            orientation=Qt.Orientation.Horizontal)

        layout.addWidget(splitter)

    def _add_component(self, component):
        dock = QtWidgets.QDockWidget(self)
        dock.setWidget(component)

        dock.setFeatures(dock.features().NoDockWidgetFeatures)

        self._splitter.addWidget(dock)

        dock.show()

        return dock

    def addWidget(self, component):
        self._add_component(component)


