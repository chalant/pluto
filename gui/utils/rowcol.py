from PyQt6 import QtWidgets


class Rows(QtWidgets.QWidget):
    def __init__(self, parent):
        super(Rows, self).__init__(parent)

        self._layout = QtWidgets.QVBoxLayout(self)
        self.setLayout(self._layout)

    def addWidget(self, widget):
        self._layout.addWidget(widget)


class Columns(QtWidgets.QWidget):
    def __init__(self, parent):
        super(Columns, self).__init__(parent)

        self._layout = QtWidgets.QHBoxLayout(self)
        self.setLayout(self._layout)

    def addWidget(self, widget):
        self._layout.addWidget(widget)