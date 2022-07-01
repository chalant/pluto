from PyQt6 import QtWidgets


class WidgetList(QtWidgets.QWidget):
    '''
    Class for holding a horizontal layout of widgets...
    '''
    def __init__(self, parent):
        super(WidgetList, self).__init__(parent)

        layout = QtWidgets.QHBoxLayout(self)

        self._layout = layout

    def add_widget(self, widget):
        self._layout.addWidget(widget)
