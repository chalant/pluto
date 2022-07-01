from PyQt6 import QtWidgets


class Field(QtWidgets.QFrame):
    # a field is a form layout with a label and a widget
    def __init__(self, parent, name, widget):
        """

        Parameters
        ----------
        parent
        name
        widget: QtWidgets.QWidget
        """
        super(Field, self).__init__(parent=parent)

        layout = QtWidgets.QHBoxLayout(self)
        label = QtWidgets.QLabel(text=name, parent=self)

        layout.addWidget(label)
        layout.addWidget(widget)

        self._name = name
        self._widget = widget

        # self.setLayout(layout)

        widget.setParent(self)

    def show(self) -> None:
        print("FIELD!")
