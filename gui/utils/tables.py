import operator

from PyQt6 import QtWidgets


class Item(object):
    def __init__(self, row, column):
        self.row = row
        self.column = column

    def value(self, obj):
        pass


class Column(object):
    def __init__(self, name, field):
        self._name = name
        self._field = operator.attrgetter(field)

    @property
    def name(self):
        return self._name

    def value(self, source):
        return str(self._field(source))


class Table(QtWidgets.QWidget):
    def __init__(self):
        super(Table, self).__init__()

        self._table = table = QtWidgets.QTableWidget(self)

        table.setEditTriggers(table.EditTrigger.NoEditTriggers)

        self._columns = []

    def set_columns(self, columns):
        self._columns = columns

        table = self._table

        table.setHorizontalHeaderLabels((col.name for col in self._columns))
        table.setColumnCount(len(self._columns))

    def add_column(self, column):
        table = self._table
        self._columns.append(column)

        table.setHorizontalHeaderLabels((col.name for col in self._columns))
        table.setColumnCount(len(self._columns))

    def set_item(self, row, item):
        table = self._table

        for j, col in enumerate(self._columns):
            table.setItem(
                row,
                j,
                QtWidgets.QTableWidgetItem(col.value(item)))

    def clear(self):
        self._table.clearContents()
