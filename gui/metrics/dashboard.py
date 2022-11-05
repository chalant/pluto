import operator as op

from PyQt6 import QtWidgets, QtCore, QtCharts, QtGui
from PyQt6.QtCore import Qt

from gui.utils import charts, timeseries, docks


class Positions(QtWidgets.QWidget):
    def __init__(self, container):
        super(Positions, self).__init__(container)

        self._layout = QtWidgets.QVBoxLayout()
        self._table = table = QtWidgets.QTableWidget()

        self._col_map = [
            op.attrgetter('asset.symbol'),
            op.attrgetter('asset.__class__.__name__'),
            op.attrgetter('amount'),
            op.attrgetter('cost_basis'),
            op.attrgetter('last_sale_price')
        ]

        self.setLayout(self._layout)

        self._layout.addWidget(table)
        table.setColumnCount(len(self._col_map))

        # todo: unrealized: potential gain or loss if we sold

        table.setHorizontalHeaderLabels((
            'Symbol',
            'Asset',
            'Shares',
            'Avg Cost',
            'Price'
        ))

        table.setRowCount(5)
        table.setEditTriggers(table.editTriggers().NoEditTriggers)
        table.setFocusPolicy(table.focusPolicy().NoFocus)
        table.setSelectionMode(table.selectionMode().NoSelection)
        # cache items to avoid  creating new objects
        # self._items = [QtWidgets.QTableWidgetItem('')] * len(self._col_map)
        self._items = {}

    def start_of_session(self, dt, calendar_name, ledger):
        pass

    def end_of_bar(self, portfolio):
        self.end_of_session(portfolio)

    def end_of_session(self, positions):
        row = 0

        table = self._table

        # table.clearContents()

        positions = positions.values()

        table.setRowCount(len(positions))

        for position in positions:
            for j, getter in enumerate(self._col_map):
                it = table.item(row, j)

                if not it:
                    it = QtWidgets.QTableWidgetItem()
                    table.setItem(row, j, it)

                it.setText(str(getter(position)))

            row += 1
        pass


class OrdersAndFills(QtWidgets.QWidget):
    def __init__(self, parent):
        super(OrdersAndFills, self).__init__(parent)

        self._col_map = [
            op.attrgetter('asset.symbol'),
            op.attrgetter('asset.__class__.__name__'),
            op.attrgetter('amount'),
            op.attrgetter('cost_basis'),
            op.attrgetter('last_sale_price')
        ]

        self._layout = layout = QtWidgets.QVBoxLayout()

        self.setLayout(layout)

        self._table = table = QtWidgets.QTableWidget(self)
        layout.addWidget(table)

        table.setColumnCount(len(self._col_map))
        table.setHorizontalHeaderLabels(('Date', 'Symbol', 'Type', 'Shares', 'Limit', 'Stop', 'Status'))

    def end_of_bar(self, orders):
        pass


class DashBoard(QtWidgets.QWidget):
    def __init__(self, parent):
        super(DashBoard, self).__init__(parent)

        # self.setContentsMargins(0, 0, 0, 0)
        self._area = area = QtWidgets.QScrollArea(self)

        self._layout = layout = QtWidgets.QVBoxLayout()
        self.setLayout(layout)

        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self._columns = cols = docks.DockColumn(area)

        layout.addWidget(area)

        self._series = series = docks.DockList(cols)
        self._tables = tables = docks.DockList(cols)

        area.setWidget(cols)
        area.setContentsMargins(0, 0, 0, 0)
        area.setLineWidth(0)

        cols.addWidget(series)
        cols.addWidget(tables)
        cols.setContentsMargins(0, 0, 0, 0)

        # timeseries chart for displaying performance
        self.performance = ts = charts.TimeSeriesChart()
        self.leverage = leverage = charts.TimeSeriesChart()
        self.exposure = exposure = charts.TimeSeriesChart()

        ts.set_plotting_range(-1, 1)
        ts.set_plotting_period(
            QtCore.QDateTime.currentDateTime().addYears(-1),
            QtCore.QDateTime.currentDateTime())

        leverage.set_plotting_range(0, 1)
        leverage.set_plotting_period(
            QtCore.QDateTime.currentDateTime().addYears(-1),
            QtCore.QDateTime.currentDateTime())

        exposure.set_plotting_range(-1, 1)
        exposure.set_plotting_period(
            QtCore.QDateTime.currentDateTime().addYears(-1),
            QtCore.QDateTime.currentDateTime())

        # area.setWidget(series)
        series.addWidget(QtCharts.QChartView(ts))
        series.addWidget(QtCharts.QChartView(leverage))
        series.addWidget(QtCharts.QChartView(exposure))

        series.setContentsMargins(0, 0, 0, 0)

        area.setWidgetResizable(True)

        ts.setMinimumHeight(300)
        leverage.setMinimumHeight(300)
        exposure.setMinimumHeight(300)

        self.cumulative_returns = timeseries.LineTimeSeries(ts, 'Cumulative Returns')
        self.leverage_line = timeseries.LineTimeSeries(leverage, 'Leverage')
        self.long_exposure = timeseries.LineTimeSeries(exposure, 'Exposure')
        self.cash = timeseries.LineTimeSeries(exposure, 'Cash')
        # self.long_exposure.setColor(QtGui.QColor('red'))
        self.returns = timeseries.LineTimeSeries(ts, 'Returns')

        self.positions = pos = Positions(tables)
        self.orders_and_fills = of = OrdersAndFills(tables)

        tables.addWidget(pos)
        tables.addWidget(of)

    def reset(self):
        self.cumulative_returns.clear()
        self.leverage_line.clear()
        self.long_exposure.clear()
        self.cash.clear()
        self.returns.clear()

        self.performance.set_plotting_range(-1, 1)
        self.leverage.set_plotting_range(0, 0.1)
        self.exposure.set_plotting_range(0, 10000)

