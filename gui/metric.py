from collections import defaultdict, OrderedDict
import operator as op

import empyrical as ep

import numpy as np

from PyQt6 import QtCore
from PyQt6 import QtWidgets

from gui.utils import timeseries, charts
from gui.metrics import dashboard


class BenchmarkReturnsAndVolatility(object):
    def __init__(self, benchmark_source):
        benchmark_returns = benchmark_source.daily_returns_series

        self._benchmark_period_return = (np.cumprod(1 + benchmark_returns.values) - 1)
        self._benchmark_volatility = (benchmark_returns.expanding(2).std(ddof=1) * np.sqrt(252)).values
        self._benchmark_returns = benchmark_returns.values

        self._session_index = 0
        self._minute_index = 0

    def end_of_session(self):
        self._session_index += 1

    @property
    def returns(self):
        return self._benchmark_returns[self._session_index]

    @property
    def annual_volatility(self):
        return self._benchmark_volatility[self._session_index]

    @property
    def cumulative_returns(self):
        return self._benchmark_period_return[self._session_index]


class UpdateHelper(object):
    def __init__(self, series, benchmark_source):
        self._series = series

        self._benchmark_source = benchmark_source
        self._session_idx = 0

    def start_of_session(self, dt):
        pass

    def end_of_bar(self, dt):
        self._series.append(dt, self._benchmark_source.get_value(dt))

    def end_of_session(self, dt):
        self._series.append(dt, self._benchmark_source.get_value(dt))


class PortfolioCash(QtWidgets.QWidget):
    def __init__(self, container):
        super(PortfolioCash, self).__init__(container)

        self._table = table = QtWidgets.QTableWidget(self)

        self._col_map = [
            op.attrgetter('cash'),
        ]

        table.setColumnCount(len(self._col_map))

        table.setHorizontalHeaderLabels(('Cash',))

        # cache items to avoid  creating new objects
        self._items = [QtWidgets.QTableWidgetItem('')] * len(self._col_map)

    def start_of_session(self, dt, calendar_name, ledger):
        pass

    def end_of_bar(self, portfolio):
        self.end_of_session(portfolio)

    def end_of_session(self, portfolio):
        row = 0

        table = self._table

        for j, getter in enumerate(self._col_map):
            item = self._items[j]
            item.setText(str(getter(portfolio)))

            table.setItem(row, j, item)


class Field(QtWidgets.QWidget):
    def __init__(self, name, value, parent):
        super(Field, self).__init__(parent)

        self._layout = layout = QtWidgets.QVBoxLayout(self)

        self._label = label = QtWidgets.QLabel(name, self)
        self._value = val = QtWidgets.QLabel(value, self)

        layout.addWidget(label)
        layout.addWidget(val)

    def set_value(self, val):
        self._value.setText(val)


class OrdersAndFills(object):
    pass


class OverallStatistics(QtWidgets.QWidget):
    def __init__(self, container):
        super(OverallStatistics, self).__init__(container)

        self._layout = layout = QtWidgets.QHBoxLayout(self)

        self._cols = (
            Field('Returns', '0.00', self),
            Field('Available Cash', '0.00', self),
            Field('Volatility', '0.00', self),
            Field('Sharpe', '0.00', self),
            Field('Sortino', '0.00', self),
            Field('Max Drawdown', '0.00', self))

        for col in self._cols:
            layout.addWidget(col)

        self._functions = [
            ep.annual_volatility,
            ep.sharpe_ratio,
            ep.sortino_ratio,
            ep.max_drawdown
        ]

        # table.setColumnCount(len(self._cols))
        # table.setHorizontalHeaderLabels(self._cols)
        # table.setRowCount(1)
        # table.verticalHeader().setStretchLastSection(False)

        self._previous_total_returns = 0.0

        self._items = [QtWidgets.QTableWidgetItem('')] * len(self._cols)

    def end_of_session(self, returns):
        for item, fn in zip(self._cols, self._functions):
            item.set_value(str(fn(returns)))


class Statistics(QtWidgets.QWidget):
    def __init__(self, container):
        super(Statistics, self).__init__(container)

        self._layout = layout = QtWidgets.QVBoxLayout()
        self._table = table = QtWidgets.QTableWidget(self)

        layout.addWidget(table)

        self._calendar_row = {}
        self._row = 0

        self._cols = (
            'Calendar',
            'Alpha',
            'Beta',
            'Volatility',
            'Sharpe',
            'Sortino',
            'Max Drawdown')

        table.setColumnCount(len(self._cols))
        table.setHorizontalHeaderLabels(
            self._cols
        )

        self._items = [QtWidgets.QTableWidgetItem('')] * len(self._cols)

    def end_of_session(self, calendar_name, returns, benchmark_returns):
        table = self._table
        items = self._items

        items[0].setText(calendar_name)

        items[1].setText(str(ep.alpha_aligned(returns, benchmark_returns)))
        items[2].setText(str(ep.beta_aligned(returns, benchmark_returns)))

        items[3].setText(str(ep.annual_volatility(returns)))
        items[4].setText(str(ep.sharpe_ratio(returns)))
        items[5].setText(str(ep.sortino_ratio(returns)))
        items[6].setText(str(ep.max_drawdown(returns)))

        row = self._calendar_row[calendar_name]

        for col, item in enumerate(self._items):
            table.setItem(row, col, item)

    def add_calendar_name(self, name):
        self._calendar_row[name] = self._row
        self._row += 1

        self._table.setRowCount(self._row)


class BacktestingMetrics(object):
    def __init__(self, container):
        self._tabs = tabs = QtWidgets.QTabWidget(container)
        tabs.setContentsMargins(0, 0, 0, 0)
        # tabs.setTabPosition(tabs.tabPosition().West)

        layout = QtWidgets.QVBoxLayout()
        overall = OverallStatistics(container)
        layout.addWidget(overall)
        container.setLayout(layout)

        # stats.setMaximumHeight(65)

        layout.addWidget(tabs)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self._dashboard = dash = dashboard.DashBoard(tabs)
        self._overall = overall

        # tabs.resize(300, 200)

        # elements on the dashboard get updated each end of bar.
        tabs.addTab(dash, 'Dashboard')
        tabs.addTab(Statistics(tabs), 'Stats')

        dash.setContentsMargins(0, 0, 0, 0)

        self._returns_per_calendar = defaultdict(list)

        self._benchmark_sources = {}
        self._benchmark_series = {}

        self._stats_meta = OrderedDict()

        self._row = 0

        self._previous_total_returns = 0.0

        self._current_dt = None

    def initialize(self, start_dt, end_dt):

        t1 = QtCore.QDateTime(start_dt.to_pydatetime())
        t2 = QtCore.QDateTime(end_dt.to_pydatetime())

        self._current_dt = start_dt

        self._dashboard.performance.set_plotting_period(t1, t2)
        self._dashboard.leverage.set_plotting_period(t1, t2)
        self._dashboard.exposure.set_plotting_period(t1, t2)

    def start_of_session(self, calendar_name, dt, ledger):
        # self._previous_total_returns = ledger.portfolio.returns
        pass

    def end_of_bar(self, dt, calendar_name, ledger):
        # todo: update positions, order, fills, etc.
        dash = self._dashboard
        # benchmark_source = self._benchmark_sources[calendar_name]
        #
        # benchmark_series = self._benchmark_series[calendar_name]

        portfolio = ledger.portfolio
        account = ledger.account

        qdt = QtCore.QDateTime(dt.to_pydatetime()).toMSecsSinceEpoch()

        returns = dash.returns
        cumulative_returns = dash.cumulative_returns
        leverage = dash.leverage_line
        exposure = dash.long_exposure
        cash = dash.cash

        dash.positions.end_of_bar(portfolio.positions)

        # todo: the order history shouldn't entirely be saved in the ledger
        dash.orders_and_fills.end_of_bar(ledger.orders(dt))

        # append only when the datetime changes.
        if dt > self._current_dt:
            self._current_dt = dt

            # if calendar_name in benchmark_source:
            #     bs = benchmark_source.returns
            #     benchmark_series.append(qdt, bs)

            r = ((portfolio.returns + 1) / (self._previous_total_returns + 1) - 1)

            # print(r, portfolio.returns, self._previous_total_returns)

            self._previous_total_returns = portfolio.returns

            cumulative_returns.append(qdt, portfolio.returns)
            returns.append(qdt, r)
            leverage.append(qdt, account.leverage)
            exposure.append(qdt, portfolio.positions_exposure)
            cash.append(qdt, portfolio.cash)

        else:
            cumulative_returns.replace_point(qdt, portfolio.returns)
            leverage.replace_point(qdt, account.leverage)
            exposure.replace_point(qdt, account.total_positions_exposure)

    def end_of_session(self, dt, calendar_name, ledger):
        self.end_of_bar(dt, calendar_name, ledger)

        # todo: update risk metrics (alpha, beta,
        # leverage, etc

    def set_benchmark_source(self, calendar_name, benchmark_source):
        if calendar_name not in self._benchmark_sources:
            self._benchmark_sources[calendar_name] = BenchmarkReturnsAndVolatility(benchmark_source)
            self._benchmark_series[calendar_name] = timeseries.LineTimeSeries(
                self._dashboard.performance,
                calendar_name)

        # self._statistics.add_item(tables.Item())

    def reset(self):
        # self._cumulative_returns.clear()
        dash = self._dashboard

        dash.reset()

        for benchmark in self._benchmark_series.values():
            benchmark.clear()

