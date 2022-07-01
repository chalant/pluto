from PyQt6 import QtCharts, QtCore, QtWidgets
from PyQt6.QtCore import Qt


from gui.plotting import plotter, time_series


class MaxLeverage(plotter.AbstractPlotter):
    def __init__(self,
                 container: QtWidgets.QWidget):
        super(MaxLeverage, self).__init__(
            container, 'Leverage')

        self._chart = ts = time_series.TimeSeries()

        ts.y_axis.setMin(0)
        ts.y_axis.setMax(4)

        self._y_axis = ts.y_axis

        self._max_y = 4
        self._min_y = 0

        self._max_leverage = ml = QtCharts.QLineSeries(self)

        ts.addSeries(ml)

        self.setChart(ts)

    def performance_update(
            self,
            open_dt: int,
            close_dt: int,
            packet):
        cum_risk = packet['cumulative_risk_metrics']

        leverage = cum_risk['max_leverage']

        if leverage > self._max_y:
            self._max_y = leverage * 2
            self._y_axis.setMax(leverage * 2)
        elif leverage < self._min_y:
            self._min_y = leverage * 2
            self._y_axis.setMin(leverage * 2)

        self._max_leverage.append(
            close_dt,
            leverage)

    def set_plotting_period(
            self,
            start_dt: QtCore.QDateTime,
            end_dt: QtCore.QDateTime):

        x = self._chart.x_axis

        x.setMin(start_dt)
        x.setMax(end_dt)

    def reset(self):
        self._max_leverage.clear()

        x, y = self._chart.axes()

        y.setMax(4)
        y.setMin(0)

        self._max_y = 4
        self._min_y = 0


class PerformancePlotter(plotter.AbstractPlotter):
    def __init__(self,
                 container: QtWidgets.QWidget):

        super(PerformancePlotter, self).__init__(
            container,
            'Performance')

        self._chart = ts = time_series.TimeSeries()

        self._strategy_returns = algo_ret = QtCharts.QLineSeries(self)
        self._benchmark_returns = bench_ret = QtCharts.QLineSeries(self)

        algo_ret.setName("Algorithm")
        bench_ret.setName("Benchmark")

        y = ts.y_axis

        y.setMax(100)
        y.setMin(-100)

        ts.addSeries(algo_ret)
        ts.addSeries(bench_ret)

        self._max_y = 100
        self._min_y = -100

        self.setChart(ts)

    def set_plotting_period(
            self,
            start_dt: QtCore.QDateTime,
            end_dt: QtCore.QDateTime):

        x = self._chart.x_axis

        x.setMin(start_dt)
        x.setMax(end_dt)

    def performance_update(self, open_dt: int, close_dt: int, packet):

        cum_risk = packet['cumulative_risk_metrics']

        stg = cum_risk['algorithm_period_return']
        bench = cum_risk['benchmark_period_return']

        y = self._chart.y_axis

        max_ = max(stg, bench)
        min_ = min(stg, bench)

        if max_ > self._max_y:
            y.setMax(max_ * 2)

            self._max_y = max_ * 2

        elif min_ < self._min_y:
            y.setMin(min_ * 2)

            self._min_y = min_ * 2

        self._strategy_returns.append(
            close_dt,
            stg,
        )

        self._benchmark_returns.append(
            close_dt,
            bench
        )

    def reset(self):
        self._strategy_returns.clear()
        self._benchmark_returns.clear()

        x, y = self._chart.axes()

        y.setMax(100)
        y.setMin(-100)

        self._max_y = 100
        self._min_y = -100



