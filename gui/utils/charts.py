import typing

from PyQt6 import QtCharts
from PyQt6.QtCharts import QAbstractSeries, QAbstractAxis
from PyQt6.QtCore import Qt
from PyQt6 import QtCore


class TimeSeriesChart(QtCharts.QChart):
    def __init__(self):
        super(TimeSeriesChart, self).__init__()

        self._series = []

        self._x_axis = x = QtCharts.QDateTimeAxis()
        self._y_axis = y = QtCharts.QValueAxis()

        # default format
        x.setFormat("dd MM yyyy")

        self.addAxis(x, Qt.AlignmentFlag.AlignBottom)
        self.addAxis(y, Qt.AlignmentFlag.AlignLeft)

    def set_plotting_period(
            self,
            start_dt: QtCore.QDateTime,
            end_dt: QtCore.QDateTime):

        x = self.x_axis

        x.setMin(start_dt)
        x.setMax(end_dt)

    def set_plotting_range(self, low, high):
        y = self.y_axis

        y.setMin(low)
        y.setMax(high)

    @property
    def x_axis(self):
        return self._x_axis

    @property
    def y_axis(self):
        return self._y_axis

    def addSeries(self, series: QAbstractSeries) -> None:
        super(TimeSeriesChart, self).addSeries(series)

        series.attachAxis(self._x_axis)
        series.attachAxis(self._y_axis)

    def axes(self,
             orientation: QtCore.Qt.Orientation = ...,
             series: typing.Optional[QAbstractSeries] = ...) -> typing.List[QAbstractAxis]:
        return [self._x_axis, self._y_axis]
    