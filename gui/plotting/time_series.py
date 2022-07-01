import typing

from PyQt6 import QtCharts
from PyQt6.QtCharts import QAbstractSeries, QAbstractAxis
from PyQt6.QtCore import Qt
from PyQt6 import QtCore


class TimeSeries(QtCharts.QChart):
    def __init__(self):
        super(TimeSeries, self).__init__()

        self._series = []

        self._x_axis = x = QtCharts.QDateTimeAxis()
        self._y_axis = y = QtCharts.QValueAxis()

        # default format
        x.setFormat("dd MM yyyy")

        self.addAxis(x, Qt.AlignmentFlag.AlignBottom)
        self.addAxis(y, Qt.AlignmentFlag.AlignLeft)

    @property
    def x_axis(self):
        return self._x_axis

    @property
    def y_axis(self):
        return self._y_axis

    def addSeries(self, series: QAbstractSeries) -> None:
        super(TimeSeries, self).addSeries(series)

        series.attachAxis(self._x_axis)
        series.attachAxis(self._y_axis)

    def axes(self,
             orientation: QtCore.Qt.Orientation = ...,
             series: typing.Optional[QAbstractSeries] = ...) -> typing.List[QAbstractAxis]:
        return [self._x_axis, self._y_axis]
    