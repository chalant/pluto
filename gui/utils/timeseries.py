from PyQt6 import QtCharts


class LineTimeSeries(QtCharts.QLineSeries):
    def __init__(self, timeseries, name):
        super(LineTimeSeries, self).__init__()

        self._line = line = QtCharts.QLineSeries(timeseries)

        line.setName(name)

        timeseries.addSeries(line)

        self._timeseries = timeseries

    @staticmethod
    def _resize_axis(axis, y):
        if axis.max() <= y:
            axis.setMax(y * 2)
        elif axis.min() >= y:
            axis.setMin(y * 2)

    def append(self, x: float, y: float) -> None:
        self._resize_axis(self._timeseries.y_axis, y)
        self._line.append(x, y)

    def replace_point(self, x: float, y: float) -> None:
        line = self._line

        self._resize_axis(self._timeseries.y_axis, y)

        line.remove(x, y)
        line.append(x, y)

    def clear(self) -> None:
        super(LineTimeSeries, self).clear()

        self._line.clear()


class BarTimeSeries(QtCharts.QBarSeries):
    def __init__(self, timeseries, name):
        super(BarTimeSeries, self).__init__()

        self._bars = bar = QtCharts.QBarSeries(timeseries)

        bar.setName(name)

        timeseries.addSeries(bar)

    def append(self, set_: QtCharts.QBarSet) -> bool:
        pass
