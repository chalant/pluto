from abc import ABC, abstractmethod

from PyQt6 import QtCharts, QtCore

from dev import events


class AbstractPlotter(QtCharts.QChartView):
    def __init__(self, container, name):
        super(AbstractPlotter, self).__init__(container)

        self._name = name

    @property
    def name(self):
        return self._name

    @abstractmethod
    def performance_update(
            self,
            open_dt: int,
            close_dt: int,
            packet):
        pass

    def closeEvent(self, a0):
        events.post_event('plotter.close', self._name)
