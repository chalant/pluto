from PyQt6 import QtWidgets, QtCore
from PyQt6.QtCore import Qt

from pluto.coms.utils import conversions

from protos import controller_pb2

from dev import events

from gui.plotting import cumulative_risk

events.add_event('plotter.closed')
events.add_event('plotter.add')
# todo: we the list of fields (note: by default, we plot the returns and the benchmark returns)

# todo: we need to be able to add plotters... (combo box?)


class PlottingHandler(QtWidgets.QWidget):
    def __init__(self, parent, layout):
        super(PlottingHandler, self).__init__(parent)

        self._plotters = {}

        self._layout = layout

        self._splitter = QtWidgets.QSplitter(
            parent=self,
            orientation=Qt.Orientation.Vertical)

        # self._layout.addWidget(self._splitter)
        layout.addWidget(self._splitter)

        # self._splitter.setMinimumWidth(parent.width())
        # self._splitter.setMinimumHeight(parent.height())
        # self._splitter.

        self._data_frequency = 'daily'
        self._emission_rate = 'minute'
        self._plotting_range = (QtCore.QDateTime.currentDateTime().addYears(-1), QtCore.QDateTime.currentDateTime())

        self._perf_field = 'daily_perf' if self._data_frequency == 'daily' else 'minute_perf'

        events.subscribe('plotter.closed', self._remove_plotter)
        events.subscribe('plotter.add', self._create_plotter)

        self._parent = parent

    def reset(self):
        for plotter in self._plotters.values():
            plotter.reset()

    def set_plotting_setup(self, **kwargs):
        self._data_frequency = kwargs['data_frequency']
        self._emission_rate = kwargs['emission_rate']

        x, y = kwargs['plotting_range']

        t1 = QtCore.QDateTime(x.to_pydatetime())
        t2 = QtCore.QDateTime(y.to_pydatetime())

        self._plotting_range = (
            QtCore.QDateTime(x.to_pydatetime()),
            QtCore.QDateTime(y.to_pydatetime()))

        for plotter in self._plotters.values():
            plotter.set_plotting_period(t1, t2)

        self._perf_field = 'daily_perf' if self._data_frequency == 'daily' else 'minute_perf'

    def performance_update(self, packet):
        # todo: need to append performance so that when we add a plotter, we plot
        #  starting from some point.

        perf = controller_pb2.PerformancePacket()
        perf.ParseFromString(packet.packet)

        perf = conversions.from_proto_performance_packet(perf)

        period = perf[self._perf_field]

        dt1 = QtCore.QDateTime(period['period_open'].to_pydatetime()).toMSecsSinceEpoch()
        dt2 = QtCore.QDateTime(period['period_close'].to_pydatetime()).toMSecsSinceEpoch()

        for plotter in self._plotters.values():
            plotter.performance_update(dt1, dt2, perf)

        if packet.end:
            events.post_event("backtesting.ended", None)

    def _add_plotter(self, plotter):
        dock = QtWidgets.QDockWidget(self)
        dock.setWidget(plotter)

        dock.setWindowTitle(plotter.name)

        dock.setFeatures(dock.features().DockWidgetClosable)

        self._plotters[plotter.name] = plotter

        self._splitter.addWidget(dock)

        dock.show()

    def _create_plotter(self, name):
        t1, t2 = self._plotting_range

        if name == 'Performance':
            plotter = cumulative_risk.PerformancePlotter(self)
            self._add_plotter(plotter)

            # size = self._layout.sizeHint()
            #
            # plotter.setGeometry(0, 0, size.width(), size.height())
            plotter.set_plotting_period(t1, t2)

        elif name == 'Leverage':
            plotter = cumulative_risk.MaxLeverage(self)
            self._add_plotter(plotter)

            plotter.set_plotting_period(t1, t2)

    def _remove_plotter(self, name):
        self._plotters.pop(name)


class DailyPerformancePlotter(object):
    def performance_update(self, packet):
        # append until end...

        perf = controller_pb2.PerformancePacket()
        perf.ParseFromString(packet.packet)

        perf = conversions.from_proto_performance_packet(perf)

        # todo: we also need minute updates...
        # todo:

        daily_perf = perf.get('daily_perf', None)
        daily_perf.update(perf['cumulative_risk_metrics']),

        cum_perf = perf['cumulative_perf']
        cum_perf.pop('period_close')
        cum_perf.pop('period_open')
        cum_perf.pop('capital_used')
        cum_perf.pop('starting_exposure')
        cum_perf.pop('ending_exposure')
        cum_perf.pop('starting_value')
        cum_perf.pop('starting_cash')
        cum_perf.pop('returns')
        cum_perf.pop('pnl')

        # todo: plot returns as they come
        # todo: append results...

        daily_perf.update(cum_perf)

        if packet.end:
            events.post_event("backtesting.ended", None)


# todo
class MinutePerformancePlotter(object):
    def performance_update(self, packet):
        # append until end...

        perf = controller_pb2.PerformancePacket()
        perf.ParseFromString(packet.packet)

        perf = conversions.from_proto_performance_packet(perf)

        # todo: we also need minute updates...
        # todo:

        daily_perf = perf.get('daily_perf', None)
        daily_perf.update(perf['cumulative_risk_metrics']),

        cum_perf = perf['cumulative_perf']
        cum_perf.pop('period_close')
        cum_perf.pop('period_open')
        cum_perf.pop('capital_used')
        cum_perf.pop('starting_exposure')
        cum_perf.pop('ending_exposure')
        cum_perf.pop('starting_value')
        cum_perf.pop('starting_cash')
        cum_perf.pop('returns')
        cum_perf.pop('pnl')

        # todo: plot returns as they come
        # todo: append results...

        daily_perf.update(cum_perf)

        if packet.end:
            events.post_event("backtesting.ended", None)
