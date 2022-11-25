from PyQt6 import QtWidgets, QtCore
from PyQt6.QtCore import Qt

from pandas import Timestamp

from zipline.assets.assets import Equity, Future
from zipline.finance.execution import (
    StopOrder,
    LimitOrder,
    StopLimitOrder,
    MarketOrder)

from pluto.setup import run_setup as rs
from pluto.data.universes import core
from pluto.data import caching
from pluto.finance.broker import broker_setup as bks

from gui.fields import fields
from gui.fields import combobox

from dev import events

events.add_event('setup.universe')
events.add_event('setup.data_frequency')
events.add_event('setup.broker')
events.add_event('setup.start_date')
events.add_event('setup.end_date')
events.add_event('setup.capital')

events.add_event('setup.internal.start')
events.add_event('setup.internal.end')

events.add_event('universe.loaded')


class Date(QtWidgets.QDateEdit):
    def __init__(self, parent, event_name):
        """

        Parameters
        ----------
        parent
        """
        super(Date, self).__init__(parent)

        self.dateChanged.connect(self._set_date)

        self._event_name = event_name

    def _set_date(self, qt_date):
        events.post_event(self._event_name, qt_date)


class Brokers(object):
    def __init__(self):
        self._item_names = [
            'alpaca'
        ]

        self._current_item_index = 0

    @property
    def item_names(self):
        return self._item_names

    @property
    def current_item_index(self):
        return self._current_item_index

    @current_item_index.setter
    def current_item_index(self, value):
        self._current_item_index = value

        events.post_event('setup.broker', self._item_names[value])


class Universes(object):
    def __init__(self):
        self._item_names = [
            'test'
        ]

        self._current_item_index = 0

    @property
    def item_names(self):
        return self._item_names

    @property
    def current_item_index(self):
        return self._current_item_index

    def set_item_index(self, value):
        events.post_event('setup.universe', self._item_names[value])


class DataFrequencies(object):
    def __init__(self, universe):
        self._item_names = [i for i in universe.data_frequencies]

        self._current_item_index = 0

    @property
    def item_names(self):
        return self._item_names

    @property
    def current_item_index(self):
        return self._current_item_index

    def set_item_index(self, value):
        events.post_event('setup.data_frequency', self._item_names[value])


class BackTestPeriod(QtWidgets.QWidget):
    def __init__(self, parent):
        super(BackTestPeriod, self).__init__(parent)

        layout = QtWidgets.QHBoxLayout(self)

        self.setLayout(layout)

        start_dt = QtWidgets.QDateEdit()
        end_dt = QtWidgets.QDateEdit()

        start_dt.setDisabled(True)
        end_dt.setDisabled(True)

        events.subscribe('universe.loaded', self._universe_loaded)

        events.subscribe('setup.internal.start', self._start_date_updated)
        events.subscribe('setup.internal.end', self._end_date_updated)

        self._start_dt = start_dt = Date(self, 'setup.internal.start')
        self._end_dt = end_dt = Date(self, 'setup.internal.end')

        layout.addWidget(
            fields.Field(
                self,
                "Start",
                start_dt))

        layout.addWidget(
            fields.Field(
                self,
                "End",
                end_dt))

        # todo: the smallest datetime is determined by the available data.
        #  asset lifetimes, or data portal?

    def _strategy_loaded(self, meta):
        pass

    def _new_strategy(self, meta):
        pass

    def _universe_loaded(self, uni):

        self._min_date = QtCore.QDate(uni.start_date.to_pydatetime().date())
        self._max_date = QtCore.QDate(uni.end_date.to_pydatetime().date())

        self._start_dt.setDate(self._min_date)
        self._end_dt.setDate(self._max_date)

        self._start_dt.setMinimumDate(self._min_date)
        self._start_dt.setMaximumDate(self._end_dt.date().addDays(-1))

        self._end_dt.setMinimumDate(self._start_dt.date().addDays(1))
        self._end_dt.setMaximumDate(self._max_date)

        self._start_dt.setDisabled(False)
        self._end_dt.setDisabled(False)

    def _start_date_updated(self, dt):
        self._end_dt.setMinimumDate(dt.addDays(1))

        events.post_event('setup.start_date', Timestamp(dt.toPyDate(), tz='utc'))

    def _end_date_updated(self, dt):
        self._start_dt.setMaximumDate(dt.addDays(-1))

        events.post_event('setup.end_date', Timestamp(dt.toPyDate(), tz='utc'))


class Setup(QtWidgets.QWidget):
    def __init__(self, container):
        super(Setup, self).__init__(container)

        self._layout = layout = QtWidgets.QHBoxLayout(self)

        # self._layout = QtWidgets.QVBoxLayout(self)
        # splitter.setLayout(self._layout)

        self._container = container
        self._width = 0

    def add_setup(self, widget):
        """

        Parameters
        ----------
        widget: QtWidgets.QWidget

        Returns
        -------

        """

        # widget.setParent(dock)
        # widget.setParent(self)
        self._layout.addWidget(widget)

        # width = self._width

        # if width < widget.width():
        #     self.setFixedWidth(widget.width())
        #     width = widget.width()
        #     self._width = width
        #
        # else:
        #     widget.setFixedWidth(width)

    # def fit(self):
    #     self._container.setFixedWidth(self._layout.sizeHint().width())


# setup widget
class BacktestSetup(QtWidgets.QWidget):
    def __init__(self, parent, session_id, directory):
        super(BacktestSetup, self).__init__(parent)

        self._run_setup = stp = rs.RunSetup(session_id)
        self._directory = directory

        stp.clear_perf = True

        # todo: add a way to serialize setup object... (yaml?)
        #  note: we already have a way in protobuf...

        # todo: default broker setup, ideally, we should load this
        #  from a file

        fts = bks.FractionalTradeSetup()
        ts = bks.TradeSetup()
        bs = bks.BrokerSetup()

        fts.asset_types = {Equity.__name__}
        fts.order_types = {MarketOrder.__name__}
        fts.minimal_fraction = 3
        fts.minimum_purchase = 1.0

        ts.asset_types = {Equity.__name__, Future.__name__}
        ts.order_types = {
            StopOrder.__name__,
            LimitOrder.__name__,
            StopLimitOrder.__name__,
            MarketOrder.__name__}

        bs.fractional = True

        stp.fractional_trade_setup = fts
        stp.trade_setup = ts

        # todo: need to add tax setup
        stp.tax_models = {}

        self._frame = frame = QtWidgets.QFrame(self)
        self._layout = layout = QtWidgets.QVBoxLayout(frame)

        events.subscribe('setup.universe', self._set_universe)
        events.subscribe('setup.data_frequency', self._set_data_frequency)

        events.subscribe('setup.start_date', self._set_start)
        events.subscribe('setup.end_date', self._set_end)

        # events.subscribe('strategy.loaded', self._activate)
        # events.subscribe('strategy.new', self._activate)

        self._datetime_setup = BackTestPeriod(frame)

        self._data_frequency = combobox.ComboBox(frame)

        layout.addWidget(fields.Field(
            frame,
            'Data Frequency',
            self._data_frequency))

        self._data_frequency.setDisabled(True)

        self._universes = uni = combobox.ComboBox(frame)
        uni.set_item_store(Universes())
        # universes.set_item_store(Universes())

        # self._set_universe(universes.value)

        layout.addWidget(fields.Field(
            frame,
            'Universe',
            uni))

        layout.addWidget(self._datetime_setup)
        self._capital_slider = slider = QtWidgets.QSlider(
            orientation=Qt.Orientation.Horizontal)

        slider.setMinimum(1000)
        slider.setMaximum(100000000)
        slider.setSingleStep(1000)

        layout.addWidget(fields.Field(
            frame,
            'Capital',
            slider))

        slider.valueChanged.connect(self._capital_value)

        parent.setFixedWidth(self._layout.sizeHint().width())

    def width(self) -> int:
        return self._layout.sizeHint().width()

    @property
    def run_setup(self):
        return self._run_setup

    def _activate(self, stg):
        self._universes.set_item_store(Universes())

    def _set_universe(self, name):
        uni = universes.get_universe(
            name,
            self._directory,
            caching.InMemoryCache())

        events.post_event('universe.loaded', uni)

        self._run_setup.trading_universe = name

        self._data_frequency.set_item_store(DataFrequencies(uni))
        self._data_frequency.setDisabled(False)

    def _capital_value(self, value):
        self._run_setup.capital = float(value)

    def load_setup(self, meta):
        # todo: might need to load stuff from file... (broker parameters)

        # set default values
        run_setup = self._run_setup
        period = self._datetime_setup

        run_setup.capital = float(self._capital_slider.value())
        # run_setup.start = period.start
        # run_setup.end = period.end

        run_setup.trading_universe = self._universes.value

        # todo: also load broker setup from a file...

        return run_setup

    def _set_data_frequency(self, value):
        self._run_setup.data_frequency = value

    def _set_start(self, value):
        self._run_setup.start = value

    def _set_end(self, value):
        self._run_setup.end = value
