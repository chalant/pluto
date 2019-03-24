import pandas as pd

import trading_calendars as tc

from logbook import Processor

from zipline.data import data_portal as dp
from zipline.protocol import BarData
from zipline.utils.pandas_utils import normalize_date

from contrib.control import clock
from contrib.coms.services import broker_service as brs


class SimulationBrokerControl(object):
    def __init__(self, calendar_name, restrictions, universe_func, start_dt, end_dt=None):
        """

        Parameters
        ----------
        calendar_name
        restrictions
        universe_func
        start_dt
        end_dt

        Attributes
        ----------
        clock : contrib.control.Clock
        """
        self._data_portal = None

        self._universe_func = universe_func
        self._restrictions = restrictions

        self._current_data = None
        self._asset_finder = None

        self._run_dt = None

        # Processor function for injecting the algo_dt into
        # user prints/logs.
        def inject_algo_dt(record):
            if 'algo_dt' not in record.extra:
                record.extra['algo_dt'] = self._get_run_dt

        self._processor = Processor(inject_algo_dt)

        self._last_sync_time = pd.NaT

        if end_dt is None:
            end_dt = pd.Timestamp.utcnow()

        self._calendar = cal = tc.get_calendar(calendar_name)

        start_dt = normalize_date(start_dt)
        if end_dt is None:
            end_dt = normalize_date(pd.Timestamp.utcnow())

        self._clock = clock.MinuteSimulationClock(cal, start_dt, end_dt)

    def _create_bar_data(self, universe_func, data_portal, get_dt, data_frequency, calendar, restrictions):
        return BarData(
            data_portal=data_portal,
            simulation_dt_func=get_dt,
            data_frequency=data_frequency,
            trading_calendar=calendar,
            restrictions=restrictions,
            universe_func=universe_func)

    def _get_run_dt(self):
        return self._run_dt
    #todo: we need an offset: each of these events must be generated BEFORE the events of the client-side (say
    # a minute)
    def run(self, bundler):
        cl = self._clock
        for dt, evt in cl:
            if evt == clock.INITIALIZE:
                self._load_attributes(bundler.load(), cl.calendar, bundler.data_frequency)
            elif evt == clock.SESSION_END:
                pass


    def _load_data_portal(self, calendar, asset_finder, first_trading_day, equity_minute_bar_reader,
                          equity_daily_bar_reader, adjustment_reader):
        return dp.DataPortal(asset_finder, calendar, first_trading_day,
                             equity_daily_bar_reader, equity_minute_bar_reader,adjustment_reader)

    def _load_attributes(self, bundle, calendar, data_frequency='daily'):
        equity_minute_reader = bundle.equity_minute_bar_reader
        self._asset_finder = asset_finder = bundle.asset_finder

        self._current_data = self._create_bar_data(
            self._universe_func,
            self._load_data_portal(
                calendar, asset_finder, equity_minute_reader.first_trading_day,
                equity_minute_reader, bundle.equity_daily_bar_reader, bundle.adjustment_reader
            ),
            self._get_run_dt,
            data_frequency,
            calendar,
            self._restrictions
        )