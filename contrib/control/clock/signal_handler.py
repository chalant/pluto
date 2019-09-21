from abc import ABC, abstractmethod
from protos import clock_pb2
import pandas as pd

from contrib.control import domain

class TimeFilter(ABC):
    def __init__(self, dom_def, exchange_mappings):
        '''regroups sessions that share the same domain'''
        self._sessions = []
        self._clocks = {}
        self._dom_def = dom_def
        self._exchange_mappings = exchange_mappings

        self._dom_struct = None
        self._sess_idx = -1
        self._minute_idx = 0
        self._minutes = None

    def update(self, clock, clock_evt):
        dom_struct = self._dom_struct
        minutes = self._minutes
        dt = clock_evt.dt
        if clock_evt.event == clock_pb2.SESSION_START:
            if self._sess_idx == -1:
                dom_struct = self._create_dom_struct(
                    self._dom_def,
                    self._exchange_mappings,
                    self._clocks.values())
            self._sess_idx += 1
            session = dom_struct.sessions[self._sess_idx]
            self._minutes = self._load_minutes(session)

            for clk in self._clocks:
                minutes.union(clk.minutes_for_session(session))
            minute_idx = 0
            while minutes[minute_idx] < dt:
                minute_idx += 1
            self._minute_idx = minute_idx
        #calendar update
        elif clock_evt.event == clock_pb2.CALENDAR:
            dom_struct = self._create_dom_struct(
                self._dom_def,
                self._exchange_mappings,
                self._clocks.values())
            self._sess_idx = 0
            label = dom_struct.sessions[self._sess_idx]
            self._minutes = minutes = self._load_minutes(label)

            minute_idx = 0
            while minutes[minute_idx] < dt:
                minute_idx += 1
            self._minute_idx = minute_idx

        #only update if initialized
        if self._sess_idx != -1:
            try:
                ts = minutes[self._minute_idx]
            except IndexError:
                dom_struct = self._create_dom_struct(
                    self._dom_struct,
                    self._exchange_mappings,
                    self._clocks.values())
                self._sess_idx = 0
                label = dom_struct.sessions[self._sess_idx]
                self._minutes = minutes = self._load_minutes(label)
                minute_idx = 0
                while minutes[minute_idx] < dt:
                    minute_idx += 1
                self._minute_idx = minute_idx
                ts = minutes[minute_idx]

            if dt == ts:
                self._minute_idx += 1

            self._update(self._sessions, clock_evt)

    @abstractmethod
    def _update(self, sessions, clock_evt):
        raise NotImplementedError

    def add_clock(self, clock):
        self._clocks[clock.exchange] = clock

    def add_session(self, session):
        self._sessions.append(session)

    def _create_dom_struct(self, dom_def, exchange_mappings, clocks):
        #todo: must raise an exception of there is no sessions in the domain struct (len(sessions==0))
        sessions_per_exchange = {}
        for clock in clocks:
            sessions_per_exchange[clock.exchange] = clock.get_sessions(5)
        return  domain.compute_domain(dom_def, exchange_mappings, sessions_per_exchange)

    def _load_minutes(self, session):
        minutes = pd.DatetimeIndex([])
        for clk in self._clocks:
            minutes = minutes.union(clk.minutes_for_session(session))
        return minutes

class SimulationTimeFilter(TimeFilter):
    def _update(self, sessions, clock_evt):
        # todo: we need to make sure that the controllables (traders) have initialized data => send
        #  data bundle to all the traders
        for session in sessions:
            session.update(sessions, clock_evt)

class LiveTimeFilter(TimeFilter):
    def __init__(self, dom_def, exchange_mappings, downloader):
        super(LiveTimeFilter, self).__init__(dom_def, exchange_mappings)
        self._downloader = downloader

    def _update(self, sessions, clock_evt):
        # todo: we need to make sure that the controllables (traders) have initialized data => send
        #  data bundle to all the traders

        for session in sessions:
            session.update(sessions, clock_evt)
        if clock_evt.event == clock_pb2.MINUTE_END:
            # downloads data at the specified datetime for the specified exchange and updates
            # the sessions data as-well as the database
            self._downloader.download(clock_evt.dt, clock_evt.exchange_name, sessions)

class SignalHandler(ABC):
    def __init__(self):
        self._update_fn = self._pass
        self._clocks = {}
        self._time_filters = {}

    def _pass(self, clock, clock_evt):
        pass

    def update(self, clock, clock_evt):
        self._update_fn(clock, clock_evt)

    def add_clock(self, clock):
        self._clocks[clock.exchange] = clock

    def get_time_filter(self, dom_def, exchange_mappings):
        id_ = domain.domain_id(dom_def)
        t_filters = self._time_filters
        filter = t_filters.get(id_, None)
        if not filter:
            self._time_filters[id_] = filter = self._create_time_filter(dom_def, exchange_mappings)
        return filter

    @abstractmethod
    def _create_time_filter(self, dom_def, exchange_mappings):
        raise NotImplementedError

    def _clock_update(self, clock, clock_evt):
        for filter in self._time_filters.values():
            filter.update(clock, clock_evt)

    def activate(self):
        self._update_fn = self._clock_update

    def deactivate(self):
        self._update_fn = self._pass


class SimulationSignalHandler(SignalHandler):
    def __init__(self):
        super(SimulationSignalHandler, self).__init__()

    def _create_time_filter(self, dom_def, exchange_mappings):
        return TimeFilter(dom_def, exchange_mappings)

class LiveSignalHandler(SignalHandler):
    def __init__(self, downloader):
        super(LiveSignalHandler, self).__init__()
        self._downloader = downloader

    def _create_time_filter(self, dom_def, exchange_mappings):
        return LiveTimeFilter(dom_def, exchange_mappings, self._downloader)