from abc import ABC, abstractmethod
from protos import clock_pb2
import pandas as pd

from contrib.control import domain

class DomainFilter(ABC):
    def __init__(self, dom_def, clocks, exchange_mappings, broker):
        '''groups sessions that share the same domain'''
        self._session_ids = []
        self._clocks = clocks = {clock.exchange:clock for clock in clocks}
        self._dom_def = dom_def
        self._exchange_mappings = exchange_mappings

        self._dom_struct = dom_struct = self._create_dom_struct(
                dom_def,
                exchange_mappings,
                clocks)

        self._sess_idx = 0
        self._minute_idx = 0

        self._to_update = []
        self._to_liquidate = []
        self._to_add = []

        self._broker = broker

        self._earliest_exc = None
        self._latest_exc = None

        session = dom_struct.sessions[0]
        self._minutes = minutes = self._load_minutes(session, session, clocks)
        self._expected_ts = None

        self._earliest_and_latest_clock(minutes, clocks)
        self._flag = False

    def clock_update(self, clock_evt):
        #todo: the first minute is the open dt, the session start and before trading starts dt are
        # before the open_dt
        # any datetime before the open_dt, can be used for initialization.

        broker = self._broker
        dom_struct = self._dom_struct
        minutes = self._minutes
        dt = clock_evt.dt

        #todo: assumption the first session of the domain is either bigger or equal to the current session
        sessions = dom_struct.sessions
        expected_session = sessions[self._sess_idx]

        #execute if the session is as expected
        if clock_evt.label == expected_session:
            # only process the signal from the earliest clock
            if clock_evt.event == clock_pb2.SESSION_START:
                # todo: only consider the earliest clock
                if clock_evt.exchange == self._earliest_exc:
                    self._update(self._sessions(self._broker), clock_evt)

            elif clock_evt.event == clock_pb2.SESSION_END:
                # only execute when the signal comes from the latest exchange.
                if clock_evt.exchange == self._latest_exc:
                    self._update(self._sessions(self._broker), clock_evt)
                    self._sess_idx += 1
                    clocks = self._clocks.values()
                    try:
                        session = dom_struct.sessions[self._sess_idx]
                    except IndexError:
                        self._sess_idx = 0
                        self._dom_struct = dom_struct = self._create_dom_struct(
                            self._dom_def,
                            self._exchange_mappings,
                            clocks)
                        sessions = dom_struct.sessions
                        session = sessions[0]

                    self._minutes = minutes = self._load_minutes(dt, session, clocks)
                    # reload earliest and latest exchanges, since we've changed sessions.
                    self._earliest_and_latest_clock(minutes, clocks)
                    self._expected_ts = clock_evt.dt

            #execute on the earliest clock
            elif clock_evt.event == clock_pb2.BEFORE_TRADING_START:
                if clock_evt.exchange == self._earliest_exc:
                    self._update(self._sessions(self._broker), clock_evt)

            # execute on any calendar event.
            elif clock_evt.event == clock_pb2.CALENDAR:
                #recompute domain and minutes.
                self._dom_struct = dom_struct = self._create_dom_struct(
                    self._dom_def,
                    self._exchange_mappings,
                    self._clocks.values())
                self._sess_idx = 0
                label = dom_struct.sessions[self._sess_idx]
                self._minutes = self._load_minutes(dt, label, self._clocks)

            elif clock_evt.event == clock_pb2.MINUTE_END:

                # only advance when dt and ts matches, => events with the same datetime won't be considered
                if dt == self._expected_ts:
                    #reset flag so that the next bar update is processed...
                    self._flag = False

                    # add and update sessions on minute end event.
                    #todo: new sessions data must be initialized... => send data to new sessions
                    broker.add_sessions((session for session in self._to_add))
                    broker.update_sessions((session, params for session, params in self._to_update))
                    broker.liquidate((session for session in self._to_liquidate))

                    self._to_add.clear()
                    self._to_update.clear()
                    self._to_liquidate.clear()

                    self._update(self._sessions(self._broker), clock_evt)

                    #pre-load the next expected datetime
                    self._minute_idx += 1
                    try:
                        self._expected_ts = minutes[self._minute_idx]
                    except IndexError:
                        self._dom_struct = dom_struct = self._create_dom_struct(
                            self._dom_def,
                            self._exchange_mappings,
                            self._clocks.values())
                        self._sess_idx = 0
                        label = dom_struct.sessions[0]
                        self._minutes = minutes = self._load_minutes(dt, label, self._clocks.values())
                        self._minute_idx = 0
                        self._expected_ts = minutes[0]

        elif clock_evt.event == clock_pb2.BAR:
            # only update once per call
            if not self._flag:
                self._update(self._sessions(self._broker), clock_evt)
                self._flag = True

    def _earliest_and_latest_clock(self, minutes, clocks):
        earliest = {}
        latest = {}

        for clk in clocks:
            erl = earliest.get(minutes[0], None)
            lat = latest.get(minutes[-1], None)
            if not erl:
                earliest[minutes[0]] = [clk.exchange]
                latest[minutes[-1]] = [lat.exchange]
            else:
                erl.append(clk.exchange)
                lat.append(clk.exchange)

        first = min(earliest.keys())
        last = min(latest.keys())

        self._earliest_exc = earliest[first].pop()
        self._latest_exc = latest[last].pop()

    def _sessions(self, broker):
        #a generator that yields active sessions
        session_ids = self._session_ids
        l = len(session_ids)
        for i in range(l):
            sess_id = session_ids[i]
            session = broker.get_session(sess_id)
            #if the session has been removed, remove its id and skip it.
            if not session:
                del session_ids[i]
                continue
            yield session

    def _update(self, sessions, clock_evt):
        for session in sessions:
            session.update(clock_evt)

    def add_session(self, session):
        self._session_ids.append(session.id)
        self._to_add.append(session)

    def update_session(self, session, params):
        self._to_update.append((session, params))

    def liquidate(self, session):
        self._to_liquidate.append(session)

    def _create_dom_struct(self, dom_def, exchange_mappings, clocks):
        #todo: must raise an exception of there is no sessions in the domain struct (len(sessions==0))
        sessions_per_exchange = {}
        for clock in clocks:
            sessions_per_exchange[clock.exchange] = clock.get_sessions(5)
        return  domain.compute_domain(dom_def, exchange_mappings, sessions_per_exchange)

    def _load_minutes(self, dt, session, clocks):
        minutes = pd.DatetimeIndex([])
        #only consider minutes that are above the current datetime
        for clk in clocks:
            minutes = minutes.union(
                pd.DatetimeIndex(
                    [minute for minute in clk.minutes_for_session(session) if minute >= dt]
                )
            )
        return minutes

    def ingest(self, data):
        if data.exchange not in self._dom_struct.exchanges:
            pass
        else:
            pass #todo

#todo: put this in a module...
class Downloader(object):
    def __init__(self):
        self._dom_filters = {}

    def clock_update(self, clock_evt):
        for domain_filter in self._dom_filters.values():
            domain_filter.clock_update(clock_evt)

        if clock_evt.event == clock_pb2.MINUTE_END:
            self._download(clock_evt.dt, clock_evt.exchange_name, self._dom_filters.values())

    def add_domain_filter(self, domain_filter):
        self._dom_filters[domain_filter.domain_id] = domain_filter

    def _download(self, dt, exchange, domain_filters):
        pass