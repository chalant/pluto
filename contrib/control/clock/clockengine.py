from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import  uuid
from concurrent import futures

import itertools

import numpy as np

import pandas as pd

import google.protobuf.empty_pb2 as emp
import grpc

import ntplib

import time

from contrib.coms.utils import conversions as crv
from contrib.coms.utils import server_utils as srv
from contrib.control.clock import clock_pb2_grpc as cl_servicer
from contrib.control.clock.clock_pb2 import (
    BAR,
    BEFORE_TRADING_START,
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    INITIALIZE,
    LIQUIDATE,
    STOP,
    CALENDAR
)

from contrib.control.clock import clock_pb2 as cl

from trading_calendars.utils.pandas_utils import days_at_time
from contrib.trading_calendars import calendar_utils as cu

'''module for events extension...'''

_nanos_in_minute = np.int64(60000000000)


class ClockEngine(ABC):
    @abstractmethod
    @property
    def alive(self):
        """

        Returns
        -------
        bool
        """

    @abstractmethod
    @property
    def emission_rate(self):
        """

        Returns
        -------
        str

        """

    @property
    @abstractmethod
    def calendar(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self, liquidate=False):
        raise NotImplementedError

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError


class MinuteEventGenerator(object):
    def __init__(self, minute_emission=False):
        self._stop = False
        self._liquidate = False
        self._minute_emission = minute_emission
        self._first_call_flag = False

    def stop(self, liquidate=False):
        self._stop = True
        self._liquidate = liquidate

    def get_event_generator(self, calendar, start_dt, end_dt, before_trading_starts_time):
        # loops every x frequency

        sessions = calendar.sessions_in_range(start_dt, end_dt)
        trading_o_and_c = calendar.schedule.ix[sessions]
        market_closes = trading_o_and_c['market_close']

        before_trading_start_minutes = days_at_time(
            sessions,
            before_trading_starts_time,
            calendar.tz
        )

        minute_emission = self._minute_emission

        if minute_emission:
            market_opens = trading_o_and_c['market_open']
            execution_opens = calendar.execution_time_from_open(market_opens)
            execution_closes = calendar.execution_time_from_close(market_closes)
        else:
            execution_closes = calendar.execution_time_from_close(market_closes)
            execution_opens = execution_closes

        market_opens_nanos = execution_opens.values.astype(np.int64)
        market_closes_nanos = execution_closes.values.astype(np.int64)

        session_nanos = sessions.values.astype(np.int64)
        bts_nanos = before_trading_start_minutes.values.astype(np.int64)

        minutes_by_session = self._calc_minutes_by_session(market_opens_nanos, market_closes_nanos, session_nanos)

        for idx, session_nano in session_nanos:
            bts_minute = pd.Timestamp(bts_nanos[idx], tz='UTC')
            regular_minutes = minutes_by_session[session_nano]
            if not self._stop:
                if not self._first_call_flag:
                    self._first_call_flag = True
                    yield start_dt, INITIALIZE
                yield start_dt, SESSION_START

                if bts_minute > regular_minutes[-1]:
                    # before_trading_start is after the last close,
                    # so don't emit it
                    for minute, evt in self._get_minutes_for_list(
                            regular_minutes,
                            minute_emission
                    ):
                        yield minute, evt
                else:
                    # we have to search anew every session, because there is no
                    # guarantee that any two session start on the same minute
                    bts_idx = regular_minutes.searchsorted(bts_minute)

                    # emit all the minutes before bts_minute
                    for minute, evt in self._get_minutes_for_list(
                            regular_minutes[0:bts_idx],
                            minute_emission
                    ):
                        yield minute, evt

                    yield bts_minute, BEFORE_TRADING_START

                    # emit all the minutes after bts_minute
                    for minute, evt in self._get_minutes_for_list(
                            regular_minutes[bts_idx:],
                            minute_emission):
                        yield minute, evt
                minute_dt = regular_minutes[-1]
                yield minute_dt, SESSION_END
            else:
                minute_dt = regular_minutes[0]
                if self._liquidate:
                    yield minute_dt, LIQUIDATE
                else:
                    yield minute_dt, STOP

    def _get_minutes_for_list(self, minutes, minute_emission):
        events_to_include = [BAR, MINUTE_END] if minute_emission else [BAR]
        for status in itertools.product(minutes, events_to_include):
            yield status

    def _calc_minutes_by_session(self, market_opens_nanos, market_closes_nanos, sessions_nanos):
        minutes_by_session = {}
        for session_idx, session_nano in enumerate(sessions_nanos):
            minutes_nanos = np.arange(
                market_opens_nanos[session_idx],
                market_closes_nanos[session_idx] + _nanos_in_minute,
                _nanos_in_minute
            )
            minutes_by_session[session_nano] = pd.to_datetime(
                minutes_nanos, utc=True, box=True
            )
        return minutes_by_session


class MinuteClockEngine(ClockEngine):
    def __init__(self, minute_emission=False):
        self._minute_emission = minute_emission

    def emission_rate(self):
        if self._minute_emission:
            return 'minute'
        return 'daily'


class MinuteSimulationClockEngine(MinuteClockEngine):
    def __init__(self, calendar, start_dt, end_dt,
                 minute_emission=False, minute_event_generator=None):
        super(MinuteSimulationClockEngine, self).__init__(minute_emission)
        """
        Parameters
        ----------
        calendar : trading_calendars.TradingCalendar
        start_dt : pandas.Timestamp
        end_dt : pandas.Timestamp
        
        """
        self._calendar = calendar
        self._start_dt = start_dt
        self._minute_emission = minute_emission
        self._egf = minute_event_generator if not None else MinuteEventGenerator()

        self._event_generator = self._egf.get_event_generator(
            calendar, self._start_dt, end_dt,
            datetime.combine(datetime.min, calendar.open_time) - timedelta(minutes=15)
        )

    def calendar_name(self):
        return self._calendar.name

    def stop(self, liquidate=False):
        self._egf.stop(liquidate)

    def calendar(self):
        return self._calendar

    def __iter__(self):
        for ts, evt in self._event_generator:
            yield ts, evt


# TODO: should have a way of modifying the calendar while the program is running...
# i.e: not changing the source code.
class MinuteRealtimeClockEngine(ClockEngine):
    def __init__(self, proto_calendar, ntp_server_address,
                 start_dt=None, minute_emission=True, minute_event_generator=None):
        super(MinuteRealtimeClockEngine, self).__init__(minute_emission)
        """
        
        Parameters
        ----------
        start_dt : pandas.Timestamp
        """
        self._calendar = None
        self._proto_calendar = proto_calendar
        # todo start_dt must be greater or equal to utc now
        self._start_dt = pd.Timestamp(pd.Timestamp.today().date(), tz='UTC') if start_dt is None else start_dt
        # the end_dt is the latest date of the calendar
        self._end_dt = None
        self._update = False

        self._egf = minute_event_generator if not None else MinuteEventGenerator(minute_emission)
        self._current_generator = None

        self._ntp_client = ntplib.NTPClient()
        self._ntp_server_address = ntp_server_address
        self._minute_counter = 0
        self._offset = 0

        self._first_call_flag = False
        self._time_idx = -1

        self._load_attributes(start_dt)

    def calendar_name(self):
        return self._cal_name

    def calendar_metadata(self):
        # returns the current calendar.
        #todo: if start dt and end dt are none, get the next "valid" session dt
        # valid: the start_dt is after or at today.
        return cl.CalendarMetadata(
            start=crv.to_datetime(self._start_dt),
            end=crv.to_proto_timestamp(self._end_dt),
            calendar=self._proto_calendar)

    def update_calendar(self, proto_calendar):
        self._proto_calendar = proto_calendar
        self._update = True

    def stop(self, liquidate=False):
        self._egf.stop(liquidate)

    def _get_seconds(self, time_delta):
        return time_delta.total_seconds()

    def __iter__(self):
        # todo: should yield an initialize, signal if haven't been done yet.
        ntp_stats = self._ntp_client.request(self._ntp_server_address)
        offset = ntp_stats.offset
        self._offset = offset

        sessions = self._calendar.all_sessions
        delta_seconds = self._get_seconds(
            pd.Timestamp(sessions[0]).tz_localize(tz='UTC') -
            pd.Timestamp.utcfromtimestamp(time.time() + offset)
        )

        if delta_seconds > 0:
            time.sleep(delta_seconds)
            return self._iter()

        elif delta_seconds < 0:
            # sleep until the next open day.
            t0 = time.time()
            start_ts = pd.Timestamp(sessions[1]).tz_localize(tz='UTC')
            self._load_attributes(self._calendar.all_sessions[1])
            time.sleep(
                self._get_seconds(
                    pd.Timestamp(start_ts) -
                    pd.Timestamp.utcfromtimestamp(time.time() + offset)) -
                time.time() - t0)
            return self._iter()
        else:
            # execute now
            return self._iter()

    def _iter(self):
        ntp_server_address = self._ntp_server_address
        minute_counter = -1
        #delay from update computation
        t1 = 0
        for ts, evt in self._current_generator:
            # for timing external computation
            t0 = time.time()
            ts = pd.Timestamp.utcfromtimestamp(time.time() + self._offset)
            if self._update:
                self._load_attributes(ts)
                self._update = False
                #store computation time for the update
                t1 = time.time() - t0
                # skip since we have a new generator
                continue
            yield ts, evt
            # update offset every 10 minutes
            minute_counter += 1
            if minute_counter == 10:
                minute_counter = 0
                ntp_stats = self._ntp_client.request(ntp_server_address)
                self._offset = ntp_stats.offset
            # we take into account the computation delay, so we don't "over-sleep"
            if evt == SESSION_START:
                # sleep until before trading starts
                self._time_idx += 1
                time.sleep(self._get_seconds(
                    self._calendar.opens[self._time_idx].tz_localize(tz='UTC') -
                    timedelta(minutes=15)) - ts - time.time() + t0 + t1)
            elif evt == SESSION_END:
                # sleep until next session start
                end_dt = self._end_dt
                if ts.date() == end_dt.date():
                    # re-load all attributes
                    self._load_attributes(end_dt + pd.Timedelta('1 day'))
                    # yield a calendar event so that the clients may load a new calendar
                    yield ts, CALENDAR
                    time.sleep(self._calendar.all_sessions[1] - ts - time.time() + t0 + t1)
                else:
                    time.sleep(
                        self._get_seconds(self._calendar.all_sessions[self._time_idx + 1]) - ts - time.time() + t0 + t1)
            elif evt == BAR:
                # sleep for a minute
                time.sleep(60 - time.time() + t0 + t1)

    def _load_attributes(self, start_dt):
        self._start_dt = start_dt

        self._calendar = cal = cu.from_proto_calendar(self._proto_calendar, start_dt)

        self._end_dt = end_dt = cal.all_sessions[-1]

        self._time_idx = -1

        self._current_generator = self._egf.get_event_generator(
            cal, start_dt, end_dt,
            datetime.combine(datetime.min, cal.open_time) - timedelta(minutes=15)
        )


class SimulationClock(cl_servicer.SimulationClockServicer):
    def __init__(self, proto_calendar, emission_rate, start_dt, end_dt, certificate=None):
        self._observers = {}
        self._certificate = certificate
        self._calendar = cal = cu.from_proto_calendar(proto_calendar, start_dt, end_dt)
        self._engine = MinuteSimulationClockEngine(cal, start_dt, end_dt, emission_rate==cl.MINUTE)
        self._proto_calendar = proto_calendar
        self._start_dt = start_dt
        self._end_dt = end_dt

    def Register(self, request, context):
        url = request.url
        observers = self._observers
        observers[url] = cl_servicer.ClockClientStub(srv.create_channel(url, self._certificate))
        return cl.Attributes(
            calendar_metadata=cl.CalendarMetadata(
                start=crv.to_proto_timestamp(self._start_dt),
                end=crv.to_proto_timestamp(self._end_dt),
                calendar=self._proto_calendar
            ))

    def Run(self, request, context):
        listeners = self._observers.values()
        for ts, evt in self._engine:
            # notify observers with the new event
            clock_event = cl.ClockEvent(
                crv.to_proto_timestamp(ts),
                event=evt
            )
            #todo: we should call these in parallel
            for listener in listeners:
                listener.Update(clock_event)

    def GetState(self, request, context):
        pass

class RealTimeClock(cl_servicer.RealtimeClockServicer):
    def __init__(self, clock_engine, certificate=None):
        """

        Parameters
        ----------
        clock_engine: ClockEngine
        """
        self._observers = {}
        self._engine = clock_engine
        self._certificate = certificate

    def Register(self, request, context):
        """

        Parameters
        ----------
        url : str
        Raises
        -------

        """

        listeners = self._observers
        url = request.url

        if url not in listeners:
            listeners[url] = cl_servicer.ClockClientStub(srv.create_channel(url, self._certificate))
        else:
            # todo: must send an error message to indicate that the the connection was refused
            # todo: what if the listener is not alive anymore?
            pass

        return cl.Attributes(calendar_metadata=self._engine.proto_calendar)

    def Run(self, request, context):
        listeners = self._observers.values()
        for ts, evt in self._engine:
            if evt == CALENDAR or evt == INITIALIZE:
                for listener in listeners:
                    listener.CalendarUpdate(self._engine.proto_calendar)
            clock_event = cl.ClockEvent(
                crv.to_proto_timestamp(ts),
                event=evt
            )
            # notify observers with the new event
            for listener in listeners:
                listener.Update(clock_event)

class SimulationClockRouter(cl_servicer.SimulationClockRouterServicer):
    def __init__(self, calendar_factories, base_address=None, certificate=None):
        self._clocks = {}
        self._factories = calendar_factories
        self._base_address = base_address
        self._certificate = certificate

    def Register(self, request, context):
        # for simulations, we classify each clock by id, so that two process with the same session_id
        # can observe the same simulation clock.
        clocks = self._clocks
        sess_id = request.session_id
        if sess_id not in clocks:
            address = self._base_address
            server = grpc.server(futures.ThreadPoolExecutor(10))
            clock = SimulationClock(MinuteSimulationClockEngine(
                self._factories[request.name].get_proto_calendar()),
                request.emission_rate,
                crv.to_datetime(request.start),
                crv.to_datetime(request.end))
            cl_servicer.add_SimulationClockServicer_to_server(clock, server)
            clocks[sess_id] = stub = cl_servicer.SimulationClockStub(
                address + '{}'.format(server.add_insecure_port(address + ':0')))
            stub.Register(request, context)
        else:
            clocks[sess_id].Register(request, context)

    def Run(self, request, context):
        self._clocks[request.session_id].Run(emp.Empty())


class RealtimeClockRouter(cl_servicer.RealtimeClockRouterServicer):
    def __init__(self, calendar_factories, base_address=None, certificate=None):
        """
        This class controls the clocks: generates its own urls to communicate with the clock
        Parameters
        ----------
        certificate
        """
        self._clocks = {}
        self._factories = calendar_factories
        self._running = False
        self._listeners = {}
        self._certificate = certificate
        self._address = base_address if base_address else 'localhost'
        self._servers = []

    def add_clock(self, calendar_name, clock, server):
        cl_servicer.add_ClockServerServicer_to_server(clock, server)
        self._clocks[calendar_name] = clock


    def _load_calendar(self, name):
        """

        Parameters
        ----------
        name

        Returns
        -------
        trading_calendars.TradingCalendar

        """
        return

    def stop(self, grace=None):
        for server in self._servers:
            server.stop(grace)

    def Register(self, request, context):
        # todo: clocks should run as sub-processes and we register each client to the clock
        # by exchange name.
        """
        Notes
        -----
        The registration spawns a clock as a sub-process. Each clock is a server and is controlled
        by the clock servicer (gateway). The servicer communicates with the other clocks through
        messaging

        Parameters
        ----------
        request
        context

        Returns
        -------

        """

        name = request.name
        clocks = self._clocks
        base_address = self._address
        if name not in clocks:
            server = grpc.server(futures.ThreadPoolExecutor(10))
            clock = RealTimeClock(MinuteRealtimeClockEngine(
                self._factories[name],
                request.emission_rate == cl.MINUTE), self._certificate)
            cl_servicer.add_ClockServerServicer_to_server(
                clock,
                server
            )
            self._servers.append(server)
            clocks[name] = stub = cl_servicer.ClockServerStub(
                grpc.insecure_channel(base_address + ':{}'.format(server.add_insecure_port(base_address + ':0'))))
            server.start()
            # run the clock at the first registration
            stub.Run(request)
            stub.Register(request)
        else:
            #register to an already running clock...
            self._clocks[name].Register(request)
