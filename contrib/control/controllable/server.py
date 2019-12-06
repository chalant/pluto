
import threading
import signal
from abc import ABC, abstractmethod
from concurrent import futures

import grpc

from google.protobuf import empty_pb2 as emp

from contrib.coms.utils import conversions
from contrib.trading_calendars import calendar_utils as cu
from contrib.coms.client import account
from contrib.coms.utils import conversions as crv
from contrib.control.controllable import states as st

from protos import controllable_pb2
from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    BAR,
    MINUTE_END,
    TRADE_END,
    BEFORE_TRADING_START
)

import click

from protos import controllable_pb2_grpc as cbl_rpc

class FrequencyFilter(ABC):
    @abstractmethod
    def filter(self, evt_exc_pairs):
        raise NotImplementedError

class DayFilter(FrequencyFilter):
    def filter(self, evt_exc_pairs):
        exchanges = []
        for evt, exc in evt_exc_pairs:
            if evt == TRADE_END:
                exchanges.append(exc)
        return exchanges

class MinuteFilter(FrequencyFilter):
    def filter(self, evt_exc_pairs):
        exchanges = []
        for evt, exc in evt_exc_pairs:
            if evt == TRADE_END or evt == BAR:
                exchanges.append(exc)
        return exchanges

class Servicer(cbl_rpc.ControllableServicer):
    def __init__(self, controller_url, controllable):
        self._calendar = None
        self._controllable = controllable
        self._controller = None
        self._ctl_url = controller_url

        self._session = None

        self._start_flag = False
        self._session_start = False
        self._started = False

        self._bfs_flag = False
        self._frequency_filter = None

        self._exchanges = None
        self._num_exchanges = None

        self._session_end = []

        self._out_session = st.OutSession(self)
        self._active = st.Active(self)
        self._in_session = st.InSession(self)
        self._bfs = st.BFS(self)
        self._idle = idle = st.Idle(self)

        self._state = idle

    #todo need to use an interceptor instead
    def _with_metadata(self, rpc, params):
        '''If we're not registered, an RpcError will be raised. Registration is handled
        externally.'''
        return rpc(params, metadata=(('Token', self._token)))

    @property
    def out_session(self):
        return self._out_session

    @property
    def active(self):
        return self._active

    @property
    def in_session(self):
        return self._in_session

    @property
    def bfs(self):
        return self._bfs

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    @property
    def exchanges(self):
        return self._exchanges

    def initialize(self, request_iterator, context):
        #we can create a calendar based on these exchanges...
        #todo de-serialize the request into an InitializeParams object
        b = b''
        for bytes_ in request_iterator:
            b += bytes_

        params = controllable_pb2.InitParams()
        params.ParseFromString(b)

        id_ = params.id #the id of the controllable => will be used in performance updates
        exchanges = params.exchanges
        capital = params.capital
        max_leverage = params.max_leverage
        calendar = params.calendar #todo: instanciate the calendar from the message
        strategy = params.strategy
        data_frequency = params.data_frequency

        if data_frequency ==  'day':
            self._frequency_filter = DayFilter()
        elif data_frequency == 'minute':
            self._frequency_filter = MinuteFilter()

        self._exchanges = exchanges
        self._num_exchanges = len(exchanges)


        controllable = self._controllable
        controllable.initialize(
            dt, calendar, strategy,
            capital, max_leverage,
            benchmark_asset, restrictions)

        self._state = self._out_session
        return emp.Empty()

    def stop(self, request, context):
        #todo
        return emp.Empty()

    def update_parameters(self, request, context):
        self._controllable.update_parameters(request)
        return emp.Empty()

    def update_account(self, request_iterator, context):
        self._controllable.update_account(request_iterator)
        return emp.Empty()

    def update_data_bundle(self, request_iterator, context):
        self._controllable.update_data_bundler(request_iterator)
        return emp.Empty()

    def clock_update(self, request, context):
        '''Note: an update call might arrive while the step is executing..., so
        we must queue the update message... => the step must be a thread that pulls data
        from the queue...
        '''

        #todo: in daily mode, we should simulate the slippage on the current bar,
        # after placing orders. we can set this as a parameter on each run
        # call "get_transaction" on the blotter, after bar event.
        #todo: we could encapsulate this behavior, using a controllable subclass
        # NOTE: use FixedBasisPointsSlippage for slippage simulation.

        evt = request.clock_event.event
        dt = request.clock_event.dt
        signals = request.signals

        s = self._state.aggregate(dt, evt, signals)

        if s:
            controllable = self._controllable
            controller = self._controller
            ts, e, exchanges = s
            #exchanges will be used to filter the assets and the resulting assets will
            # be used to filter data
            #only run when the observed exchanges are active
            if e == SESSION_START:
                controllable.session_start(dt, [exc for evt, exc in exchanges])

            elif e == SESSION_END:
                #todo: we need to identify the controllable (needs an id)
                # send performance packet to controller.
                controller.PerformancePacketUpdate(
                    crv.to_proto_performance_packet(
                        controllable.session_end(dt)))

            elif e == MINUTE_END:
                #todo: we need to identify the controllable (needs an id)
                # send performance packet to the controller
                controller.PerformancePacketUpdate(
                    crv.to_proto_performance_packet(
                        controllable.on_minute_end(
                            dt, [exc for evt, exc in exchanges])))
            else:
                #TRADE_END/BAR event
                targets = self._frequency_filter.filter(exchanges)
                if targets:
                    #note: in daily mode, this can still be called more than once (if it is
                    # different exchange)
                    controllable.bar(dt, targets)
        return emp.Empty()

    def _update(self, dt, event, calendar, broker_state):
        raise NotImplementedError

    def update_calendar(self, request_iterator, context):
        self._calendar = self._update_calendar(
            conversions.to_datetime(request.start).tz_localize('UTC').normalize(),
            conversions.to_datetime(request.end).tz_localize('UTC').normalize(),
            calendar)
        return emp.Empty()

    def _update_calendar(self, start_dt, end_dt, proto_calendar):
        return cu.TradingCalendar(start_dt, end_dt, proto_calendar)

class Server(object):
    def __init__(self):
        self._event = threading.Event()
        self._server = grpc.server(futures.ThreadPoolExecutor(10))

    def start(self, url, controllable):
        server = self._server
        server.add_insecure_port(url)
        cbl_rpc.add_ControllableServicer_to_server(controllable, server)
        server.start()
        self._event.wait()
        controllable.stop()
        server.stop()

    def stop(self):
        self._event.set()

_SERVER = Server()

def get_controllable(mode):
    return

def termination_handler(signum, frame):
    _SERVER.stop()

def interruption_handler(signum, frame):
    _SERVER.stop()

signal.signal(signal.SIGINT, interruption_handler)
signal.signal(signal.SIGTERM, termination_handler)

@click.group()
def cli():
    pass

@cli.command()
@click.argument('controller_url')
@cli.argument('controllable_url')
@cli.argument('mode')
def start(mode, controller_url, controllable_url):
    _SERVER.start(controllable_url, Servicer(controller_url, get_controllable(mode)))

if __name__ == '__main__':
    cli()
