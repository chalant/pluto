import threading
import grpc
import signal

from concurrent import futures

from google.protobuf import empty_pb2 as emp

from contrib.coms.utils import conversions
from contrib.trading_calendars import calendar_utils as cu
from contrib.coms.client import account
from contrib.coms.utils import conversions as crv

from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    BAR,
    MINUTE_END,
    STOP,
    LIQUIDATE
)

import click

from protos import controllable_pb2_grpc as cbl_rpc

#TODO: Two multiple types of controllables: a simulation controllable runs its own clock
# for performance reasons.


class Servicer(cbl_rpc.ControllableServicer):
    def __init__(self, controller_url, controllable):
        self._calendar = None
        self._controllable = controllable
        self._controller = None
        self._ctl_url = controller_url

    def _with_metadata(self, rpc, params):
        '''If we're not registered, an RpcError will be raised. Registration is handled
        externally.'''
        return rpc(params, metadata=(('Token', self._token)))

    def Initialize(self, request_iterator, context):
        controllable = self._controllable
        controllable.initialize(
            dt, calendar, strategy,
            capital, max_leverage,
            benchmark_asset, restrictions)
        return emp.Empty()

    def UpdateParameters(self, request, context):
        self._controllable.update_parameters(request)
        return emp.Empty()

    def UpdateAccount(self, request_iterator, context):
        self._controllable.update_account(request_iterator)
        return emp.Empty()

    def UpdateDataBundle(self, request_iterator, context):
        self._controllable.update_data_bundler(request_iterator)
        return emp.Empty()

    def ClockUpdate(self, request, context):
        '''Note: an update call might arrive while the step is executing..., so
        we must queue the update message... => the step must be a thread that pulls data
        from the queue...
        '''

        evt = request.clock_event.event
        dt = request.clock_event.dt

        controllable = self._controllable
        controller = self._controller

        if evt == SESSION_START:
            controllable.session_start(dt)
        elif evt == SESSION_END:
            # send performance packet to controller.
            controller.PerformancePacketUpdate(
                crv.to_proto_performance_packet(
                    controllable.session_end(dt)))

        elif evt == BAR:
            controllable.bar(dt)

        elif evt == MINUTE_END:
            # send performance packet to the controller
            controller.PerformancePacketUpdate(
                crv.to_proto_performance_packet(
                    controllable.on_minute_end(
                        dt,
                        metrics_tracker)))
        elif evt == STOP:
            controllable.stop(dt)
        elif evt == LIQUIDATE:
            controllable._liquidate(dt)
        return emp.Empty()

    def Stop(self, request, context):
        return emp.Empty()

    def stop(self):
        pass

    def _update(self, dt, event, calendar, broker_state):
        raise NotImplementedError

    def UpdateCalendar(self, request_iterator, context):
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
