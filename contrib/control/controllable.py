from abc import abstractmethod

import pandas as pd

from google.protobuf import empty_pb2 as emp

from contrib.coms.utils import conversions
from contrib.trading_calendars import calendar_utils as cu
from contrib.coms.client import account

import grpc

import click
from . import domain

from protos import controllable_pb2_grpc as ctbl_rpc
from protos.clock_pb2 import (
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

#TODO: Two multiple types of controllables: a simulation controllable runs its own clock
# for performance reasons.


class Controllable(ctbl_rpc.ControllableServicer):
    def __init__(self, server):
        self._calendar = None
        ctbl_rpc.add_ControllableServicer_to_server(self, server)
        self._domain = None

    def _with_metadata(self, rpc, params):
        '''If we're not registered, an RpcError will be raised. Registration is handled
        externally.'''
        return rpc(params, metadata=(('Token', self._token)))

    def Initialize(self, request, context):
        self._domain = domain.compute_domain(request.domain)

    def Update(self, request, context):
        '''Note: an update call might arrive while the step is executing..., so
        we must queue the update message... => the step must be a thread that pulls data
        from the queue...
        '''
        #todo: this shouldn't block an empty message must be sent right-away!
        self._update(
            pd.Timestamp(conversions.to_datetime(request.timestamp)).tz_localize('UTC'),
            request.event,
            self._calendar,
            request.broker_state
        )

    @abstractmethod
    def _update(self, dt, event, calendar, broker_state):
        raise NotImplementedError

    def UpdateCalendar(self, request, context):
        self._calendar = cu.TradingCalendar(
            pd.Timestamp(conversions.to_datetime(request.start)).tz_localize('UTC').normalize(),
            pd.Timestamp(conversions.to_datetime(request.end)).tz_localize('UTC').normalize(),
            request.calendar)

@click.group()
def cli():
    pass

@cli.command()
@click.argument('server_url')
@cli.argument('url')
def register(server_url, url):
    #TODO: run the controllable server
    pass

if __name__ == '__main__':
    cli()