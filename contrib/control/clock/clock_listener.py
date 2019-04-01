import pandas as pd

from google.protobuf import empty_pb2 as emp

from contrib.control.clock import clock_pb2_grpc as cl_grpc

from contrib.coms.utils import conversions
from contrib.coms.utils import server_utils as srv
from contrib.trading_calendars import calendar_utils as cu


class ClockListener(cl_grpc.ClockClientServicer):
    def __init__(self, listener, channel, address, calendar_name):
        self._listener = listener
        self._stub = cl_grpc.ClockServerStub(channel)
        self._register(address, calendar_name)

    def _register(self, address, calendar_name):
        attributes = self._stub.Register(url=address, calendar_name=calendar_name)
        self._emission_rate = attributes.emission_rate

    def Update(self, request, context):
        self._listener.update(
            pd.Timestamp(conversions.to_datetime(request.timestamp)).tz_localize('UTC'),
            request.event,
            self._calendar
        )

    def CalendarUpdate(self, request, context):
        self._calendar = cu.TradingCalendar(
            pd.Timestamp(conversions.to_datetime(request.start)).tz_localize('UTC').normalize(),
            pd.Timestamp(conversions.to_datetime(request.end)).tz_localize('UTC').normalize(),
            request.calendar)


def register_clock_listener(server, clock_server_address, clock_client_address,
                            listener, calendar_name, certificate_authority=None):
    """

    Parameters
    ----------
    server :
    clock_server_address :
    clock_client_address :
    listener :
    calendar_name : str
    certificate_authority :
    """

    cl_grpc.add_ClockClientServicer_to_server(
        ClockListener(
            listener,
            srv.create_channel(clock_server_address, certificate_authority),
            clock_client_address,
            calendar_name),
        server)
