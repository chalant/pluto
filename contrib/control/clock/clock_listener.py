import pandas as pd

from google.protobuf import empty_pb2 as emp

from contrib.control.clock import clock_pb2_grpc as cl_grpc
from contrib.control.clock import clock_pb2 as cl
from contrib.coms.utils import conversions
from contrib.coms.utils import server_utils as srv


class ClockListener(cl_grpc.ClockClientServicer):
    def __init__(self, listener, channel, address, calendar_name):
        self._listener = listener
        self._stub = cl_grpc.ClockServerStub(channel)
        self._register(address, calendar_name)

    def _load_calendar(self, proto_calendar):
        # todo: create a calendar instance from proto_calendar...
        return

    def _register(self, address, calendar_name):
        #todo: what if we get an error (like connection refused?)
        self._stub.Register(url = address, calendar_name=calendar_name)

    def Update(self, request, context):
        stub = self._stub
        if request.event == cl.Event.INITIALIZE:
            self._calendar = self._load_calendar(stub.GetCalendar(emp.Empty()))
            self._listener.update(
                pd.Timestamp(conversions.to_datetime(request.timestamp)).tz_localize('UTC'),
                request.event,
                self._calendar
            )
        elif request.event == cl.Event.CALENDAR:
            self._calendar = self._load_calendar(stub.GetCalendar(emp.Empty()))
        else:
            self._listener.update(
                pd.Timestamp(conversions.to_datetime(request.timestamp)).tz_localize('UTC'),
                request.event,
                self._calendar
            )

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



