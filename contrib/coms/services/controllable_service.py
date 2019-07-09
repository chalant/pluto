from protos import controller_service_pb2 as ctl_msg, controllable_service_pb2_grpc as cbl_grpc, \
    controller_service_pb2_grpc as ctl_srv
from contrib.control.clock import clock_pb2 as cl_msg

from contrib.coms.client import account

from contrib.gens import control

from contrib.coms.utils import server_utils as srv

from contrib.finance.metrics import tracker

from contrib.trading_calendars.calendar_utils import from_proto_calendar

from contrib.coms.utils import conversions as crv

#todo between lifetimes, the controllable must store it's name. so that it can be identified by the
# server, we need a unique name, since the url might change between lifetimes.
class ControllableServicer(cbl_grpc.ControllableServicer):
    def __init__(self, strategy, channel, address, certificate_auth=None):
        self._strategy = strategy
        self._stub = ctl_srv.ControllerStub(channel)
        #register to the controller
        creds = self._stub.Register(ctl_msg.Identity(name=strategy.name, url=address))
        self._worker = ctl_srv.WorkerStub(srv.create_channel(creds.url, certificate_auth))
        self._broker = broker = account.BrokerClient(channel, creds.token)
        self._metrics_tracker = tracker.MetricsTracker(broker)
        #register to the controller.

        self._initialized = False
        #storage_path.
        self._state_storage_path = ""

    def ClockUpdate(self, request, context):
        evt = request.clock_event.event
        dt = request.clock_event.dt
        metrics_tracker = self._metrics_tracker
        bundler = self._bundler
        calendar = self._calendar

        control = self._control
        worker = self._worker

        if evt == cl_msg.INITIALIZE:
            control.on_initialize(
                dt, metrics_tracker, bundler,
                self._calendar, self._capital, self._state_storage_path
            )

        elif evt == cl_msg.SESSION_START:
            if not self._initialized:
                control.on_initialize(dt, metrics_tracker)
            control.on_session_start(dt, bundler, calendar, self._emission_rate, metrics_tracker)

        elif evt == cl_msg.SESSION_END:
            #send performance packet to controller.
            worker.ReceivePerformancePacket(crv.to_proto_performance_packet(control.on_session_end(dt)))

        elif evt == cl_msg.BAR:
            control.on_bar(dt, metrics_tracker, calendar, bundler, self._broker)

        elif evt == cl_msg.MINUTE_END:
            #send performance packet to the controller
            worker.ReceivePerformancePacket(crv.to_proto_performance_packet(control.on_minute_end(dt, metrics_tracker)))

        elif evt == cl_msg.STOP:
            control.on_stop(dt, metrics_tracker, self._state_storage_path)

        elif evt == cl_msg.LIQUIDATE:
            control.on_liquidate(dt, metrics_tracker)

    def UpdateCalendar(self, request, context):
        """Called if the calendar needs to be updated."""
        self._calendar = from_proto_calendar(
            request.calendar,
            crv.to_pandas_timestamp(request.start),
            crv.to_pandas_timestamp(request.end))

    def UpdateParameters(self, request, context):
        self._control.set_capital_target(request.capital)

    def UpdateBroker(self, request, context):
        """Called regularly before the clock update."""
        self._broker.update(request)

    def ReceiveDataBundle(self, request, context):
        #todo: the data bundle should be an "environment" for the strategy developer.
        # this means that if we had an IDE for this, the developer should describe the environment and
        # will receive a data bundle before running the strategy. This way he can get a clear view of the
        # available data of the chosen environment.
        #todo: ingest and construct classes that reflects the bundle data.
        self._bundler = None

    def Initialize(self, request, context):
        """First function called by the controller. Called once per-lifetime"""
        self._emission_rate = request.emission_rate
        self._data_frequency = request.data_frequency
        self._capital = request.capital
        self._max_leverage = request.maximum_leverage

        self._control = control.AlgorithmController(self._strategy, request.benchmark_asset)