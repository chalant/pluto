import abc

from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START
)

from pluto.coms.utils import conversions as crv


class Command(abc.ABC):
    __slots__ = ['request']

    def __init__(self, request):
        self._request = request

    def __call__(self):
        self._execute(self._request)

    @abc.abstractmethod
    def _execute(self, request):
        raise NotImplementedError('{}'.format_map(self._execute.__name__))

class CapitalUpdate(Command):
    __slots__ = ['_controllable']
    def __init__(self, controllable, request):
        super(CapitalUpdate, self).__init__(request)
        self._controllable = controllable

    def _execute(self, request):
        raise NotImplementedError


class ClockUpdate(Command):
    __slots__ = ['_controller', '_controllable', '_frequency_filter', '_state']

    def __init__(self, controller, controllable, frequency_filter, state, request):
        super(ClockUpdate, self).__init__(request)
        self._controller = controller
        self._controllable = controllable
        self._state = state
        self._frequency_filter = frequency_filter

    def _execute(self, request):
        # todo: what about capital updates etc? => each request is bound to a function
        # ex:
        evt = request.clock_event.event
        dt = request.clock_event.dt
        signals = request.signals

        s = self._state.aggregate(dt, evt, signals)

        if s:
            controllable = self._controllable
            controller = self._controller

            # todo: exchanges should be filtered in the here
            ts, e, exchanges = s
            # exchanges will be used to filter the assets and the resulting assets will
            # be used to filter data
            # only run when the observed exchanges are active
            if e == SESSION_START:
                controllable.session_start(dt)
            elif e == BEFORE_TRADING_START:
                controllable.before_trading_starts(dt)

            elif e == SESSION_END:
                # todo: we need to identify the controllable (needs an id)
                # send performance packet to controller.
                controller.PerformancePacketUpdate(
                    crv.to_proto_performance_packet(
                        controllable.session_end(dt)))

            elif e == MINUTE_END:
                # todo: we need to identify the controllable (needs an id)
                # send performance packet to the controller
                controller.PerformancePacketUpdate(
                    crv.to_proto_performance_packet(
                        controllable.minute_end(
                            dt)))
            else:
                # TRADE_END/BAR event
                targets = self._frequency_filter.filter(exchanges)
                if targets:
                    # note: in daily mode, this can still be called more than once (if it is
                    # a different exchange)
                    controllable.bar(dt)
                # todo: store the controllable state
                # state = controllable.get_state()
