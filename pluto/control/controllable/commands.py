import abc

from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START
)

from pluto.coms.utils import conversions


class Command(abc.ABC):
    __slots__ = ['request']

    def __init__(self, request):
        self._request = request

    def __call__(self):
        self._execute(self._request)

    @property
    def dt(self):
        return self._request.dt

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
    __slots__ = ['_perf_writer', '_controllable', '_frequency_filter', '_state_store']

    def __init__(self, perf_writer, controllable, frequency_filter, request, state_store):
        '''

        Parameters
        ----------
        perf_writer
        controllable
        frequency_filter
        state
        request
        state_store
        '''
        super(ClockUpdate, self).__init__(request)
        self._perf_writer = perf_writer
        self._controllable = controllable
        self._frequency_filter = frequency_filter
        self._state_store = state_store

    def _execute(self, request):
        # todo: what about capital updates etc? => each request is bound to a function
        # ex:
        evt = request.event
        dt = conversions.to_datetime(request.timestamp)
        signals = request.signals
        controllable = self._controllable

        s = controllable.state.aggregate(dt, evt, signals)

        if s:
            writer = self._perf_writer

            # todo: exchanges should be filtered in the here
            ts, e, exchanges = s
            # exchanges will be used to filter the assets and the resulting assets will
            # be used to filter data
            # only run when the observed exchanges are active
            dt = conversions.to_datetime(ts)
            if e == SESSION_START:
                controllable.session_start(dt)
            elif e == BEFORE_TRADING_START:
                controllable.before_trading_starts(dt)
            elif e == SESSION_END:
                # todo: we need to identify the controllable (needs an id)
                # send performance packet to controller.
                # todo: write the performance in a file (don't sent it back to the controller)
                # or send back performance and write to file
                writer.performance_update(controllable.session_end(dt))

            elif e == MINUTE_END:
                # todo: we need to identify the controllable (needs an id)
                # send performance packet to the controller
                # todo: write the performance in a file (don't sent it back to the controller)
                writer.performance_update(controllable.minute_end(dt))
            else:
                # TRADE_END/BAR event
                targets = self._frequency_filter.filter(exchanges)
                if targets:
                    # note: in daily mode, this can still be called more than once (if it is
                    # a different exchange)
                    controllable.bar(dt)

                # todo: non-blocking!
                # todo: PROBLEM: we might have some conflicts in state, since we could have
                # multiple controllables with the same session_id running in different
                # modes...
                # todo: store state
                # todo: store the controllable state
                self._state_store.store(dt, controllable)
