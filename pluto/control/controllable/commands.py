import abc

from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START
)

from pluto.coms.utils import conversions

class StopExecution(Exception):
    pass

class Command(abc.ABC):
    __slots__ = ['_request', '_controllable', '_dt']

    def __init__(self, controllable, request):
        self._controllable = controllable
        self._request = request
        self._dt = conversions.to_datetime(request.real_ts)

    def __call__(self):
        controllable = self._controllable
        request = self._request
        controllable.real_dt = self._dt
        self._execute(controllable, request)

    @property
    def dt(self):
        return self._dt

    @abc.abstractmethod
    def _execute(self, controllable, request):
        '''

        Parameters
        ----------
        controllable: pluto.control.controllable.controllable.Controllable
        request

        Returns
        -------

        '''
        raise NotImplementedError('{}'.format_map(self._execute.__name__))

class Stop(Command):
    def _execute(self, controllable, request):
        pass

class CapitalUpdate(Command):
    def __init__(self, controllable, request):
        super(CapitalUpdate, self).__init__(controllable, request)

    def _execute(self, controllable, request):
        raise NotImplementedError

class AccountUpdate(Command):
    def __init__(self, controllable, request):
        super(AccountUpdate, self).__init__(controllable, request)

    def _execute(self, controllable, request):
        controllable.update_blotter(request)

class ClockUpdate(Command):
    __slots__ = [
        '_perf_writer',
        '_controllable',
        '_frequency_filter',
        '_state_store']

    def __init__(self,
                 perf_writer,
                 controllable,
                 frequency_filter,
                 request,
                 state_store):
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
        super(ClockUpdate, self).__init__(controllable, request)
        self._perf_writer = perf_writer
        self._frequency_filter = frequency_filter
        self._state_store = state_store

    def _execute(self, controllable, request):
        evt = request.event
        dt = conversions.to_datetime(request.timestamp)
        signals = request.signals

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
                packet, end = controllable.session_end(dt)
                writer.performance_update(packet, end)
                if end:
                    raise StopExecution
            elif e == MINUTE_END:
                packet, end = controllable.minute_end(dt)
                writer.performance_update(packet, end)
                if end:
                    raise StopExecution
            else:
                # TRADE_END/BAR event
                targets = self._frequency_filter.filter(exchanges)
                if targets:
                    # note: in daily mode, this can still be called more than once (if it is
                    # a different exchange)
                    controllable.bar(dt)

                #store state at each bar event, since it only changes on bar/trade_end events

                # todo: non-blocking!
                # todo: PROBLEM: we might have some conflicts in state, since we could have
                # multiple controllables with the same session_id running in different
                # modes...
                # todo: store state
                # todo: store the controllable state
                self._state_store.store(dt, controllable)
