import abc

from google.protobuf import empty_pb2

from pluto.coms.utils import conversions

from protos.clock_pb2 import (
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START,
    TRADE_END,
    BAR
)

from protos import broker_pb2


class StopExecution(Exception):
    pass


class Command(abc.ABC):
    __slots__ = ['_request', '_controllable', '_dt']

    def __init__(self, controllable, request):
        self._controllable = controllable
        self._request = request
        self._dt = conversions.to_datetime(request.timestamp)

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
        raise StopExecution


class ParametersUpdate(Command):
    def _execute(self, controllable, request):
        controllable.capital = request.capital
        controllable.max_leverage = request.max_leverage


class AccountUpdate(Command):
    def _execute(self, controllable, request):
        controllable.update_blotter_and_positions(request)


class ClockUpdate(Command):
    __slots__ = [
        '_perf_writer',
        '_controllable',
        '_frequency_filter',
        '_state_store',
        '_flag',
        '_broker']

    def __init__(self,
                 perf_writer,
                 controllable,
                 frequency_filter,
                 request,
                 state_store):
        """

        Parameters
        ----------
        perf_writer
        controllable
        frequency_filter
        request
        state_store
        """
        super(ClockUpdate, self).__init__(controllable, request)
        self._perf_writer = perf_writer
        self._frequency_filter = frequency_filter
        self._state_store = state_store
        self._flag = False
        self._broker = controllable.broker

    def _execute(self, controllable, request):
        broker = self._broker

        batch = broker_pb2.OrderBatch(
            ts=request.timestamp,
            ended=False)

        # batch.not_running = 0
        s = controllable.clock.update(conversions.to_datetime(request.timestamp))

        if s:
            writer = self._perf_writer

            ts, e = s

            if e == SESSION_START:
                controllable.session_start(ts)

            elif e == BEFORE_TRADING_START:
                controllable.before_trading_starts(ts)

            elif e == SESSION_END:
                packet, end = controllable.session_end(ts)
                writer.performance_update(packet, end)

                controllable.ended = end

            elif e == MINUTE_END:
                packet, end = controllable.minute_end(ts)
                writer.performance_update(packet, end)
                controllable.ended = end

            else:
                # send orders to broker.

                # TRADE_END/BAR event
                if self._frequency_filter.filter(e):
                    blotter = controllable.bar(ts)

                    for order in blotter.new_orders:

                        batch.orders.append(
                            conversions.to_proto_order(order.to_dict()))

                    blotter.new_orders.clear()

                # store state at each bar event, since it only changes on bar/trade_end events

                # todo: non-blocking!
                # todo: PROBLEM: we might have some conflicts in state, since we could have
                # multiple controllables with the same session_id running in different
                # modes...
                # todo: store state
                # todo: store the controllable state
                self._state_store.store(ts, controllable)

        # print("Placed orders", request.timestamp)
        # if request.event == BAR or request.event == TRADE_END:
        broker.PlaceOrders(batch.SerializeToString(), ())

        if controllable.ended:
            controllable.not_running = 1
            raise StopExecution
