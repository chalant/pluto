from pluto.finance.broker import broker_stub

from pluto.control import phases
from pluto.control.controllable import server
from pluto.control.controllable.utils import factory
from pluto.control.controllable import simulation_controllable
from pluto.control.controllable import live_controllable
from pluto.control.controllable import stub

from pluto.interface import monitor

from pluto.interface.utils import service_access

from protos import broker_pb2


class FakeContext(object):
    __slots__ = ['_invocation_metadata', '_status_code']

    def __init__(self, metadata=None):
        self._invocation_metadata = metadata if metadata else ()
        self._status_code = ''

    def invocation_metadata(self):
        return self._invocation_metadata

    def set_code(self, status_code):
        self._status_code = status_code

    def set_details(self, message):
        raise RuntimeError(
            self._status_code,
            message)

    def abort(self, status_code, message):
        raise RuntimeError(
            status_code,
            message)


def _unchunk(request_iterator):
    return b''.join(chunk.data for chunk in request_iterator)


class BrokerStub(broker_stub.BrokerStub):
    def __init__(self, servicer, session_id):
        '''

        Parameters
        ----------
        servicer: pluto.control.controller.process_manager.StrategyManager
        '''
        self._servicer = servicer
        self._session_id = session_id

    def _publish(self, topic, message, timestamp):
        # the runtime sends its orders and watchlist to the controller side on the bus of the run, in the
        # trade phase of the clock event that made them (the phase's ``done`` is the endpoint's)
        self._servicer.bus.publish(
            topic,
            message,
            type=phases.message_type(message),
            logical_ts=timestamp.ToNanoseconds(),
            producer_id=self._session_id,
            phase=phases.TRADE.rank,
            key=self._session_id)

    def _execute_cancel_policy(self, request, metadata):
        return self._servicer.ExecuteCancelPolicy(
            request,
            FakeContext(metadata))

    def _place_orders(self, request_iterator, metadata):
        batch = broker_pb2.OrderBatch()
        batch.ParseFromString(_unchunk(request_iterator))

        self._publish(phases.ORDERS_TOPIC, batch, batch.ts)

    def _cancel_all_orders_for_asset(self, request, metadata):
        return self._servicer.CancelAllOrdersForAsset(
            request,
            FakeContext(metadata))

    def _cancel_order(self, request, metadata):
        return self._servicer.CancelOrder(
            request,
            FakeContext(metadata))

    def _stopped(self, request, metadata):
        return self._servicer.Stopped(
            request,
            FakeContext(metadata))

    def _set_watchlist(self, request, metadata):
        watchlist = broker_pb2.WatchList()
        watchlist.ParseFromString(_unchunk(request))

        self._publish(phases.WATCHLIST_TOPIC, watchlist, watchlist.ts)


class InMemoryControllableFactory(factory.ControllableFactory):
    def __init__(self, broker):
        self._broker = broker

    def _create_broker(self, session_id):
        return BrokerStub(self._broker, session_id)

    def _create_controllable(self, params):
        if params.mode == 'backtesting':
            return simulation_controllable.SimulationControllable()
        elif params.mode == 'live':
            return live_controllable.LiveControllable()
        else:
            raise ValueError('Mode: {} not supported'.format(params.mode))

# todo: perform handle some stuff this side... like updating broker etc.


class InMemoryControllableStub(stub.ControllableStub):
    def __init__(self, directory, mode, broker, running_behavior):
        # creator monitor service
        mon = monitor.Monitor()

        self._monitor = mon

        self._servicer = server.ControllableService(
            directory,
            InMemoryMonitorStub(mon),
            InMemoryControllableFactory(broker),
            mode,
            running_behavior)

    def _run(self, request, metadata):
        return self._servicer.Run(request, FakeContext(metadata))

    def _initialize(self, request, metadata=None):
        return self._servicer.Initialize(
            request,
            FakeContext(metadata))

    def _update_parameters(self, request, metadata):
        return self._servicer.UpdateParameters(
            request,
            FakeContext(metadata))

    def _clock_update(self, request, metadata):
        return self._servicer.ClockUpdate(
            request,
            FakeContext(metadata))

    def _stop(self, request, metadata):
        return self._servicer.Stop(
            request,
            FakeContext(metadata))

    def _update_account(self, request, metadata):
        return self._servicer.UpdateAccount(
            request,
            FakeContext(metadata))

    def _watch(self, request, metadata):
        return self._servicer.Watch(
            request,
            FakeContext(metadata))

    def _stop_watching(self, request, metadata):
        return self._servicer.StopWatching(
            request,
            FakeContext(metadata))


class InMemoryMonitorStub(object):
    def __init__(self, monitor_servicer):
        self._monitor_server = monitor_servicer

    @service_access.framework_method
    def Watch(self, request):
        return self._monitor_server.Watch(
            request,
            FakeContext())

    @service_access.framework_method
    def StopWatching(self, request):
        return self._monitor_server.StopWatching(
            request,
            FakeContext())

    @service_access.framework_method
    def PerformanceUpdate(self, request_iterator, metadata=None):
        return self._monitor_server.PerformanceUpdate(
            request_iterator,
            FakeContext(metadata))


