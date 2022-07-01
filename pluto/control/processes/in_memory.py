from pluto.finance.broker import broker_stub

from pluto.control.controllable import server
from pluto.control.controllable.utils import factory
from pluto.control.controllable import simulation_controllable
from pluto.control.controllable import live_controllable
from pluto.control.controllable import stub

from pluto.interface import monitor

from pluto.interface.utils import service_access


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


class BrokerStub(broker_stub.BrokerStub):
    def __init__(self, servicer, session_id):
        '''

        Parameters
        ----------
        servicer: pluto.broker.broker_service.BrokerService
        '''
        self._servicer = servicer
        self._session_id = session_id

    def _execute_cancel_policy(self, request, metadata):
        return self._servicer.ExecuteCancelPolicy(
            request,
            FakeContext(metadata))

    def _place_orders(self, request_iterator, metadata):
        return self._servicer.PlaceOrders(
            request_iterator,
            FakeContext(metadata))

    def _cancel_all_orders_for_asset(self, request, metadata):
        return self._servicer.CancelAllOrdersForAsset(
            request,
            FakeContext(metadata))

    def _cancel_order(self, request, metadata):
        return self._servicer.CancelOrder(
            request,
            FakeContext(metadata))


class InMemoryControllableFactory(factory.ControllableFactory):
    def __init__(self, broker_service=None):
        self._broker_service = broker_service

    def _create_controllable(self, params):
        if params.mode == 'backtesting':
            return simulation_controllable.SimulationControllable()
        elif params.mode == 'live':
            # todo: this is temporary and ugly!
            if not self._broker_service:
                raise ValueError("Broker service not set for live mode")

            return live_controllable.LiveControllable(
                BrokerStub(self._broker_service, params.session_id))

# todo: perform handle some stuff this side... like updating broker etc.


class InMemoryControllableStub(stub.ControllableStub):
    def __init__(self, directory, mode):
        # creator monitor service
        mon = monitor.Monitor()

        self._monitor = mon

        self._servicer = server.ControllableService(
            directory,
            InMemoryMonitorStub(mon),
            InMemoryControllableFactory(),
            mode)

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

    def _join(self, request, metadata):
        return self._servicer.Join(request, FakeContext(metadata))


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


