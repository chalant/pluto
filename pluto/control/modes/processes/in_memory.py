import threading
import signal

from pluto.broker import broker_stub
from pluto.control.modes.processes import process_factory
from pluto.control.controllable import server
from pluto.control.controllable.utils import factory
from pluto.control.controllable import simulation_controllable
from pluto.control.controllable import live_controllable
from pluto.control.controllable import stub
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


class ControllableFactory(factory.ControllableFactory):
    def __init__(self, broker_service):
        self._broker_service = broker_service

    def _create_controllable(self, mode, session_id):
        if mode == 'simulation':
            return simulation_controllable.SimulationControllable()
        elif mode == 'live':
            return live_controllable.LiveControllable(
                BrokerStub(self._broker_service, session_id))


class ControllableStub(stub.ControllableStub):
    def __init__(self, servicer):
        self._servicer = servicer

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


class MonitorStub(object):
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


class InMemoryProcess(process_factory.Process):
    def __init__(self,
                 directory,
                 monitor_service,
                 controllable_factory,
                 framework_url,
                 session_id,
                 root_dir,
                 execute_events=False):
        self._directory = directory
        self._monitor_service = monitor_service
        self._controllable_fty = controllable_factory
        super(InMemoryProcess, self).__init__(
            framework_url,
            session_id,
            root_dir,
            execute_events)

    def _create_controllable(self,
                             framework_id,
                             framework_url,
                             session_id,
                             root_dir,
                             recover=False,
                             execute_events=False):
        return ControllableStub(
            server.ControllableService(
                MonitorStub(self._monitor_service),
                self._controllable_fty,
                self._directory))

    def _stop(self):
        pass


class InMemoryProcessFactory(process_factory.ProcessFactory):
    def __init__(self, directory):
        self._monitor_service = None
        self._controllable_fct = None
        self._directory = directory

    def set_monitor_service(self, monitor_service):
        self._monitor_service = monitor_service

    def set_broker_service(self, broker_service):
        self._controllable_fct = ControllableFactory(broker_service)

    def _create_process(self, framework_url, session_id, root_dir):
        return InMemoryProcess(
            self._directory,
            self._monitor_service,
            self._controllable_fct,
            framework_url,
            session_id,
            root_dir,
            execute_events=True)
