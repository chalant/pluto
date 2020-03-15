from pluto.control.modes.processes import process_factory
from pluto.control.controllable import server
from pluto.interface.utils.method_access import invoke

def framework_method(func):
    def wrapper(instance, request):
        invoke(func, request)
    return wrapper

class FakeContext(object):
    __slots__ = ['_invocation_metadata']

    def __init__(self, metadata=None):
        self._invocation_metadata = metadata if metadata else ()

    def invocation_metadata(self):
        return self._invocation_metadata


class ControllableStub(object):
    def __init__(self, servicer):
        self._servicer = servicer

    def Initialize(self, request, metadata=None):
        return self._servicer.Initialize(request, FakeContext(metadata))

    def UpdateParameters(self, request, metadata=None):
        return self._servicer.UpdateParameters(request, FakeContext(metadata))

    def ClockUpdate(self, request, metadata=None):
        return self._servicer.ClockUpdate(request, FakeContext(metadata))

    def Stop(self, request, metadata=None):
        return self._servicer.Stop(request, FakeContext(metadata))

    def UpdateAccount(self, request, metadata=None):
        return self._servicer.UpdateAccount(request, FakeContext(metadata))

    def Watch(self, request, metadata=None):
        return self._servicer.Watch(request, FakeContext(metadata))

    def StopWatching(self, request, metadata=None):
        return self._servicer.StopWatching(request, FakeContext(metadata))


class MonitorStub(object):
    def __init__(self, monitor_servicer):
        self._monitor_server = monitor_servicer

    def Watch(self, request):
        return self._monitor_server.Watch(request, FakeContext())

    def StopWatching(self, request):
        return self._monitor_server.StopWatching(request, FakeContext())

    def PerformanceUpdate(self, request_iterator, metadata=None):
        return self._monitor_server.PerformanceUpdate(request_iterator, FakeContext(metadata))


class InMemoryProcess(process_factory.Process):
    def __init__(self, monitor_service, framework_url, session_id, root_dir):
        self._monitor_service = monitor_service
        super(InMemoryProcess, self).__init__(framework_url, session_id, root_dir)

    def _create_controllable(self, framework_id, framework_url, session_id, root_dir):
        return ControllableStub(
            server.ControllableService(
                MonitorStub(self._monitor_service)))

    def _stop(self):
        pass


class InMemoryProcessFactory(process_factory.ProcessFactory):
    def __init__(self):
        self._monitor_service = None

    def set_monitor_service(self, monitor_service):
        self._monitor_service = monitor_service

    def _create_process(self, framework_url, session_id, root_dir):
        return InMemoryProcess(
            self._monitor_service,
            framework_url,
            session_id,
            root_dir)
