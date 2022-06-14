import queue
import threading
from abc import abstractmethod, ABC

from google.protobuf import empty_pb2 as emp

from dev import events

from pluto.interface.utils import service_access

from protos import interface_pb2_grpc as itf
from protos import interface_pb2 as msg


class AbstractWatcher(ABC):
    def __init__(self, process_id):
        self._monitor = None
        self._process_id = process_id

    @property
    def process_id(self):
        return self._process_id

    @property
    def monitor(self):
        return self._monitor

    @monitor.setter
    def monitor(self, value):
        self._monitor = value

    @abstractmethod
    def performance_update(self, packet):
        raise NotImplementedError(self.performance_update.__name__)


class GeneratorWatcher(object):
    def __init__(self):
        self._event = threading.Event()
        self._queue = queue.Queue()
        self._monitor = None

    @property
    def monitor(self, ):
        return self._monitor

    @monitor.setter
    def monitor(self, value):
        self._monitor = value

    def performance_update(self, packet):
        self._queue.put(packet)

    def watch(self):
        if self._monitor:
            while True:
                packet = self._queue.get()
                yield packet

                if packet.end:
                    break

    def stop_watching(self):
        self._monitor.stop_watching()


class Monitor(itf.MonitorServicer):
    def __init__(self):
        self._watchers = []

    @service_access.framework_only
    def PerformanceUpdate(self, request, context):
        events.post_event(
            'monitor.{}'.format(request.session_id),
            request)

        return emp.Empty()

    # def Watch(self, request, context):
    #     session_id = request.session_id
    #     pr = self._process_manager.get_process(session_id)
    #
    #     # make a watch request to the controllable
    #     pr.watch()
    #
    #     self._watch_list[session_id] = watcher = _Watcher(pr)
    #
    #     return watcher

    # def StopWatching(self, request, context):
    #     pr = self._watch_list.pop(request.session_id, None)
    #     pr.stop_watching()
    #
    #     return msg.StopWatchingRequest()
