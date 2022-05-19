from abc import abstractmethod, ABC

from google.protobuf import empty_pb2 as emp

from pluto.interface.utils import service_access

from protos import interface_pb2_grpc as itf
from protos import interface_pb2 as msg


class AbstractWatcher(ABC):
    def __init__(self, process):
        self._process = process
        # self._queue = queue.Queue()

    @property
    def process_id(self):
        return self._process.process_id

    # def watch(self):
    #     while True:
    #         packet = self._queue.get()
    #         yield packet
    #         if packet.end:
    #             break
    #
    # def stop_watching(self):
    #     self._process.stop_watching()

    @abstractmethod
    def performance_update(self, packet):
        pass


class Monitor(itf.MonitorServicer):
    def __init__(self):
        self._watch_list = {}

    @service_access.framework_only
    def PerformanceUpdate(self, request, context):
        watcher = self._watch_list.get(request.session_id, None)

        if watcher:
            watcher.performance_update(request)

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

    def watch(self, watcher):
        self._watch_list[watcher.process_id] = watcher

    # def StopWatching(self, request, context):
    #     pr = self._watch_list.pop(request.session_id, None)
    #     pr.stop_watching()
    #
    #     return msg.StopWatchingRequest()
