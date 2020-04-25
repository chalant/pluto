import threading
import queue

from google.protobuf import empty_pb2 as emp

from pluto.interface.utils import method_access

from protos import interface_pb2_grpc as itf
from protos import interface_pb2 as msg

class _Watcher(object):
    def __init__(self, process):
        self._event = threading.Event()
        self._process = process

        self._packet = None
        self._queue = queue.Queue()

    def watch(self):
        while True:
            packet = self._queue.get()
            yield packet
            if packet.end:
                break

    def stop_watching(self):
        self._process.stop_watching()

    def performance_update(self, packet):
        self._queue.put(packet)

class Monitor(itf.MonitorServicer):
    def __init__(self):
        self._watch_list = {}

        self._control_mode = None

    def set_control_mode(self, mode):
        '''

        Parameters
        ----------
        mode: pluto.control.modes.mode.ControlMode
        '''
        self._control_mode = mode

    @method_access.framework_only
    def PerformanceUpdate(self, request, context):
        watcher = self._watch_list.get(request.session_id, None)
        if watcher:
            watcher.performance_update(request)
        return emp.Empty()

    def Watch(self, request, context):
        session_id = request.session_id
        pr = self._control_mode.get_process(session_id)
        pr.watch()
        self._watch_list[session_id] = watcher = _Watcher(pr)
        for packet in watcher.watch():
            yield packet

    def StopWatching(self, request, context):
        pr = self._watch_list.pop(request.session_id, None)
        pr.stop_watching()
        return msg.StopWatchingRequest()