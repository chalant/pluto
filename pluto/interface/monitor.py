import threading
import uuid

from pluto.interface.utils import method_access

from protos import interface_pb2_grpc as itf

class _Watcher(object):
    def __init__(self, process):
        self._event = threading.Event()
        self._process = process

        self._packet = None

    def watch(self):
        self._event.wait()
        yield self._packet
        self._event.clear()

    def stop_watching(self):
        self._process.stop_watching()

    def performance_update(self, packet):
        self._event.set()
        self._packet = packet

class Monitor(itf.MonitorServicer):
    def __init__(self, control_mode):
        '''

        Parameters
        ----------
        control_mode: pluto.control.modes.mode.ControlMode
        '''
        self._control_mode = control_mode

        self._watch_list = {}

    @method_access.framework_only
    def PerformanceUpdate(self, request, context):
        #called by the controllable to update performance
        #todo: this can only be called by the framework
        #check if the token is correct, else do nothing or send an error
        # request.token

        watcher = self._watch_list.get(request.session_id, None)
        if watcher:
            watcher.performance_update(request)

    def Watch(self, request, context):
        session_id = request.session_id
        pr = self._control_mode.get_process(session_id)
        token = uuid.uuid4().hex
        self._tokens.append(token)
        pr.watch(token)
        self._watch_list[session_id] = watcher = _Watcher(pr)

        for packet in watcher.watch():
            yield packet

    def StopWatching(self, request, context):
        pr = self._watch_list.pop(request.session_id, None)
        pr.stop_watching()