import subprocess

import grpc

from protos import controllable_pb2_grpc as cbl

from pluto.control.modes.processes import process


class LocalProcess(process.Process):
    __slots__ = ['_process', '_broker']

    def __init__(self, session_id, framework_url):
        super(LocalProcess, self).__init__(session_id, framework_url)
        self._process = None

    def _create_controllable(self, framework_url, session_id):
        self._process = pr = subprocess.Popen(
            ['python',
             'pluto/control/controllable/server.py',
             'start',
             framework_url,
             session_id],
            stdout=subprocess.PIPE)

        return cbl.ControllableStub(grpc.insecure_channel(
            'localhost:{}'.format(int(pr.stdout.readline().decode('utf-8')))))

    def _update(self, event_writer):
        pass

    def _stop(self):
        # blocks until process is terminated
        self._process.terminate()
