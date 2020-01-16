import subprocess

import grpc

from protos import controllable_pb2_grpc as cbl

from pluto.control.modes.processes import process

class LocalProcess(process.Process):
    __slots__ = ['_process']
    def __init__(self, framework_url):
        super(LocalProcess, self).__init__(framework_url)
        self._process = None

    def _create_controllable(self, framework_url):
        self._process = pr = subprocess.Popen(
            ['python', 'pluto/control/controllable/server.py', 'start', framework_url],
            stdout=subprocess.PIPE)

        return cbl.ControllableStub(grpc.insecure_channel(
            'localhost:{}'.format(int(pr.stdout.readline().decode('utf-8')))))

    def _stop(self):
        #blocks until process is terminated
        self._process.terminate()