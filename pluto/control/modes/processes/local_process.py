import subprocess

import grpc

from protos import controllable_pb2_grpc as cbl

from pluto.control.modes.processes import process_factory


class LocalProcess(process_factory.Process):
    def __init__(self, framework_url, session_id):
        super(LocalProcess, self).__init__(framework_url, session_id)
        self._process = None

    def _create_controllable(self, framework_url, session_id):
        self._process = pr = subprocess.Popen(
            ['python',
             'pluto/control/controllable/server.py',
             'start',
             framework_url,
             session_id],
            stdout=subprocess.PIPE)
        port = pr.stdout.readline().decode('utf-8')
        return cbl.ControllableStub(grpc.insecure_channel(
            'localhost:{}'.format(int(port))))

    def _stop(self, controllable, param):
        controllable.Stop(param)
        self._process.terminate()


class LocalProcessFactory(process_factory.ProcessFactory):
    __slots__ = ['_process', '_broker']

    def _create_process(self, framework_url, session_id):
        return LocalProcess(framework_url, session_id)

