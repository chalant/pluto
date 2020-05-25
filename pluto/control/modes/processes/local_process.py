import subprocess

import grpc

from protos import controllable_pb2_grpc as cbl

from pluto.control.modes.processes import process_factory


class LocalProcess(process_factory.Process):
    def __init__(self,
                 framework_url,
                 session_id,
                 root_dir,
                 execute_events=False):
        super(LocalProcess, self).__init__(
            framework_url,
            session_id,
            root_dir,
            execute_events)
        self._process = None

    def _create_controllable(self,
                             framework_id,
                             framework_url,
                             session_id,
                             root_dir,
                             recover=False,
                             execute_events=False):
        args = [
            'python',
            'pluto/control/controllable/server.py',
            'start',
            framework_id,
            framework_url,
            session_id,
            root_dir]

        if recover:
            args.extend(['-re', '-ee'])

        self._process = pr = subprocess.Popen(
            args,
            stdout=subprocess.PIPE)
        port = pr.stdout.readline().decode('utf-8')
        return cbl.ControllableStub(grpc.insecure_channel(
            'localhost:{}'.format(int(port))))

    def _stop(self):
        self._process.terminate()


class LocalProcessFactory(process_factory.ProcessFactory):
    __slots__ = ['_process', '_broker']

    def _create_process(self, framework_url, session_id, root_dir):
        return LocalProcess(framework_url, session_id, root_dir)
