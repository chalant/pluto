import threading
import signal
from concurrent import futures

import grpc

class _Server(object):
    def __init__(self, service):
        self._event = threading.Event()
        self._server = server = grpc.server(
            futures.ThreadPoolExecutor(),
            interceptors=service.get_interceptors())
        self._service = service
        service.set_server(server)

    def serve(self, framework_url=None, recover=False):
        server = self._server
        if framework_url:
            port = server.add_insecure_port(framework_url)
        else:
            port = server.add_insecure_port('localhost:0')
        print(port)
        service = self._service
        server.start()
        if recover:
            service.recover()
        event = self._event
        event.clear()
        event.wait()
        service.stop()
        server.stop(0)

    def stop(self):
        self._event.set()

_SERVERS = []

def termination_handler(signum, frame):
    for srv in _SERVERS:
        srv.stop()

def interruption_handler(signum, frame):
    for srv in _SERVERS:
        srv.stop()

signal.signal(signal.SIGINT, interruption_handler)
signal.signal(signal.SIGTERM, termination_handler)

def get_server(service):
    srv = _Server(service)
    _SERVERS.append(srv)
    return srv
