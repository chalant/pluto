from abc import ABC, abstractmethod

from concurrent.futures import ThreadPoolExecutor

from grpc import (
    server,
    secure_channel,
    insecure_channel,
    ssl_server_credentials,
    ssl_channel_credentials
)

from kubernetes import (
    config,
    client
)

try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()


def create_channel(url,certificate=None):
    if not certificate:
        return insecure_channel(url)
    else:
        return secure_channel(url, ssl_channel_credentials(certificate))


class IServer(ABC):

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self, grace=None):
        raise NotImplementedError


class Server(IServer):
    def __init__(self, server_address, key=None, certificate=None):
        self._server = self._get_server(server_address, key, certificate)
        self._registered = False

    def _get_server(self, address, key, certificate):
        return self._create_inner_server(address, key, certificate)

    @abstractmethod
    def _add_servicer_to_server(self, server):
        raise NotImplementedError

    def _create_inner_server(self, url, key=None, certificate=None):
        srv = server(ThreadPoolExecutor(max_workers=10))
        if key:
            creds = ssl_server_credentials(((key, certificate),))
            srv.add_secure_port(url, server_credentials=creds)
        else:
            srv.add_insecure_port(url)
        return srv

    def start(self):
        server = self._server
        if not self._registered:
            self._add_servicer_to_server(server)
        server.start()

    def stop(self, grace=None):
        self._server.stop(grace)