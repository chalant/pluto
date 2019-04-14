from abc import ABC, abstractmethod

from concurrent.futures import ThreadPoolExecutor

from grpc import (
    server,
    secure_channel,
    insecure_channel,
    ssl_server_credentials,
    ssl_channel_credentials
)


def create_channel(url, cert_auth=None):
    if not cert_auth:
        return insecure_channel(url)
    else:
        return secure_channel(url, ssl_channel_credentials(cert_auth))


class ServerWrapper(ABC):
    def __init__(self, server):
        self._server = server

    def start(self):
        if not self._started:
            self._server.start()
            self._started = True

    def stop(self, grace=None):
        if self._started:
            self._server.stop(grace)

class ServerFactory(ABC):
    @abstractmethod
    def get_server(self, server_address, key=None, certificate=None):
        """

        Parameters
        ----------
        server_address
        key
        certificate

        Returns
        -------
        ServerWrapper
        """
        raise NotImplementedError


class MainServerFactory(ServerFactory):
    def get_server(self, server_address, key=None, certificate=None):
        return self._create_server(server_address, key, certificate)

    def _create_server(self, url, key=None, certificate=None):
        srv = server(ThreadPoolExecutor(max_workers=10))
        if key and certificate:
            creds = ssl_server_credentials(((key, certificate),))
            srv.add_secure_port(url, server_credentials=creds)
        else:
            srv.add_insecure_port(url)
        return srv

def create_server(address, key_cert_pair=None):
    """

    Parameters
    ----------
    address : str
    key_cert_pair : tuple

    Returns
    -------
    grpc.Server

    """
    srv = server(ThreadPoolExecutor(max_workers=10))
    if key_cert_pair:
        srv.add_secure_port(address, server_credentials=ssl_server_credentials(key_cert_pair,))
    else:
        srv.add_insecure_port(address)
    return srv

