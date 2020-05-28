from abc import ABC, abstractmethod


class Service(ABC):
    @abstractmethod
    def get_interceptors(self):
        raise NotImplementedError

    @abstractmethod
    def set_server(self, server):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError

    @abstractmethod
    def recover(self):
        raise NotImplementedError