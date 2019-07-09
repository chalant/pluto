import abc
import grpc

from . import controllable_pb2_grpc as cbl_rpc
import subprocess as sub

#TODO: should be savable: will be used to retrieve a session
#TODO: this should be an abstraction to encapsulate deployment etc. depending on the
# environment.
class Session(abc.ABC):
    '''encapsulates a controllable...'''

    def __init__(self, id_, domain, strategy):
        self._id_ = id_
        self._domain = domain
        self._process = None
        self._controllable = None
        self._strategy = strategy

    @property
    def domain(self):
        return self._domain

    @property
    def id(self):
        return self._id_

    def deploy(self, server_url, controllable_url):
        self._controllable = cbl_rpc.ControllableStub(grpc.insecure_channel(controllable_url))
        self._deploy(server_url, controllable_url)

    @abc.abstractmethod
    def _deploy(self, server_url, controllable_url):
        raise NotImplementedError
    def start(self):
        pass

    def update(self):
        pass

    def stop(self):
        pass

    @abc.abstractmethod
    def shutdown(self):
        '''shuts down the session quietly'''
        pass

class LocalSession(Session):
    def __init__(self, id_, domain, strategy):
        super(LocalSession, self).__init__(id_, domain, strategy)
        self._process = None

    def _deploy(self, server_url, controllable_url):
        # TODO: this could also be launched as a pod or a container...
        self._process = sub.Popen(['python', 'controllable.py', 'register', server_url, controllable_url])

class SessionFactory(abc.ABC):
    @abc.abstractmethod
    def create_session(self, id_, domain, strategy):
        raise NotImplementedError

class LocalSessionFactory(SessionFactory):
    def create_session(self, id_, domain, strategy):
        return LocalSession(id_, domain, strategy)
