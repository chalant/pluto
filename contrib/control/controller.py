import  grpc

from . import controller_pb2_grpc as ctrl_rpc
from . import controller_pb2 as ctrl
from . import controllable_pb2_grpc as cbl_rpc

from . import metadata

from zipline.pipeline import domain as dom
import socket
import subprocess as sub

import uuid

#TODO: should be savable: will be used to retrieve a se
#TODO: this should be an abstraction to encapsulate deployment etc. depending on the
# environment.
class Session(object):
    '''used to track strategy performance, state and other data...'''
    def __init__(self, id_, domain):
        self._id_ = id_
        self._domain = domain
        self._process = None
        self._controllable = None

    @property
    def domain(self):
        return self._domain

    @property
    def id(self):
        return self._id_

    def deploy(self, strategy_file, server_url, controllable_url):
        self._process = sub.Popen(['python', 'controller.py', 'register',
                                   strategy_file, server_url, controllable_url])
        self._controllable = cbl_rpc.ControllableStub(grpc.insecure_channel(controllable_url))

    def update(self):
        pass

    def stop(self):
        pass

    def shutdown(self):
        pass

#TODO: the controller should always send an "session-id" as metadata
# the client and the controller will exchange this session-id
#TODO: raise a grpc error if a request doesn't have a session-id...
class Controller(ctrl_rpc.ControllerServicer):
    '''server that controls'''
    def __init__(self, address):
        self._sessions = {}
        self._address = address

    def _create_session(self, domain):
        '''creates a session from the domain...'''
        #TODO: what is a session: a session describes the environment associated with
        # the strategy (domain, id etc.)
        '''create domain'''
        id_ = uuid.uuid4().hex
        self._sessions[id_] = Session(id_, domain)
        return id_

    def _extract_strategy_file(self, stream):
        return

    def _get_clock(self, domain):
        '''creates or retrieves a clock depending on the domain and the environment...'''
        return

    def Deploy(self, request_iterator, context):
        session_id = dict(context.invocation_metadata())['session_id']
        file = self._extract_strategy_file(request_iterator)
        s = socket()
        s.bind(('', 0))
        address = 'localhost:{}'.format(s.getsockname()[1])
        s.close()
        self._sessions[session_id].deploy(file, self._address, address)

    def Run(self, request, context):
        session_id = dict(context.invocation_metadata())['session_id']
        session = self._sessions[session_id]
        clock = self._get_clock(session.domain)
        clock.register(session)


    def GetEnvironment(self, request, context):
        '''generates a set of files and to be sent to the client as a tar file...'''
        #send session id to the client...
        id_ = self._create_session(request.domain)
        metadata.send_session_id(id_, context)
        #TODO: create tar file, send it in chuncks to the database.
        #      store the session in database or file system. The session uniquely
        #      identifies the strategy. if it is a file, the file name is the session_id
        '''creates a set of files that are a "reflection" of the database (bundle)
         structure and sends them as a tar file.'''
