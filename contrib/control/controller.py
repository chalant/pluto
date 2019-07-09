import  grpc

from . import controller_pb2_grpc as ctrl_rpc
from . import controller_pb2 as ctrl
from . import controllable_pb2_grpc as cbl_rpc

from contrib.hub.hub_pb2 import StrategyRequest, StrategyID

from contrib.hub.environment_pb2 import ControllerEnvironment

from . import metadata
from . import domain

import socket

import uuid
import abc

class Controller(ctrl_rpc.ControllerServicer):
    def __init__(self, address, hub_stub, session_factory):
        self._sessions = {}
        self._address = address
        self._hub = hub_stub
        self._sess_factory = session_factory

    def Run(self, request, context):
        # we run a session (not a strategy). as session is a strategy on a given domain.
        #TODO: we can also run multiple strategies at the same time...
        session_id = request.id
        if session_id is None:
            raise grpc.RpcError('')
        #NOTE: we can load the environment from bytes..;
        env = self._load_env(
            self._hub.GetStrategy(StrategyRequest(
                user_type='CONTROLLER',
                id=StrategyID(id=session_id))))
        self._sessions[session_id] = sess = self._sess_factory.create_session(session_id, env.domain, env.strategy)
        sess.deploy(self._address, self._create_address())
        #delete environment...
        sess.start()
        del env
        return ctrl.Status()

    def Stop(self, request, context):
        pass

    def Watch(self, request, context):
        pass

    def ClockUpdate(self, request, context):
        pass

    @abc.abstractmethod
    def _get_clock(self, country_code):
        raise NotImplementedError

    def _create_domain(self, domain_proto):
        return domain.compute_domain(domain_proto)

    def _register_to_clock(self, session):
        for code in self._country_codes(session.domain):
            self._get_clock(code).register(session)

    def _country_codes(self, domain):
        return domain.country_code

    def _load_env(self, env_def):
        return ControllerEnvironment().ParseFromString(env_def)

    def _create_address(self):
        return


#TODO: the controller should always send a "session-id" as metadata
# the client and the controller will exchange this session-id
#TODO: raise a grpc error if a request doesn't have a session-id...
#TODO: We should encapsulate the controller, since the behavior changes with the environment..
class TestController(Controller):
    '''server that controls'''
    def __init__(self, address, hub_stub, session_factory):
        super(TestController, self).__init__(address, hub_stub, session_factory)
        self._clocks = {}

    def _create_session(self, domain_def):
        '''creates a session from the domain...'''
        #TODO: what is a session: a session describes the environment associated with
        # the strategy (domain, id etc.)
        '''create domain'''
        struct = domain.compute_domain(domain_def)

        id_ = uuid.uuid4().hex
        self._sessions[id_] = Session(id_, struct)
        return id_

    def Deploy(self, request_iterator, context):
        session_id = dict(context.invocation_metadata())['session_id']
        file = self._extract_strategy_file(request_iterator)
        s = socket()
        s.bind(('', 0))
        address = 'localhost:{}'.format(s.getsockname()[1])
        s.close()
        self._sessions[session_id].deploy(file, self._address, address)

    def Run(self, request, context):
        try:
            session_id = dict(context.invocation_metadata())['session_id']
            #register session to the corresponding clocks...
            #note: depending on the environment, clock might already be running or
            # a clock thread will be created...
            #TODO: in order to keep clock realisticly synchronized, we need to take
            # clock instantiation time into account.
            self._register_to_clock(self._sessions[session_id])
        except KeyError:
            #TODO: what message should we display?
            raise grpc.RpcError('')

class LiveController(Controller):
    def __init__(self, address, hub_stub, session_factory):
        super(LiveController, self).__init__(address, hub_stub, session_factory)






def get_controller(type=None):
    pass