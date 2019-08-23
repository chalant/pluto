from concurrent  import futures

from protos import controller_pb2_grpc as ctrl_rpc
from protos import controller_pb2 as ctrl
from protos import controllable_pb2_grpc as cbl_rpc
from protos import graph_pb2
from protos import data_bundle_pb2

from contrib.utils import graph
from contrib.control import clock_utils

from contrib.control import domain

import uuid
import abc

class Session(object):
    def __init__(self, sess_node, dom_struct, cbl_stub, perf_file):
        '''controls '''
        self._sess_node = sess_node
        self._perf_file = perf_file
        self._dom_struct = dom_struct
        self._stub = cbl_stub

        #todo: put this in the algorithm class?
        self._strategy = next(self._sess_node.get_element('strategy')[0].load()).decode()

    @property
    def domain_struct(self):
        return self._dom_struct

    def clock_update(self, clock_evt):
        pass

    def update_performance(self, stream):
        '''receives a stream of bytes (that represents a performance packet) and appends it to a
        binary file.'''
        bytes_ = b''
        for d in stream:
           bytes_ = bytes_ + d.data

        #each set of bytes is on a line.
        bytes_ = bytes_ + b'\n'

        #appends the data to the perf file...
        self._perf_file.store(bytes_)

    def stop(self, params):
        pass

    def watch(self):
        pass

class ControlMode(abc.ABC):
    def __init__(self):
        pass

    @abc.abstractmethod
    @abc.abstractproperty
    def name(self):
        raise NotImplementedError

    def run(self, sessions):
        self._run(sessions)

    @abc.abstractproperty
    def _run(self, sessions):
        raise NotImplementedError

    def create_controllable(self):
        address = self._create_address()
        # todo: create and return controllable stub object
        #  using a the address

    @abc.abstractproperty
    def _create_address(self):
        raise NotImplementedError



class Controller(ctrl_rpc.ControllerServicer):
    def __init__(self, hub_stub, builder, control_mode):
        '''

        Parameters
        ----------
        address
        hub_stub
        builder : contrib.utils.graph.Builder
        control_mode : ControlMode
        '''
        self._sessions = {}
        self._hub = hub_stub
        self._builder = builder #todo: this builder must have a different loader (remote)
        self._gph = None
        self._control_mode = control_mode

    def _load_graph(self, bytes_, builder):
        return graph.load_graph_from_bytes(bytes_, builder)

    def UpdateGraph(self, request_iterator, context):
        #todo: the loader of the builder must be a "remote" loader: it loads data from
        # the hub/dev service.
        self._gph = self._load_graph(request_iterator, self._builder)

    def Run(self, request_iterator, context):
        '''runs a session
            1)load create and store domain
            2)load create and store sessions (and strategies)
        '''
        mode = self._control_mode
        gph = self._gph
        hub = self._hub
        builder = self._builder

        if not gph:
            self._gph = self._load_graph(hub.get_graph(), builder)

        envs = {}
        sessions = self._sessions

        pfn = mode.name + '_perf'
        for request in request_iterator:
            node = builder.get_element(request.session_id)


            el = next(node.get_element('dom_def_id')[0].load()).decode()
            dom_def = envs.get(el, None)
            if not dom_def:
                dom_def = data_bundle_pb2.CompoundDomainDef()
                dom_def.ParseFromString(next(builder.get_element(el).load()))
                domain.compute_domain(dom_def)
                envs[el] = dom = domain.compute_domain(dom_def)
            else:
                dom = envs[el]

            file = node.get_element(pfn)[0]
            if not file:
                file = builder.file(pfn)
                node.add_element(file)

            stub = mode.create_controllable()

            sess = Session(node, dom, stub, file)

            sessions[node.id] = sess
        #run the session.
        mode.run(sessions)

        #todo: makes a request to the hub for a strategy, then runs that strategy.
        #we run a session (not a strategy). as session is a strategy on a given domain.
        #TODO: we can also run multiple strategies at the same time...

        return ctrl.Status()

    def Stop(self, request, context):
        session = self._sessions[request.id]
        session.stop()

    def Watch(self, request, context):
        pass

    def ClockUpdate(self, request, context):
        self._clock_update(request)

    def Deploy(self, request_iterator, context):
        '''receives a strategy and its associated domain'''
        pass

    @abc.abstractmethod
    def _clock_update(self, clock_evt):
        raise NotImplementedError

    @abc.abstractmethod
    def _run(self, sessions):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_controllable(self):
        raise NotImplementedError

    @abc.abstractmethod
    @property
    def name(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_session(self, session_node, domain):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_clock(self, country_code):
        raise NotImplementedError

    def _register_to_clock(self, session):
        for code in self._country_codes(session.domain):
            self._get_clock(code).register(session)

    def _country_codes(self, domain):
        for d in  domain.country_code:
            yield d

    def _create_address(self):
        return