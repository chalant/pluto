from concurrent  import futures

from protos import controller_pb2_grpc as ctrl_rpc
from protos import controller_pb2 as ctrl
from protos import controllable_pb2_grpc as cbl_rpc
from protos import graph_pb2
from protos import data_bundle_pb2

from pluto.utils import graph
from pluto.control import clock_utils

from . import metadata
from . import domain

import socket

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

class Controller(ctrl_rpc.ControllerServicer):
    def __init__(self, address, hub_stub, builder):
        self._sessions = {}
        self._address = address
        self._hub = hub_stub
        self._builder = builder #todo: this builder must have a different loader (remote)
        self._gph = None

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
        gph = self._gph
        hub = self._hub
        builder = self._builder

        if not gph:
            self._gph = self._load_graph(hub.get_graph(), builder)

        envs = {}
        sessions = self._sessions

        pfn = self.name + '_perf'
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

            stub = self._create_controllable()

            sess = Session(node, dom, stub, file)

            sessions[node.id] = sess
        #run the session.
        self._run(sessions)

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


#TODO: the controller should always send a "session-id" as metadata
# the client and the controller will exchange this session-id
#TODO: raise a grpc error if a request doesn't have a session-id...
#TODO: We should encapsulate the controller, since the behavior changes with the environment..
class TestController(Controller):
    '''server that controls'''

    class ControllerThread(object):
        def __init__(self):
            self._sessions = []
            self._pool = futures.ThreadPoolExecutor()

        def ClockUpdate(self, clock_evt):
            pass

        def add_session(self, session):
            self._sessions.append(session)

        def start(self):
            pool = self._pool
            #todo: a clock must be callable
            for clock in self._clocks:
                pool.submit(clock)

    #TODO: HOW TO CREATE A CLOCK? => scan through the list of country codes and assets
    # a clock takes a calendar as argument => how do we instanciate the "right" calendar?
    # maybe we should specify the exchange instead of the country?
    def __init__(self, address, local_hub, session_factory):
        super(TestController, self).__init__(address, local_hub, session_factory)
        self._clocks = {}
        self._exchanges = exchanges = {}
        self._hub = local_hub

        for exc in local_hub.get_exchanges():
            country_code = exc.country_code
            self._append_to_dict(country_code, exc.name, exchanges)
            for at in exc.asset_types:
                self._append_to_dict(at, exc.name, exchanges)

    def _append_to_dict(self, key, value, dict_):
        v = dict_.get(key, None)
        if not v:
            dict_[key] = [value]
        else:
            dict_[key].append(value)

    def name(self):
        return 'simulation'

    def Deploy(self, request_iterator, context):
        session_id = dict(context.invocation_metadata())['session_id']
        file = self._extract_strategy_file(request_iterator)
        s = socket()
        s.bind(('', 0))
        address = 'localhost:{}'.format(s.getsockname()[1])
        s.close()
        self._sessions[session_id].deploy(file, self._address, address)

    def _run(self, sessions):
        '''

        Parameters
        ----------
        sessions

        Returns
        -------

        1)create clocks using the domain_struct of the session.
        2)

        '''

        clocks = self._clocks
        sess_per_exc = {}
        exc_set = set()
        for session in sessions:
            results = self._resolve_exchanges(session.domain_struct, self._exchanges)
            exc_set = exc_set & results
            for exc in results:
                self._append_to_dict(exc, session, sess_per_exc)
        #todo: create clocks
        #each clock


    def _resolve_exchanges(self, domain_struct, exchanges):
        cc = domain_struct.country_code
        at = domain_struct.asset_types

        # dictionray mapping keys like asset types and country codes, to sets of exchanges

        at_set = set()
        cc_set = set()

        # union of all exchanges trading the given asset types
        for c in at:
            at_set = at_set | exchanges[c]

        # union of all exchanges operating in the given countries
        cc_set = cc_set | exchanges[cc]

        # intersection of exchanges trading in the given countries and asset types
        return cc_set & at_set


    def _clock_update(self, clock_evt):

        pass


class LiveController(Controller):
    '''controls the downloader controllables. handles live and paper trading.'''

    def _clock_update(self, clock_evt):
        '''
        1)download data
        2)ingest data (stored in disk)
        3)send data to the controllables
        4)send clock event to controllables
        '''
        pass
