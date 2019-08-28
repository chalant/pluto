import grpc

import difflib

from protos import dev_pb2_grpc as rpc
from protos import controller_pb2_grpc
from protos import controller_pb2 as ctrl

from contrib.controller import simulation_mode, controller

from contrib.utils import graph

from contrib.control import domain
from contrib.control.clock import clock

from protos import data_pb2
from protos import dev_pb2

import io

def chunk_bytes(bytes_, chunk_size):
    with io.BytesIO(bytes_) as f:
        while True:
            buffer = f.read(chunk_size)
            if buffer == b'':
                break
            else:
                yield buffer

def to_bytes(message):
    message.SerializeToString()

def stream(iterable):
    '''a generator that yields data by chunk'''
    for chunk in iterable:
        yield data_pb2.Data(data=chunk)

class Dev(rpc.DevServicer):
    #todo: the client must push back the strategy it has (if it has one) before requesting
    # a new or existing strategy...
    def __init__(self, env_builder, hub_client, sim_clock_factory, server):
        self._builder = env_builder
        #the hub returns the root of the graph
        self._root = root = hub_client.get_graph()

        self._domains = root.get_element('domains')[0]
        self._sessions_node = root.get_element('sessions')[0]
        self._envs = root.get_element('envs')[0]
        self._strategies = root.get_element('strategies')[0]

        self._hub = hub_client
        self._scf = sim_clock_factory
        self._controller = ctl = controller.Controller(self._hub, self._builder)
        controller_pb2_grpc.add_ControllerServicer_to_server(ctl, self._server)

        self._server = server

    def Run(self, request, context):
        '''
        runs a session
            1)load/create and store domain
            2)load/create and store sessions (and strategies)
        '''
        #todo: what if this method gets called multiple times?
        #todo: warning: this method can be called multiple times...
        #todo: should queue run requests to avoid repeated costly calls
        # this function is computationally expensive (queue simulation control modes)

        #todo: requests must be queued to avoid race conditions
        if self._controller.running:
            raise grpc.RpcError('')
        else:
            # this call doesn't block... how do we know when it is done? call back?
            self._controller.run(request.run_params,
                    simulation_mode.SimulationControlMode(
                        self._scf,
                        request.start_date.ToDatetime(),
                        request.end_date.ToDatetime()))
            return ctrl.Status()



    def Deploy(self, request, context):
        '''
        marks the strategy as deployed. At this point, no modification can be performed on this
        strategy

        #todo: in order to deploy a strategy, it must run without errors => perform a test run before
          deployment
        .

        #todo: 1)we should set the deployed files to read-only
               2)should make a test run before deploying.
               3)mark the strategy as deployed
        '''

        root = self._root
        builder = self._builder
        strategies = self._strategies
        hub_client = self._hub

        metadata = dict(context.invocation_metadata())
        id_ = metadata['strategy_id']
        session_id = metadata['session_id']


        sessions = self._sessions_node

        hub_client.freeze_graph()

        #upload the graph to the hub. (iterable)
        hub_client.upload_graph(stream(root.load(1024 * 16)))

        return data_pb2.Data() #todo: why are we returning data?

    def New(self, request_iterator, context):
        '''
        creates or loads a domain and adds a new strategy to the domain.

        an existing strategy could be provided if we want to transfer the strategy to
        another domain.
        
        '''

        #todo: what if the strategy is already in the domain?
        #todo : send the domain to the controllers.

        builder = self._builder

        #we will add the new strategy in the current branch.
        root = self._root
        hub_client = self._hub

        metadata = dict(context.invocation_metadata())

        try:
            stg = builder.get_element(metadata['strategy_id'])
        except KeyError:
            stg = None

        #will fit into memory
        dom_def = list(request_iterator)
        dom_id = domain.domain_id(dom_def)
        # create a new session id_ for the strategy

        dms = self._domains

        try:
            dom = dms.get_element(dom_id)[0]
            #if an existing strategy_id is provided, check if it is not a duplicate
            #if it is, this will raise an error.
            f = dom.get_element('domain_def')[0]
            if stg:
                self._check_copy(stg, dom)

        except IndexError:
            #if the domain doesn't exist, it is safe to add the strategy.
            dom = builder.group(dom_id)
            f = builder.file('domain_def')
            dom.add_element(f.store(dom_def))
            environ = builder.file('environ')
            dom.add_element(environ)

            environ.store(self._create_environment(dom_def))

        sessions = self._sessions_node
        strategies = self._strategies
        #a session keeps track of the strategy performance, and state...
        session = builder.group('session')
        strategy = builder.file('strategy')
        dom.add_element(session)

        session.add_element(strategy)

        strategies.add_element(strategy)
        sessions.add_element(session)

        #store the state of the session as dev
        session.add_element(builder.single('stage', builder.value('dev')))

        if stg:
            strategy.store(stg.load()) #todo: store a template with the proper function definitions
        else:
            strategy.store(b'')

        session.add_element(builder.value('dom_def_id', f.id))

        #store freeze the graph
        hub_client.freeze_graph()

        #send the domain_id and the strategy_id => will be used when pushing back to the hub.
        context.send_initial_metadata((
            ('session_id', session.id),
            ('domain_id', dom_id),
            ('strategy_id', strategy.id)))

        # send the all the strategies and there corresponding environment...
        size = 1024 * 16
        for chunk in stream(strategy.load(size)):
            yield chunk

        for chunk in stream(environ.load(size)):
            yield chunk

    def Modify(self, request, context):
        '''
        Request a strategy for modification from an existing domain.
        a modification request can be made on a session that is either in test or dev
        stage. if a session is in a deployed stage, a copy of the session is created and sent.
        todo: what if the copy is not modified? how do we prevent it from being deployed?
         => diff
        '''
        hub = self._hub
        root = self._root
        builder = self._builder

        metadata = dict(context.invocation_metadata())
        try:
            session_id = metadata['session_id']
            domain_id = metadata['domain_id']
            str_id = metadata['strategy_id']
        except KeyError:
            raise grpc.RpcError('')

        session = graph.find_by_id(root, session_id) #get the session that corresponds to the id.

        stage = next(session.get_element('stage')[0].load()).decode()
        dom = self._domains.get_element(domain_id)[0]
        strategy = builder.get_element(str_id)

        #get the cached environment...
        environ = dom.get_element('environ')[0]

        #if a strategy has been deployed, it can't be modified
        if stage == 'deployed':
            #can't modify a deployed session => create and return a copy.
            #copy the strategy into a new session...
            session = builder.group('session')
            session.add_element(builder.single('stage', builder.value('dev')))
            #create a copy of the strategy.
            strategy = graph.copy(strategy)
            session.add_element(strategy)
            dom.add_element(session)
            self._sessions_node.add_element(session)

        #freeze the data
        hub.freeze_graph()

        #send back metadata
        context.send_initial_metadata((
            ('session_id', session.id),
            ('strategy_id', strategy.id),
            ('domain_id', domain_id))
        )

        sz = 1024 * 16  # 16KB

        # send the strategy
        for chunk in stream(strategy.load(sz)):
            yield chunk

        # send the environment
        for chunk in stream(environ.load(sz)):
            yield chunk

    def Push(self, request_iterator, context):
        '''Stores the strategy'''

        metadata = dict(context.invocation_metadata())
        str_id = metadata['strategy_id']

        strategy = self._builder.get_element(str_id)

        #reads the bytes
        def read():
            for data in request_iterator:
                yield data.data
        #store the strategy
        strategy.store(read())

        return dev_pb2.DeploymentReply('') #todo: send back the controller url.

    def List(self, request, context):
        '''returns a list of elements'''
        metadata = dict(context.invocation_metadata())

        #todo: we could either get strategies from a certain domain, or strategies
        # of all domains (based on the metadata)
        #sends the strategy metadata as-well (id, etc.)
        for chunk in stream(self._strategies.load(1024 * 16)):
            yield chunk

    def ViewDomains(self, request, context):
        #sends the metadata as-well...
        '''returns a list of stored domains'''
        return

    def _check_copy(self, strategy, domain_):
        '''Checks if the copy is valid. If it is an un-modified copy, it will raise an error.'''
        #get all the strategies in the domain.
        strategies = domain_.get_element('strategy')[0]
        for stg in strategies:
            src_id = stg.id
            #first check if the strategy is the same...
            if src_id == strategy.id:
                raise grpc.RpcError("Can't add a strategy twice to the same domain.")
            #check if str is derived from strategy
            if graph.is_derived_from(stg, strategy):
                #compare the strategy and the copy (str)
                org_file = next(stg.load(headless=True))
                cpy = next(strategy.load(headless=True))
                r = difflib.unified_diff(org_file.decode(), cpy.decode())
                if not r:
                    # there can be at most 1 strategy duplicate...
                    # todo: send back a html file to compare side by side
                    raise grpc.RpcError("This strategy is an exact replica")


