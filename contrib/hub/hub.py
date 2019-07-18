import grpc

import difflib

from protos import hub_pb2_grpc as rpc
from protos import environment_pb2 as env

from .data_layer import graph

from contrib.control import domain

from protos import data_pb2
from protos import hub_pb2

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

#TODO: we need a base path to store things... database or file system? => encapsulate this
#TODO: check for credentials... (One hub per client?) => credentials are added on app launch
# or added from environment...
class Hub(rpc.HubServicer):
    #todo: the client must push back the strategy it has (if it has one) before requesting
    # a new or existing strategy...
    def __init__(self, env_builder, graph_path):
        self._test_controller = None #todo

        self._builder = builder = env_builder
        self._path = graph_path
        #todo: root is loaded from disk. If it is none, we create it.
        graph_def = graph.load_graph(graph_path)

        if not graph_def:
            self._root = root = builder.group('hub')

            self._strategies = builder.group('strategies')
            self._domains = domains = builder.group('domains')
            self._sessions = sessions = builder.group('sessions')
            self._envs = envs = builder.group('envs')

            root.add_element(domains)
            root.add_element(sessions)
            root.add_element(envs)

            #store the graph...
            graph.freeze(root, graph_path)

        else:
            self._root = root = graph.create_graph(graph_def, env_builder)
            self._domains = graph.find_by_name(root, 'domains')[0]
            self._sessions = graph.find_by_name(root, 'sessions')[0]
            self._envs = graph.find_by_name(root, 'envs')[0]
            self._strategies = graph.find_by_name(root, 'strategies')[0]

    def GetController(self, request, context):
        '''
        Returns the proper a controller (simulation, paper or live)
        for live, we might need some extra credentials, username, password etc.

        #TODO: sends the graph(state) to the requested controller and returns the url of the controller...
        '''
        return

    def Deploy(self, request, context):
        '''
        Requests a deployment of a session to be used for real trading... Some credentials
        will be asked : username, password...

        #todo: 1)we should also store the tree in cloud
               2)we should set the deployed files to read-only
        '''

        root = self._root
        builder = self._builder
        strategies = self._strategies

        metadata = dict(context.invocation_metadata())
        id_ = metadata['strategy_id']
        session_id = metadata['session_id']


        sessions = self._sessions
        # get the strategy that corresponds to a session.
        strategy = graph.find_by_id(sessions, id_)

        #if the strategy is a copy, compare it to its original
        if strategy.copy:
            stg_file = graph.find_by_name(strategy, 'stg_file')[0]
            ogn_stg = graph.find_by_name(graph.find_by_id(strategies, stg_file.origin), 'stg_file')[0]
            org_file = next(ogn_stg.load(headless=True))
            cpy = next(stg_file.load(headless=True))
            #compare the two strategy files...
            result = difflib.unified_diff(org_file.decode(), cpy.decode())
            if not result:
                # no modifications were made to the file! wont deploy or test it.
                raise grpc.RpcError('Duplicated strategy!')

        stage_element = strategy.get_element('stage')[0]
        stage = next(stage_element).decode()
        #todo: create a session_id if non-existent (unique) add it to the strategy, and send the session + the strategy
        # file to the controller depending on the state of the session.

        #todo: send it to the test-controller, send back the controller-url to the client.
        if stage == 'dev':
            #the strategy has never been deployed in a session => create a new session.
            #update

            session = builder.group('session')
            dom = graph.find_by_name(strategies, 'domain')[0]
            dom.add_element(session)
            stage.set_element(builder.value('test'))
            #returns a controller-url for testing... to the client

            #if a test fail to execute, the state must stay in dev

            #a user might call deploy again on a failed test, and the strategy will move to paper or live...
            # => if the state is test, the hub must perform a benchmark... as for bugs there is no
            #much we can do except from putting some "barriers" like limits to leverage
            #limits the beta etc.

        elif stage == 'test':
            #todo: performs a test, if it passes, deploys it to "paper" or "live", depending on the hub.
            grpc.RpcError('Not implemented') #temporary...


        # freeze the graph.
        graph.freeze(root, self._path)  # todo: non-blocking.

        return data_pb2.Data()

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

        metadata = dict(context.invocation_metadata())

        try:
            stg = graph.find_by_id(root, metadata['strategy_id'])
        except KeyError:
            stg = None

        #will fit into memory
        dom_def = list(request_iterator)
        dom_id = domain.domain_id(dom_def)
        # create a new session id_ for the strategy

        dms = self._domains

        try:
            dom = dms.get_element(dom_id)[0]
            dom.get_element('environ')[0]
            #if an existing strategy_id is provided, check if it is not a duplicate
            #if it is, this will raise an error.
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

        sessions = self._sessions
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

        #store freeze the graph
        graph.freeze(root, self._path) #todo: non-blocking

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

        if stage == 'deployed':
            #can't modify a deployed session => create and return a copy.
            #copy the strategy into a new session...
            session = builder.group('session')
            session.add_element(builder.single('stage', builder.value('dev')))
            #create a copy of the strategy.
            strategy = graph.copy(strategy)
            session.add_element(strategy)
            dom.add_element(session)
            self._sessions.add_element(session)

        #freeze the data
        graph.freeze(root, self._path)

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
        '''Pushes the strategy in the correct slot...'''

        metadata = dict(context.invocation_metadata())
        str_id = metadata['strategy_id']

        strategy = self._builder.get_element(str_id)

        #reads the bytes
        def read():
            for data in request_iterator:
                yield data.data
        #store the strategy
        strategy.store(read())

        return hub_pb2.DeploymentReply('') #todo: send back the controller url.

    def DeploymentStatus(self, request, context):
        pass

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
                    # there can be atmost 1 strategy duplicate...
                    # todo: send back a html file to compare side by side
                    raise grpc.RpcError("This strategy is an exact replica")

