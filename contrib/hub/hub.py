import uuid
import grpc

from datetime import datetime

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

        #classify performance file by session_id => used for tracking performance.
        self._performance = d = {}
        for session in sessions:
            d[session.id] = graph.find_by_name(session, 'performance')[0]

    def Deploy(self, request, context):
        '''
        Deploy a strategy on a certain domain...

        If a strategy is in development state, it is deployed to a testing environment.
        If it is in testing environment, it gets deployed in staging environment.

        A test is ran on the strategy to see if it works fine (no code error etc.)
        If it fails, the strategy won't be deployed and an error is sent to the client...
        The strategy state must be reverted to development...

        The client must then make a modify request

        After that, the strategy remains pending and awaits some external party (admin)
        to confirm its deployment to live.

        and a controller_url is sent back to the client (test, live...) if the deployment was
        successful.

        How do we control the deployment? A branch must be tested etc. Before being deployed
        to an "upper" level (paper trading, then live)
        simulation -> paper -> live.
        This must be enforced.
        A branch deployment is accepted by external parties with sufficient access level
        (admin, etc.)
        Once a branch is accepted it is "froze/saved" and any subsequent request for
        development will return a copy of this branch...

        What if the user wants to do some experimentation? Always start from the
        latest deployed branch, at which point the user can branch-off of..(copy that branch
        and develop) The user can also branch-off a non-deployed branch...
        => must specify the branch.

        Deploys a set of strategies on a "higher" environment... (paper or live)
        returns the controller url... (can only deploy a pushed strategy)
        1) can only deploy a (working => must not contain code errors, etc.) pushed strategy
        2) re-deploying a strategy will over-write the previously deployed strategy
        3) re-deploying an already running strategy will cause it to shut-down and the
           modified strategy will start from the previous strategy state...
        4) re-deployment might cause capital to be rebalanced

        Copies the develop branch into the master branch and sends the master branch
        to a controller (paper, live). This must be validated by some peers.

        If a strategy is a copy of another, we must compare it to the origin to make sure
        it is not exactly the same (use difflib)... also compare benchmarks, the best of the two might be selected,
        or they might be complementary etc.

        #todo: 1)we should also store the tree in cloud
               2)we should set the deployed files to read-only
        '''

        root = self._root
        builder = self._builder
        strategies = self._strategies

        id_ = dict(context.invocation_metadata())['strategy_id']
        sessions = self._sessions
        # get the strategy that corresponds to a session.
        strategy = graph.find_by_id(sessions, id_)


        #todo: maybe put the state as a property of an element (faster) => no need for searches
        # problem: not all elements uses this...
        if not strategy:
            strategy = graph.find_by_id(strategies, id_)
            session = builder.group('session')
            session.add_element(strategy)
            session.add_element(graph.find_by_id(self._domains, id_))

        else:
            session = [sess for sess in strategy.parents if sess.value == 'session'].pop()


        #if the strategy is a copy, compare it to its original
        if strategy.copy:
            stg_file = graph.find_by_name(strategy, 'stg_file')[0]
            ogn_stg = graph.find_by_name(graph.find_by_id(strategies, stg_file.origin), 'stg_file')[0]
            org_file = ogn_stg.load()
            cpy = stg_file.load()
            #compare the two strategy files...
            result = difflib.unified_diff(org_file.decode(), cpy.decode())
            if not result:
                # no modifications were made to the file! wont deploy or test it.
                raise grpc.RpcError('Duplicated strategy!')

        state_element = graph.find_by_name(strategy, 'state')[0]
        state = next(state_element.load())
        #todo: create a session_id if non-existent (unique) add it to the strategy, and send the session + the strategy
        # file to the controller depending on the state of the session.

        if state == 'dev':
            #the strategy has never been deployed in a session => create a new session.
            #update

            session = builder.group('session')
            dom = graph.find_by_name(strategies, 'domain')[0]
            dom.add_element(session)
            state_element.set_element(builder.value('test'))
            #returns a controller-url for testing... to the client

            #if a test fail to execute, the state must stay in dev

            #a user might call deploy again on a failed test, and the strategy will move to paper or live...
            # => if the state is test, the hub must perform a benchmark... as for bugs there is no
            #much we can do except from putting some "barriers" like limits to leverage
            #limits the beta etc.

        elif state == 'test':
            #todo: performs a test, if it passes, deploys it to "paper" or "live", else,
            # change the state to dev
            grpc.RpcError('Not implemented') #temporary...


        # freeze the graph.
        graph.freeze(root, self._path)  # todo: non-blocking.

        return data_pb2.Data()

    def New(self, request_iterator, context):
        '''
        creates a new strategy, and sends back its id
        
        '''
        metadata = dict(context.invocation_metadata())
        dom_id = metadata['domain_id']

        builder = self._builder

        #we will add the new strategy in the current branch.
        root = self._root

        #will fit into memory
        dom_def = list(request_iterator)
        dom_id = domain.domain_id(dom_def)
        # create a new session id_ for the strategy

        doms = graph.find_by_name(root, 'domains')[0]
        envs = graph.find_by_name(root, 'envs')[0]

        #get the group with the dom_id
        result = graph.find_by_name(dom_id)
        strategy = builder.group('strategy_meta')
        stg_file = builder.file('strategy')
        strategy.add_element(stg_file)
        #store the strategy
        self._strategies.add_element(strategy)
        try:
            [i for i in result if i.name == 'domain'].pop()
            environ = graph.find_by_name(envs, dom_id)[0]
            # add the strategy to the environment
            # environ.add_element(strategy)
        except IndexError:
            dom = builder.group('domain')
            doms.add_element(dom)
            d = builder.file('dom_def')
            d.store(dom_def)
            dom.add_element(d)

            # add the domain to the strategy
            strategy.add_element(dom)

            # new environment (since the dom doesn't exist yet)
            environ = builder.group('environ')
            # we can get the element by dom_id
            envs.add_element(environ)
            # store the environment created from dom_def
            environ.store(self._create_environment(dom_def))
        #store the dom_def if it deosn't exist...

        stg_file.store(b'') #todo: store a template or empty file...
        sessions = self._sessions

        #a session stores the performance of a strategy... so is bound to a single strategy
        sess = builder.group('session')
        # id_element = builder.group('session_id')
        #
        # session_id = uuid.uuid4().hex
        #
        # id_element.add_element(builder.value(session_id))
        #
        # sess.add_element(id_element)
        sess.add_element(strategy)
        sess.add_element(builder.file('performance')) #performance file.
        sess.add_element(environ) #add the environment to the session.

        #create a state single element and set it value to dev
        strategy.add_element(builder.single('state', builder.value('dev')))

        sessions.add_element(sess)
        #freeze the data and tree structure
        graph.freeze(root, self._path)

        #sends the all the strategies and there corresponding environment...



        #send the strategy_id to the client...
        context.send_initial_metadata((('strategy_id', strategy.id)))

    def Modify(self, request, context):
        '''
        a modification request can be made on a session that is either in test or dev
        state. if a session is in a deployed state, a copy of the session is created and sent.
        todo: what if the copy is not modified? how do we prevent it from being deployed?

        =>

        => APPEND ONLY

        During development cycle, we might need to store the state of the strategy we're
        developing (pushed (non-deployed) strategies can still be modified...)

        requests modification of a strategy on the current branch.

        creates a new branch which is a copy of the current branch...
        the client must give back the previous strategy he's working on...
        if there is no previous strategy
        '''
        root = self._root

        metadata = dict(context.invocation_metadata())
        stg_id = metadata['strategy_id']
        session = graph.find_by_id(root, stg_id) #get the session that corresponds to the id.

        state = next(session.get_element('state')[0].load())

        if state != 'deployed':
            strategy = graph.find_by_name(session,'strategy')[0]
            environ = graph.find_by_name(session,'environ')[0]
        else:
            #return a copy of the strategy... a copy keeps an origin
            session = graph.copy(session)
            graph.find_by_name('sessions')[0].add_element(session)

        #send back a new session id
        context.send_initial_metadata((('session_id', session.id)))

        sz = 1024 * 16  # 16KB

        # send the environment
        for chunk in stream(environ.load(sz)):
            yield chunk
        # send the strategy
        for chunk in stream(strategy.load(sz)):
            yield chunk


    def Push(self, request_iterator, context):
        '''Used to save a session in the hub...'''

        metadata = dict(context.invocation_metadata())
        session = self._root.get_group(metadata['session_id'])[0]

        state = next(session.get_element('state')[0].load())


        return hub_pb2.DeploymentReply('') #todo: send back the controller url.

    def DeploymentStatus(self, request, context):
        pass

    def List(self, request, context):
        '''returns all strategies and their environments of the current branch.'''
        for chunk in stream(self._envs.load(1024 * 16)):
            yield chunk

    def SelectDomain(self, request, context):
        '''returns the domain of the selected domain'''
        return

    def AddEnvironment(self, request_iterator, context):
        builder = self._builder

        dom_def = list(request_iterator)
        dom_id = domain.domain_id()

        domains = self._domains
        envs = self._envs

        dom = graph.find_by_name(domains, dom_id)[0]
        if not dom:
            dom = builder.group(dom_id)
            f = builder.file('domain_def')
            dom.add_element(f.store(dom_def))
            environ = builder.group(dom_id)
            env_f = builder.file('environ')
            environ.add_element(env_f)
            env_f.store(self._create_environment(dom_def))
            envs.add_element(environ)

        context.send_initial_metadata((('domain_id', dom_id),))