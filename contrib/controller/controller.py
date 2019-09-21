import threading
import grpc
import abc
import math

from datetime import datetime

from protos import controller_pb2_grpc, controller_pb2, clock_pb2, data_bundle_pb2

from contrib.control import domain

class SessionParams(object):
    __slots__ = ['capital', 'max_leverage']

    def __init__(self, capital, max_leverage):
        self.capital = capital
        self.max_leverage = max_leverage

class Session(object):
    def __init__(self, sess_node, dom_id, cbl_stub, perf_file, param):
        self._clocks = {}
        self._sess_node = sess_node
        self._perf_file = perf_file

        self._dom_id = dom_id
        self._stub = cbl_stub

        #todo: put this in the algorithm class? no, this is doen server side (controllable)
        #todo:
        self._strategy = next(self._sess_node.get_element('strategy')[0].load()).decode()
        self._param = param

    @property
    def dom_id(self):
        return self._dom_id

    #this is called at some frequency
    def clock_update(self, clock, clock_evt):
        #todo: send signals to the controllable service.
        # the controllable sends back a data packet through the update performance method
        if clock_evt.event == clock_pb2.CALENDAR:
            self._update_sessions()

    def parameters_update(self, params):
        #todo: update stub parameters
        pass

    def _update_sessions(self):
        clocks = self._clocks

    def update_performance(self, stream):
        #
        '''receives a stream of bytes (that represents a performance packet) and appends it to a
        binary file.'''

        bytes_ = b''
        for d in stream:
           bytes_ = bytes_ + d.data

        #each set of bytes is on a line.
        bytes_ = bytes_ + b'\n'

        #appends the data to the perf file...
        self._perf_file.store(bytes_) #todo: how do we specify the append mode?

    def stop(self, params):
        pass

    def watch(self):
        pass

    def liquidate(self):
        pass

    def add_clock(self, clock):
        self._clocks[clock.exchange] = clock


class ControlMode(abc.ABC):
    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def signal_filter(self):
        raise NotImplementedError

    def get_trader(self, capital, max_leverage, broker_url):
        cbl = self._get_trader(capital, max_leverage, broker_url)
        return cbl

    @abc.abstractmethod
    def get_loop(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_broker(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_trader(self, capital, max_leverage, broker_url):
        raise NotImplementedError

class Controller(controller_pb2_grpc.ControllerServicer):
    def __init__(self, hub_client, builder):
        self._hub = hub_client
        self._builder = builder

        self._sessions = {}
        self._exchange_mappings = exchanges = {}
        self._clocks = []

        self._stop_event = threading.Event()
        self._run_lock = threading.Lock()
        self._stop_lock = threading.Lock()

        #classify exchanges by country_codes and asset_types
        for exc in hub_client.get_exchanges():
            country_code = exc.country_code
            name = exc.name
            self._append_to_dict(country_code, name, exchanges)
            for at in exc.asset_types:
                self._append_to_dict(at, name, exchanges)

        self._running_flag = False
        self._interrupt = False
        self._num_clocks = 0
        self._status = controller_pb2.Status()

    @property
    def running(self):
        return self._running_flag

    def stop(self, liquidate=False):
        #todo: liquidate is passed to the control mode
        with self._stop_lock:
            if self._running_flag: #only stop when the sessions are running
                for clock in self._clocks:
                    clock.stop()
                self._stop_event.wait() #block until the stop event occurs

    def Watch(self, session_id):
        for packet in self._sessions[session_id].watch():
            yield packet

    def run(self, params, control_mode):
        '''
        runs a session
            1)load/create and store domain
            2)load/create and store sessions (and strategies)
        '''
        # event = self._stop_event
        # lock = self._run_lock


        #todo: perform other parameters checks (total leverage, etc.)
        if sum([param.capital_ratio for param in params]) > 1:
            #todo: should we avoid raising an exception here? => might interrupt the execution
            raise grpc.RpcError('The sum of capital ratios must not exceed 1')

        self._running_flag = True

        mode = control_mode
        # todo: the broker must be somewhere here as a variable...

        broker = mode.get_broker() #the mode holds the broker
        builder = self._builder
        loop = mode.get_loop()

        envs = {}
        clocks = set()

        sessions = self._sessions

        session_ids = set(p.session_id for p in params)
        cur_sess_ids = set(sessions.keys())
        if cur_sess_ids:
            to_remove = cur_sess_ids.difference(session_ids)
            for sess_id in to_remove:
                sess = sessions.pop(sess_id)
                sess.stop(liquidate=True) #stop and liquidate the traders positions

        signal_handler = mode.get_signal_handler()
        # todo: each performance is stored in a group (simulation group, live group etc.)
        pfn = mode.name
        for param in params:
            session_id = param.session_id
            sess = sessions.get(sess_id, None)
            #create a new session instance.
            capital = math.floor(broker.capital * param.capital_ratio)
            max_leverage = param.max_leverage
            if not sess:
                node = builder.get_element(session_id)

                el = next(node.get_element('dom_def_id')[0].load()).decode()

                gr = node.get_element('performance')[0]

                #a session has 3 folders for storing perfomance metrics: simulation, paper, live.
                #each folder contains folders with 2 files: benchmark and parameters
                #parameters file contains the start_date and end_date (end_date is written after execution
                #either after interruption or completion), capital, max_leverage, benchmark asset etc.)
                #each folder is named by timestamp (the timestamp is the start date in the param)
                if not gr:
                    gr = builder.group('performance')

                #create a group for storing a set of files
                #we use a group
                pf = builder.file(pfn)
                gr.add_element(pf)
                node.add_element(gr)

                #todo: should we initialize the controllable here?
                stub = mode.get_trader(capital, max_leverage, broker.address)
                sess = Session(node, stub, pf, param)
                sessions[session_id] = sess

                mappings = self._exchange_mappings

                dom_def = envs.get(el, None)
                if not dom_def:
                    dom_def = data_bundle_pb2.CompoundDomainDef()
                    dom_def.ParseFromString(next(builder.get_element(el).load()))
                    envs[el] = dom_def
                else:
                    dom_def = envs[el]

                exchanges = domain.get_exchanges(dom_def, mappings)
                time_filter = signal_handler.get_time_filter(dom_def, mappings)
                clk = loop.get_clocks(exchanges)

                clocks.union(set(clk))


                time_filter.add_session(sess)
                for clock in clk:
                    time_filter.add_clock(clock)

            else:
                #todo: update the existing sessions capital and max_leverage
                sess.parameters_update(SessionParams(capital, max_leverage))

        #add the signal handler to the clocks
        #there might be some new clocks, so add the signal_handler again.
        #todo: must make sure that the signal_handler isn't added more than once.
        for clk in clocks:
            clk.add_signal_handler(signal_handler)

        # activate the signal filter (starts listening)
        signal_handler.activate()

        # event.wait() #wait for the execution to end.

        #this call will be ignored if the loop is already running
        loop.run()

        # status = self._status
        # if not self._interrupt:
        #     status.session_status = controller_pb2.COMPLETED
        # else:
        #     status.session_status = controller_pb2.INTERRUPTED
        # return status

    def _append_to_dict(self, key, value, dict_):
        v = dict_.get(key, None)
        if not v:
            dict_[key] = [value]
        else:
            v.append(value)