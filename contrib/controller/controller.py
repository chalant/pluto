import threading
import grpc
import abc

from datetime import datetime

from protos import controller_pb2_grpc, controller_pb2, clock_pb2, data_bundle_pb2

from contrib.control import domain
from contrib.control.clock import clock


class Session(object):
    def __init__(self, sess_node, dom_struct, cbl_stub, perf_file, param):
        '''controls '''
        self._sess_node = sess_node
        self._perf_file = perf_file

        self._dom_struct = dom_struct
        self._stub = cbl_stub

        #todo: put this in the algorithm class? no, this is doen server side (controllable)
        #todo:
        self._strategy = next(self._sess_node.get_element('strategy')[0].load()).decode()
        self._param = param

    @property
    def domain_struct(self):
        return self._dom_struct

    #this is called at some frequency
    def clock_update(self, clock_evt):
        #todo: send signals to the controllable service.
        # the controllable sends back a data packet through the update performance method
        pass

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



class ControlMode(abc.ABC):
    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def signal_filter(self):
        raise NotImplementedError

    def get_controllable(self):
        cbl = self._get_controllable()
        return cbl

    @abc.abstractmethod
    def get_loop(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_broker(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _get_controllable(self):
        raise NotImplementedError

class Controller(controller_pb2_grpc.ControllerServicer):
    def __init__(self, hub_client, builder):
        self._hub = hub_client
        self._builder = builder

        self._sessions = {}
        self._exchanges = exchanges = {}
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
        event = self._stop_event
        lock = self._run_lock

        with lock:
            #todo: perform other parameters checks (total leverage, etc.)
            if sum([param.capital_ratio for param in params]) > 1:
                raise grpc.RpcError('The sum of capital ratios must not exceed 1')

            self._running_flag = True

            mode = control_mode
            # todo: the broker must be somewhere here as a variable...

            broker = mode.get_broker() #the mode holds the broker
            builder = self._builder

            envs = {}

            sess_per_exg = {}

            # todo: each performance is stored in a group (simulation group, live group etc.)
            pfn = mode.name
            for param in params:
                session_id = param.session_id
                node = builder.get_element(session_id)

                el = next(node.get_element('dom_def_id')[0].load()).decode()
                dom_def = envs.get(el, None)
                if not dom_def:
                    dom_def = data_bundle_pb2.CompoundDomainDef()
                    dom_def.ParseFromString(next(builder.get_element(el).load()))
                    envs[el] = dom = domain.compute_domain(dom_def)
                else:
                    dom = envs[el]

                gr = node.get_element(pfn)[0]

                #a session has 3 folders for storing perfomance metrics: simulation, paper, live.
                #each folder contains folders with 2 files: benchmark and parameters
                #parameters file contains the start_date and end_date (end_date is written after execution
                #either after interruption or completion), capital, max_leverage, benchmark asset etc.)
                #each folder is named by timestamp (the timestamp is the start date in the param)
                if not gr:
                    gr = builder.group(pfn)

                #create a group for storing a set of files
                #we use a group
                fld = builder.group(str(datetime.utcnow()))
                pf = builder.file('performance')
                fld.add_element(pf)
                gr.add_element(fld)
                node.add_element(gr)

                #todo: we need to pass the broker_address ?
                stub = mode.get_controllable(broker.address)

                sess = Session(node, dom, stub, pf, param)
                self._sessions[session_id] = sess


                results = self._resolve_exchanges(sess.domain_struct, self._exchanges)
                for exg in results:
                    self._append_to_dict(exg, sess, sess_per_exg)

            def callback_fn(request):
                evt = request.event
                if evt == clock_pb2.STOP:
                    if self._num_clocks > 0:
                        self._num_clocks -= 1
                    if self._num_clocks == 0:
                        #once the number of listeners is reached, notify the waiting threads
                        self._running_flag = False
                        event.set()

            loop = mode.get_loop()

            signal_filter = clock.CallBackSignalFilter(mode.signal_filter, callback_fn)

            clocks = loop.get_clocks(sess_per_exg.keys())
            self._num_clocks = len(clocks)


            for cl in clocks:
                # register sessions to clock note: a session could be registered to multiple clocks
                # we need thread safety, since clock might be processes
                # listener = clock.CallBackClockListener(
                #     signal_router.register_listener(exg),
                #     callback_fn)
                # todo: how do we make sure that the signal filters will run at the same
                #  time? the clock might receive a signal while we're adding the
                #  signal filter => we need to "activate" the filter
                #  the filter ignores updates until it is activated.
                cl.add_signal_filter(signal_filter)
                for session in sess_per_exg[cl.exchange]:
                    signal_filter.add_session(session)

            # activate the signal filter (starts listening)
            signal_filter.activate()

            event.wait() #wait for the execution to end.

            status = self._status
            if not self._interrupt:
                status.session_status = controller_pb2.COMPLETED
            else:
                status.session_status = controller_pb2.INTERRUPTED
            return status

    def _resolve_exchanges(self, domain_struct, exchanges):
        cc = domain_struct.country_code
        at = domain_struct.asset_types

        # dictionary mapping keys like asset types and country codes, to sets of exchanges

        at_set = set()
        cc_set = set()

        # union of all exchanges trading the given asset types
        for c in at:
            at_set = at_set | exchanges[c]

        # union of all exchanges operating in the given countries
        cc_set = cc_set | exchanges[cc]

        # intersection of exchanges trading in the given countries and asset types
        return cc_set & at_set

    def _append_to_dict(self, key, value, dict_):
        v = dict_.get(key, None)
        if not v:
            dict_[key] = [value]
        else:
            v.append(value)