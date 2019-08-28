from protos import data_bundle_pb2
from protos import clock_pb2
from protos import controller_pb2_grpc

from contrib.control import domain
from contrib.control.clock import clock

import grpc

import abc

class Session(object):
    def __init__(self, sess_node, dom_struct, cbl_stub, perf_file, param):
        '''controls '''
        self._sess_node = sess_node
        self._perf_file = perf_file
        self._dom_struct = dom_struct
        self._stub = cbl_stub

        #todo: put this in the algorithm class?
        self._strategy = next(self._sess_node.get_element('strategy')[0].load()).decode()
        self._param = param

    @property
    def domain_struct(self):
        return self._dom_struct

    #this is called at some frequency
    def clock_update(self, clock_evt):
        #todo: send signals to the controllable service.
        self._stub.Clock

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
    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def signal_router(self):
        raise NotImplementedError

    def get_controllable(self):
        cbl = self._get_controllable()
        return cbl

    @abc.abstractmethod
    def _get_controllable(self):
        raise NotImplementedError

class Controller(controller_pb2_grpc.ControllerServicer):
    def __init__(self, hub_client, builder):
        self._hub = hub_client
        self._builder = builder

        self._sessions = {}
        self._exchanges = exchanges = {}

        #classify exchanges by country_codes and asset_types
        for exc in hub_client.get_exchanges():
            country_code = exc.country_code
            name = exc.name
            self._append_to_dict(country_code, name, exchanges)
            for at in exc.asset_types:
                self._append_to_dict(at, name, exchanges)

        self._running = False
        self._num_listeners = 0

    @property
    def running(self):
        return self._running

    def stop(self):
        self._running = False

    def Watch(self, session_id):
        for packet in self._sessions[session_id].watch():
            yield packet

    def run(self, params, control_mode):
        '''
        runs a session
            1)load/create and store domain
            2)load/create and store sessions (and strategies)
        '''
        if not self._running:
            self._running = True

            if sum([param.capital_ratio for param in params]) > 1:
                self._running = False
                raise grpc.RpcError('The sum of capital ratios must not exceed 1')

            mode = control_mode
            builder = self._builder

            envs = {}

            sess_per_exg = {}
            sessions = self._sessions

            # todo: each performance is stored in a group (simulation group, live group etc.)
            pfn = mode.name
            for param in params:
                node = builder.get_element(param.session_id)

                el = next(node.get_element('dom_def_id')[0].load()).decode()
                dom_def = envs.get(el, None)
                if not dom_def:
                    dom_def = data_bundle_pb2.CompoundDomainDef()
                    dom_def.ParseFromString(next(builder.get_element(el).load()))
                    envs[el] = dom = domain.compute_domain(dom_def)
                else:
                    dom = envs[el]

                gr = node.get_element(pfn)[0]
                if not gr:
                    gr = builder.group(pfn)
                    node.add_element(gr)

                stub = mode.get_controllable(node.id)

                # todo: create and name a file to store the performance data
                sess = Session(node, dom, stub, file, param)
                self._sessions[sess.id] = sessions


                results = self._resolve_exchanges(sess.domain_struct, self._exchanges)
                for exg in results:
                    self._append_to_dict(exg, sess, sess_per_exg)

            clocks = []


            def callback_fn(request):
                evt = request.event
                if evt == clock_pb2.STOP or evt == clock_pb2.LIQUIDATE:
                    self._num_listeners -= 1
                    if self._num_listeners == 0:
                        self._running = False

            for exg in sess_per_exg.keys():
                signal_router = mode.signal_router #returns a clock listener service
                # register sessions to clock note: a session could be registered to multiple clocks
                # we need thread safety, since clock might be processes
                cl = signal_router.get_clock(exg)
                listener = clock.CallBackClockListener(
                    signal_router.register_listener(exg),
                    callback_fn)
                for session in sess_per_exg[exg]:
                    listener.add_session(session)
                self._num_listeners += 1
                clocks.append(cl)

            # todo: start the clocks at the same time => adjust the starting time with the lag induced from
            #  the loop

            #note: this doesn't block
            #todo: ensure that this doesn't block
            for clk in clocks:
                clk.start() #todo: the start method can take a "lag offset" parameter
        else:
            #doesn't do anything when it is already running
            pass

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