import threading
import grpc
import abc
import math

from datetime import datetime

from protos import controller_pb2_grpc, controller_pb2, clock_pb2, data_bundle_pb2

from contrib.control import domain
from contrib.utils import stream


class Session(object):
    def __init__(self, sess_node, sess_id, dom_id, trader, perf_file, param):
        self._sess_node = sess_node
        self._perf_file = perf_file

        self._sess_id = sess_id
        self._dom_id = dom_id
        self._stub = trader

        # todo: put this in the algorithm class? no, this is done server side (controllable)
        self._strategy = next(self._sess_node.get_element('strategy')[0].load()).decode()
        self._param = param

        self._param_update = None

    @property
    def id(self):
        return self._sess_id

    @property
    def domain_id(self):
        return self._dom_id

    @property
    def capital_ratio(self):
        return self._param.capital_ratio

    # this is called at some frequency
    def clock_update(self, clock_evt):
        # todo: send signals to the controllable service.
        # the controllable sends back a data packet through the update performance method
        param_update = self._param_update
        if param_update:
            pass

    def update_parameters(self, params):
        # todo: update stub parameters on the session_end event.
        self._param_update = params

    def update_performance(self, stream):
        #
        '''receives a stream of bytes (that represents a performance packet) and appends it to a
        binary file.'''

        bytes_ = b''
        for d in stream:
            bytes_ = bytes_ + d.data

        # each set of bytes is on a line.
        bytes_ = bytes_ + b'\n'

        # appends the data to the perf file...
        self._perf_file.store(bytes_)  # todo: how do we specify the append mode?

    def update_account(self, data):
        self._stub.BrokerState(stream.chunk_bytes(data, 16 * 1024))

    def stop(self, params):
        pass

    def watch(self):
        pass

    def liquidate(self):
        pass


class _Stop(object):
    def __init__(self, control_mode, params, liquidate=False):
        self._mode = control_mode
        self._liquidate = liquidate
        self._params = params

    def __call__(self, clock_factory):
        mode = self._mode
        sessions = mode.sessions
        liquidate = self._liquidate

        to_stop = set(session.id for session in sessions) & set(p.session_id for p in self._params)

        if liquidate:
            for session in to_stop:
                mode._liquidate(session)
        else:
            for session in to_stop:
                mode.stop(session)


class _Run(object):
    def __init__(self, builder, control_mode, params, exchange_per_country_code):
        self._builder = builder
        self._ctl_mode = control_mode
        self._params = params
        self._exchange_per_country_code = exchange_per_country_code

    def __call__(self, clock_factory):
        '''
        runs a session
            1)load/create and store domain
            2)load/create and store sessions (and strategies)
        '''

        mode = self._ctl_mode
        builder = self._builder

        envs = {}
        clocks = set()

        sessions = mode.sessions
        params = self._params

        session_ids = set(p.session_id for p in params)
        cur_sess_ids = set(session.id for session in sessions)
        # todo: we need to make sure that all the positions have been liquidated before adding new
        # sessions and updating parameters, HOW?
        # possible solution: update capital each end of session...
        # problem? we are rebalancing capital, which means that all returns are redestributed
        # accross all strategies...
        # capital is redistributed each session_end event? => should let the client decide...
        # we still need a solution for distributing cash from liquidated assets...
        # maybe put the cash in a special variable (un-assigned capital) in the tracker object
        # the tracker could update the sessions capital as it receives new un-assigned capital...
        # each capital change event is documented in the performance packet.

        # schedule all sessions that are not in the params for liquidation
        if cur_sess_ids:
            for sess_id in cur_sess_ids.difference(session_ids):
                mode.liquidate(sessions[sess_id])

        signal_handler = mode.get_signal_handler()

        # todo: each performance is stored in a group (simulation group, live group etc.)
        pfn = mode.name
        for param in params:
            session_id = param.session_id
            sess = sessions.get(sess_id, None)
            # create a new session instance.
            if not sess:
                node = builder.get_element(session_id)
                el = next(node.get_element('dom_def_id')[0].load()).decode()
                gr = node.get_element('performance')[0]

                # a session has 3 folders for storing perfomance metrics: simulation, paper, live.
                # each folder contains folders with 2 files: benchmark and parameters
                # parameters file contains the start_date and end_date (end_date is written after execution
                # either after interruption or completion), capital, max_leverage, benchmark asset etc.)
                # each folder is named by timestamp (the timestamp is the start date in the param)
                if not gr:
                    gr = builder.group('performance')

                    # create a group for storing a set of files
                    # we use a group
                    node.add_element(gr)

                pf = gr.get_element(pfn)[0]
                if not pf:
                    pf = builder.file(pfn)
                    gr.add_element(pf)

                mappings = self._exchange_per_country_code

                dom_def = envs.get(el, None)
                if not dom_def:
                    dom_def = data_bundle_pb2.CompoundDomainDef()
                    dom_def.ParseFromString(next(builder.get_element(el).load()))
                    envs[el] = dom_def
                else:
                    dom_def = envs[el]

                dom_id = domain.domain_id(dom_def)
                sess = Session(node, session_id, dom_id, mode.get_trader(), pf, param)
                sessions[session_id] = sess
                # add the session to the tracker for capital and leverage updates

                exchanges = domain.get_exchanges(dom_def, mappings)
                clk = clock_factory(exchanges)

                clocks = clocks.union(set(clk))
                domain_filter = mode.get_domain_filter(dom_def, clocks, mappings)

                # todo: the session must be added after session end event
                domain_filter.add_session(sess)

            else:
                #  todo: the loop doesn't know the events... => it will get executed on the next iteration,
                #  but each observer will execute it on a specific event (SESSION_END) exception: stop and liquidate,
                #  will be executed directly since we will be needing the capital in order to deploy the new
                #  sessions.
                mode.update_session(sess, params)

        # add the signal handler to the clocks
        # there might be some new clocks, so add the signal_handler again.
        # todo: must make sure that the signal_handler isn't added more than once.
        for clk in clocks:
            clk.add_signal_handler(mode)


class Controller(controller_pb2_grpc.ControllerServicer):
    def __init__(self, hub_client, builder):
        self._hub = hub_client
        self._builder = builder

        self._sessions = {}
        self._exchange_mappings = exchanges = {}

        self._stop_event = threading.Event()

        # classify exchanges by country_codes and asset_types
        for exc in hub_client.get_exchanges():
            country_code = exc.country_code
            name = exc.name
            self._append_to_dict(country_code, name, exchanges)
            for at in exc.asset_types:
                self._append_to_dict(at, name, exchanges)

    def stop(self, loop, control_mode, params, liquidate=False):
        loop.execute(_Stop(control_mode, params, liquidate))

    def watch(self, session_id):
        for packet in self._sessions[session_id].watch():
            yield packet

    def run(self, params, control_mode, loop):
        '''
        runs a session
            1)load/create and store domain
            2)load/create and store sessions (and strategies)
        '''
        # event = self._stop_event
        # lock = self._run_lock

        # todo: perform other parameters checks (total leverage, etc.)
        if sum([param.capital_ratio for param in params]) > 1:
            # todo: should we avoid raising an exception here? => might interrupt the execution
            raise grpc.RpcError('The sum of capital ratios must not exceed 1')

        loop.execute(_Run(self._builder, self._sessions, control_mode, params, self._exchange_mappings))
        loop.run()  # this call will be ignored if the loop is already running

    def _append_to_dict(self, key, value, dict_):
        v = dict_.get(key, None)
        if not v:
            dict_[key] = [value]
        else:
            v.append(value)
