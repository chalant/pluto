import abc
import threading

from pandas import Timestamp

from grpc import RpcError

from pluto.control.events_log import events_log
from pluto.coms.utils import conversions
from pluto.broker import broker_service
from pluto.utils import stream

from protos import controller_pb2
from protos import clock_pb2


# todo: the locking mechanism should be done in live and live-simulation
# and are only used for reading and writing process and recovering dicts
# only times this is used is for either reading the process dict or transferring
# process between recovery and process dict
# => create an abstract class that encapsulates this behavior
class ControlMode(abc.ABC):
    def __init__(self,
                 framework_url,
                 process_factory,
                 thread_pool):
        '''

        Parameters
        ----------
        framework_url: str
        server: grpc.Server
        thread_pool: concurrent.futures.ThreadPoolExecutor
        '''

        self._params_buffer = {}

        self._to_update = set()
        self._to_stop = set()

        self._framework_url = framework_url

        self._events_log = self._create_events_log()

        self._process_factory = process_factory
        self._broker = brk = broker_service.BrokerService(self._create_broker())
        process_factory.set_broker_service(brk)

        self._thread_pool = thread_pool
        self._lock = threading.Lock()

        self._process_manager = self._create_process_manager()

    @property
    def running_sessions(self):
        return self._process_manager.active_session_ids

    def process(self, dt):
        # todo: we need to make sure that all the positions have been liquidated before adding new
        # sessions and updating parameters, HOW?
        # possible solution: update capital each end of session...
        # problem? we are rebalancing capital, which means that all returns are redistributed
        # across all strategies...
        # capital is redistributed each session_end event? => should let the client decide...

        # pushes all the changes to the controllables
        # todo: problem: we need to re-distribute the capital from "stopped"
        # controllables, but we don't know the capital we will get from the liquidation, and
        # estimation isn't very "reliable"
        # => the capital we be assigned on the next iteration => we re-balance the
        # capital on each loop we can still attribute available capital

        broker = self._broker

        real_dt = conversions.to_proto_timestamp(
            Timestamp.utcnow())

        manager = self._process_manager
        to_stop = self._to_stop

        for p in to_stop:
            # remove the controllable from the dict and stop it
            # this will liquidate all the positions

            # set the session to liquidation
            # todo: first check if stopping is followed by a liquidation flag
            broker.set_to_liquidation(p)
            # submit the stop call as a thread, since it blocks until it
            # stops
            try:
                manager.stop(p)  # todo: need liquidate parameter
            except KeyError:
                # process is in recovery, and the signal will be executed
                # at a later time.
                pass

        params = self._params_buffer
        arguments = []

        for p in self._to_update:
            param = params[p]
            capital = broker.compute_capital(param.capital_ratio)
            max_leverage = broker.adjust_max_leverage(param.max_leverage)
            params = controller_pb2.RunParams(
                session_id=p,
                capital=capital,
                max_leverage=max_leverage,
                real_ts=real_dt)
            try:
                manager.get_process(p).parameter_update(params)
            except KeyError:
                # process is recovering, request will be executed after
                # recovery from the log
                pass
            arguments.append(params)

        with self._events_log.writer() as writer:
            run_params = controller_pb2.RunParamsList(
                run_params=arguments,
                timestamp=conversions.to_proto_timestamp(dt))
            writer.write_event('parameter', run_params)
            writer.write_event(
                'stop',
                controller_pb2.StopRequests(
                    requests=[
                        controller_pb2.StopRequest(session_id=id_)
                        for id_ in to_stop]))

    def get_process(self, session_id):
        '''

        Parameters
        ----------
        session_id: str

        Returns
        -------
        pluto.control.modes.processes.process_factory.Process
        '''
        return self._process_manager.get_process(session_id)

    def stop(self, params):
        # add to scheduled stoppage
        self._to_stop = {p.session_id for p in params} | self._to_stop

    def clock_update(self, dt, evt, signals):
        '''

        Parameters
        ----------
        dt: pandas.Timestamp
        evt: int
        signals:

        '''
        # self._broker.update(dt, evt, signals)

        clock_event = clock_pb2.ClockEvent(
            timestamp=conversions.to_proto_timestamp(dt),
            real_ts=conversions.to_proto_timestamp(Timestamp.utcnow()),
            event=evt,
            signals=signals
        )

        manager = self._process_manager

        for process in manager.active_processes:
            try:
                process.clock_update(clock_event)
            except RpcError:
                manager.recover(process)

        # todo: non-blocking!
        with self._events_log.writer() as writer:
            writer.write_event('clock', clock_event)

    def update(self, dt, evt, signals):
        with self._events_log.writer() as writer:
            # we initialize the events log here, since this is the
            # first method to be called by the loop
            if evt == clock_pb2.SESSION_START:
                writer.initialize(dt)
            writer.write_datetime(dt)

            broker_state = self._broker.update(
                dt,
                evt,
                signals)

            if broker_state:
                manager = self._process_manager
                broker_state.real_dt = conversions.to_proto_timestamp(
                    Timestamp.utcnow())

                bytes_ = broker_state.SerializeToString()

                for process in manager.active_processes:
                    # todo this whole instruction should run in a thread
                    try:
                        # todo: we need to launch each call in its own thread
                        # each controllable can queue calls from the controller
                        process.account_update(stream.chunk_bytes(bytes_))
                    except RpcError:
                        manager.recover(process)
                writer.write_event('broker', broker_state)

    def add_strategies(self, directory, params):
        '''

        Parameters
        ----------
        directory : typing.pluto.interface.directory._Read
        params : typing.Iterable[pluto.controller.controller._RunParameter]

        '''
        manager = self._process_manager

        # note: the params include a mix of running sessions and new sessions.
        # if a sessions capital_ratio is 0, then liquidate it.

        # make sure that the total ratio doesn't exceed 1
        ratio_sum = sum([param.capital_ratio for param in params])
        if ratio_sum > 1:
            raise RuntimeError(
                'Sum of capital ratios must be below or equal to 1 but is {}'.format(ratio_sum))

        self._params_buffer = params_per_id = {p.session_id: p for p in params}

        ids = set([param.session_id for param in params])

        running_ids = manager.all_session_ids

        self._to_update = ids & running_ids
        # stop any process that is running and is not in the request
        self._to_stop = running_ids - ids

        broker = self._broker
        framework_url = self._framework_url

        def filter_to_run(ppi, ids):
            for r in ids:
                yield ppi.get(r)

        # # create and initialize process so that we can run it later.
        # per_str_id = {}
        # # strategy cache
        # strategies = {}
        #
        # for p in filter_to_run():
        #     stg_id = p.strategy_id
        #     stg = strategies.get(stg_id, None)
        #     if not stg:
        #         strategies[stg_id] = directory.get_strategy(stg_id)
        #     lst = per_str_id.get(stg_id, None)
        #     if not lst:
        #         per_str_id[stg_id] = lst = []
        #     lst.append(p)

        mode = self.mode_type

        # todo: should put this step in a thread
        # only initialize processes that aren't already running
        for p in filter_to_run(params_per_id, ids - running_ids):
            session_id = p.session_id
            process = self._create_process(session_id, framework_url)
            capital = broker.compute_capital(p.capital_ratio)
            # adjusts the max_leverage based on available margins from the broker
            max_leverage = broker.adjust_max_leverage(p.max_leverage)
            sess = p.session

            # prepare for trade simulation for live simulation case
            universe_name = sess.universe_name
            start, end = p.start, p.end

            broker.add_market(
                session_id,
                sess.data_frequency,
                start,
                end,
                universe_name)
            broker.add_session_id(session_id)

            process.initialize(
                start=start,
                end=end,
                capital=capital,
                max_leverage=max_leverage,
                mode=mode)

            manager.add_process(process)

    def _create_process(self, session_id, framework_url):
        '''

        Returns
        -------
        pluto.control.modes.process.process.Process
        '''
        return self._process_factory.create_process(
            session_id,
            framework_url)

    def _create_events_log(self):
        '''

        Returns
        -------
        events_log.AbstractEventsLog
        '''
        return events_log.get_events_log(self.mode_type)

    def accept_loop(self, loop):
        if not self._accept_loop(loop):
            raise ValueError(
                "Cannot run {} with {}".format(
                    type(self),
                    type(loop)))

    @abc.abstractmethod
    def _create_process_manager(self):
        '''

        Returns
        -------
        pluto.control.modes.processes.process_manager.ProcessManager
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def _accept_loop(self, loop):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_broker(self):
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def mode_type(self):
        raise NotImplementedError()
