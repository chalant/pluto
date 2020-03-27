import abc

from pluto.control.events_log import events_log
from pluto.coms.utils import conversions

from protos import controller_pb2
from protos import clock_pb2


def load_implementation(strategy):
    buffer = b''
    for bytes_ in strategy.get_implementation():
        buffer += bytes_
    return buffer


class ControlMode(abc.ABC):
    def __init__(self, framework_url, process_factory):
        '''

        Parameters
        ----------
        framework_url: str
        '''

        # maps session id to a session
        self._running = {}
        self._processes = {}

        self._params_buffer = None

        self._to_update = set()
        self._to_stop = set()

        self._framework_url = framework_url

        self._events_log = self._create_events_log()

        self._process_factory = process_factory

        self._broker = self._create_broker()

    @property
    def running_sessions(self):
        return self._processes.keys()

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

        processes = self._processes
        broker = self._broker

        for p in self._to_stop:
            # remove the controllable from the dict and stop it
            # this will liquidate all the positions
            processes.pop(p).stop()

        params = self._params_buffer
        arguments = []

        for p in self._to_update:
            param = params[p]
            capital = broker.compute_capital(param.capital_ratio)
            max_leverage = broker.adjust_max_leverage(param.max_leverage)
            params = controller_pb2.RunParams(
                session_id=p,
                capital=capital,
                max_leverage=max_leverage)
            processes[p].parameter_update(params)
            arguments.append(params)

        with self._events_log.writer() as writer:
            run_params = controller_pb2.RunParamsList(
                run_params=arguments,
                timestamp=conversions.to_proto_timestamp(dt))
            writer.write_event('parameter', run_params)

    def get_process(self, session_id):
        '''

        Parameters
        ----------
        session_id: str

        Returns
        -------
        pluto.control.modes.processes.process_factory.Process
        '''
        return self._processes.get(session_id, None)

    def stop(self, params):
        # add to
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
            event=evt,
            signals=signals
        )

        for process in self._processes.values():
            process.clock_update(clock_event)

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
                for process in self._processes.values():
                    process.account_update(broker_state)
                writer.write_event('broker', broker_state)

    def add_strategies(self, directory, params):
        '''

        Parameters
        ----------
        directory : typing.pluto.interface.directory._Read
        params : typing.Iterable[pluto.controller.controller.RunParameter]

        '''
        processes = self._processes

        # note: the params include a mix of running sessions and new sessions.
        # if a sessions capital_ratio is 0, then liquidate it.

        # make sure that the total ratio doesn't exceed 1
        ratio_sum = sum([param.capital_ratio for param in params])
        if ratio_sum > 1:
            raise RuntimeError(
                'Sum of capital ratios must be below or equal to 1 but is {}'.format(ratio_sum))

        self._params_buffer = params_per_id = {p.session_id: p for p in params}
        running = self._processes

        ids = set([param.session_id for param in params])
        # stop any session that isn't present in the parameters
        running_ids = set(running.keys())
        to_run_ids = ids - running_ids

        self._to_update = update_ids = (ids & running_ids)
        self._to_stop = to_run_ids - (to_run_ids | update_ids)

        broker = self._broker
        framework_url = self._framework_url

        def filter_to_run():
            for r in to_run_ids:
                yield params_per_id.get(r)

        # create and initialize process so that we can run it later.
        per_str_id = {}
        # strategy cache
        strategies = {}

        for p in filter_to_run():
            stg_id = p.strategy_id
            stg = strategies.get(stg_id, None)
            if not stg:
                strategies[stg_id] = directory.get_strategy(stg_id)
            lst = per_str_id.get(stg_id, None)
            if not lst:
                per_str_id[stg_id] = lst = []
            lst.append(p)

        mode = self._mode_type()

        for key, values in per_str_id.items():
            implementation = load_implementation(strategies.pop(key))
            for p in values:
                # todo: should put these steps in a thread
                session_id = p.session_id
                process = self._create_process(session_id, framework_url)
                # fixme: find a general way for rounding ratios
                capital = broker.compute_capital(p.capital_ratio)
                # adjusts the max_leverage based on available margins from the broker
                max_leverage = broker.adjust_max_leverage(p.max_leverage)
                sess = p.session

                # prepare for trade simulation for live simulation case
                universe_name = sess.universe_name
                start, end = p.start, p.end

                broker.add_market(session_id, start, end, universe_name)

                process.initialize(
                    start=start,
                    end=end,
                    capital=capital,
                    max_leverage=max_leverage,
                    universe=universe_name,
                    look_back=sess.look_back,
                    data_frequency=sess.data_frequency,
                    strategy=implementation,
                    mode=mode)

                processes[session_id] = process

    @abc.abstractmethod
    def _create_broker(self):
        raise NotImplementedError

    def _create_process(self, session_id, framework_url):
        '''

        Returns
        -------
        pluto.control.modes.process.process.Process
        '''
        return self._process_factory.create_process(session_id, framework_url)

    def _create_events_log(self):
        '''

        Returns
        -------
        events_log.AbstractEventsLog
        '''
        return events_log.get_events_log(self._mode_type())

    @abc.abstractmethod
    def _mode_type(self):
        raise NotImplementedError()

    def accept_loop(self, loop):
        if not self._accept_loop(loop):
            raise ValueError("Cannot run {} with {}".format(type(self), type(loop)))

    @abc.abstractmethod
    def _accept_loop(self, loop):
        raise NotImplementedError
