import abc

def load_implementation(strategy):
    buffer = b''
    for bytes_ in strategy.get_implementation():
        buffer += bytes_
    return buffer

class Info(object):
    def session_id(self):
        return

    def capital_ratio(self):
        return

    def max_leverage(self):
        return


class ControlMode(abc.ABC):
    def __init__(self, framework_url):
        '''

        Parameters
        ----------
        framework_url: str
        '''
        # todo: we get the capital from the broker.
        # todo: in simulation, the broker is just a placeholder for capital and max_leverage...
        self._broker = self._create_broker()

        # maps session id to a session
        self._running = {}
        self._processes = {}

        self._params_buffer = None

        self._to_update = set()
        self._to_stop = set()

        self._framework_url = framework_url

    @property
    def running_sessions(self):
        '''

        Returns
        -------
        typing.Iterable[Info]
        '''
        return self._running.values()

    def process(self):
        # pushes all the changes to the controllables
        # todo: problem: we need to re-distribute the capital from "stopped"
        # controllables, but we don't know the capital we will get from the liquidation
        # => the capital we be assigned on the next iteration => we re-balance the
        # capital on each loop we can still attribute available capital

        processes = self._processes
        broker = self._broker

        for p in self._to_stop:
            # remove the controllable from the dict and stop it
            # this will liquidate all the positions
            processes.pop(p).stop()

        params = self._params_buffer

        for p in self._to_update:
            param = params[p]
            capital = broker.compute_capital(param.capital_ratio)
            processes[p].parameter_update(capital, param.max_leverage)

    def stop(self, params):
        # add to
        self._to_stop = {p.session_id for p in params} | self._to_stop

    def clock_update(self, dt, evt, signals):
        # update all the controllables
        # note: in live mode, we download data and update the controllables data
        # => the download is made at loop level how do we update the controllables?
        # before updating them.

        # self._broker.update(dt, evt, signals)

        for process in self._processes.values():
            process.clock_update(dt, evt, signals)

    def broker_update(self, dt, evt):
        # called before clock update by the loop
        self._broker.update(dt, evt)

    def add_strategies(self, directory, params):
        '''

        Parameters
        ----------
        directory : typing.pluto.interface.directory._Read
        params : typing.Iterable[pluto.controller.controller.RunParameter]

        '''
        # todo: must immediately create and initialize the controllables
        # since the params contain a session_id, we need to load
        # the initialization parameters "static" parameters and send them to
        # the controllable.
        processes = self._processes

        # note: the params include a mix of running sessions and new sessions.
        # if a sessions capital_ratio is 0, then liquidate it.

        # make sure that the total ratio doesn't exceed 1
        if sum([param.capital_ratio for param in params]) > 1:
            raise RuntimeError('Sum of capital ratios must be below or equal to 1')

        self._params_buffer = params_per_id = {p.session_id: p for p in params}
        # todo: create launch controllable server,
        running = self._running

        ids = set([param.session_id for param in params])
        # stop any session that isn't present in the parameters
        running_ids = set(running.keys())
        to_run_ids = ids - running_ids

        self._to_update = update_ids = (ids & running_ids) | to_run_ids
        self._to_stop = to_run_ids - (to_run_ids | update_ids)

        broker = self._broker
        framework_url = self._framework_url

        def filter_to_run():
            for r in to_run_ids:
                yield params_per_id.get(r)

        #create and initialize process so that we can run it later.
        per_str_id = {}
        #cache
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

        for key, values in per_str_id.items():
            implementation = load_implementation(strategies.pop(key))
            for p in values:
                # todo: should put these steps in a thread
                session_id = p.session_id
                process = self._create_process(session_id, framework_url)
                capital = broker.compute_capital(p.capital_ratio)
                #adjusts the max_leverage based on available margins from the broker
                max_leverage = broker.adjust_max_leverage(p.max_leverage)

                #todo: put in the necessary parameters
                # data_frequency (from the session)
                # bundle_name (note: the universe has a bundle as property)

                sess = p.session
                process.initialize(
                    start=p.start,
                    end=p.end,
                    capital=capital,
                    max_leverage=max_leverage,
                    universe=sess.universe_name,
                    look_back=sess.look_back,
                    data_frequency=sess.data_frequency,
                    strategy=implementation)

                processes[session_id] = process

    def _add_strategy(self, controllables, strategy, exchanges, capital_ratio, max_leverage):
        # todo: if a strategy is already running on a set of exchanges, update its parameters
        # todo: how to set capital?

        # todo: we must delegate the capital update to the broker.

        # the controllable's account gets updated each minute using the data of the master
        # account. The controllable filters the data.  So when updating capital, we update capital
        # ratio and maybe max leverage.
        # todo: parameters update must be sent ON MINUTE_END or SESSION_END, it will then get processed
        # by the controllable on the next BAR or SESSION_START

        # todo: the controllables make trades by calling the broker service.
        # the next minute, the broker send "state" (positions, etc.) each controllable tracks its own
        # orders, so it filters-out its own positions etc.

        # todo PROBLEM: in daily mode (live) we can place orders before the market closes, so that
        # they get processed on the same day, or we can place them the next day before the market
        # closes, since in the simulation, we use next day's closing price.
        # ideally, we should buy as close as possible to the closing price => so buy before market
        # closes. PROBLEM: we might get very different results than the ones in simulation,
        # since the simulation is done on the next day's closing price (the price might change
        # drastically) => performance might get better or worse. With higher frequency, we might
        # get more realistic slippage simulation. We could try and use the same closing price
        # as the previous day... => it would be more realistic than buying or selling on the next
        # trading day's closing price.
        # NOTE: the live/paper should be the STANDARD, we should adjust back-testing parameters
        # to be a close as possible to live/paper. => We should be able to modify the run parameters
        # like the "slippage price to use" etc.
        # so the STANDARD is: buy as close as possible to the current price. and since we don't have
        # enough data in daily mode, we will be using the previous closing price for slippage simulation.

        # todo: need to add a LAST_BAR event for live daily strategies since the BAR event is emited
        # each minute. NOTE: the TRADE_END is emitted some minutes before the SESSION_END event
        # so that we can place trades before the market closes (live). In live we leave a margin
        # of say, 5 to 10 minutes before market close.
        # in live, the closing price, is the price 5 minute before the market closes...

        # todo: order fillings should be simulated each minute... (we will repeat the same price for
        # 5 minutes...), should also be done controller-side, since it is the one that holds the
        # broker.

        # todo: add a TRADE_END event, emitted some minutes before market closes...
        # PROBLEM: how do we update the broker state? In backtesting mode, it is updated depending
        # on the data_frequency (daily or minutely) in live, it is updated minutely.

        pass

    @abc.abstractmethod
    def _update(self, dt, evt, signals):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_broker(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_process(self, session_id, framework_url):
        '''

        Returns
        -------
        pluto.control.modes.process.process.Process
        '''
        raise NotImplementedError(self._create_process.__name__)
