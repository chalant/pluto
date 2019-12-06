import abc

class Controllable(object):
    def __init__(self, stub, strategy, exchanges):
        self._stub = stub
        self._strategy = strategy

        self._exchanges = exchanges
        self._num_exchanges = len(exchanges)

        #todo initialize the stub with the proper parameters
        stub.Initiliaze()

    def update(self, ts, evt, signals):
        stub = self._stub

        stub.Cloc_Update()

class ControlMode(abc.ABC):
    def __init__(self):
        #todo: we get the capital from the broker.
        self._broker = broker = self._create_broker()
        self._trader = self._create_trader(broker.address)

        self._domain_filters = {}
        #maps session id to a session
        self._sessions = []
        self._controllables = {}

        self._strategy_to_exchanges = {}
        self._exchanges_to_strategies = {}

    @property
    def sessions(self):
        return self._broker.sessions

    def liquidate(self, session):
        self._domain_filters[session.domain_id].liquidate(session)

    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError

    def update(self, dt, evt, signals):
        #update all the controllables
        #note: in live mode, we download data and update the controllables data
        #before updating them.
        for controllable in self._controllables:
            controllable.update(dt, evt, signals)

    def add_strategies(self, params):
        controllables = self._controllables

        if sum([param.capital_ratio for param in params]) > 1:
            raise RuntimeError()

        capital = self._broker.capital

        for param in params:
            self._add_strategy(
                controllables,
                param.strategy,
                param.exchanges,
                self._compute_capital(capital, param.capital_ratio),
                param.max_leverage)

    def _add_strategy(self, controllables, strategy, exchanges, capital_ratio, max_leverage):
        #todo: if a strategy is already running on a set of exchanges, update its parameters
        #todo: how to set capital?

        #todo: we must delegate the capital update to the broker.

        #the controllable's account gets updated each minute using the data of the master
        # account. The controllable filters the data.  So when updating capital, we update capital
        # ratio and maybe max leverage.
        #todo: parameters update must be sent ON MINUTE_END or SESSION_END, it will then get processed
        # by the controllable on the next BAR or SESSION_START

        #todo: the controllables make trades by calling the broker service.
        # the next minute, the broker send "state" (positions, etc.) each controllable tracks its own
        # orders, so it filters-out its own positions etc.

        #todo: if data frequency is 'day' we can't emit the BAR event each minute...
        # we should only emit the BAR event once at each execution close.
        # in live mode, we must track data more frequently (executed trades etc.)
        # controller side, (in live) the clock should run each minute, controllable side, if
        # it is running on daily mode, BAR event must be skipped until we reach the LAST_BAR,
        # then it emits a BAR event.

        #todo PROBLEM: in daily mode (live) we can place orders before the market closes, so that
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

        #todo: need to add a LAST_BAR event for live daily strategies since the BAR event is emited
        # each minute. NOTE: the TRADE_END is emitted some minutes before the SESSION_END event
        # so that we can place trades before the market closes (live). In live we leave a margin
        # of say, 5 to 10 minutes before market close.
        # in live, the closing price, is the price 5 minute before the market closes...

        #todo: order fillings should be simulated each minute... (we will repeat the same price for
        # 5 minutes...), should also be done controller-side, since it is the one that holds the
        # broker.

        #todo: add a TRADE_END event, emitted some minutes before market closes...
        # PROBLEM: how do we update the broker state? In backtesting mode, it is updated depending
        # on the data_frequency (daily or minutely) in live, it is updated minutely.

        exchanges.sort()
        id_ = hash(tuple(exchanges) + (strategy.id))

        cbl = controllables.get(id_, None)
        if cbl:
            #todo set parameters
            cbl.update_parameters()
        else:
            stub = self._create_trader(self._broker.address)
            controllable = Controllable(stub, strategy, exchanges)
            controllables[id_] = controllable

    def _compute_capital(self, capital, capital_ratio):
        return capital * capital_ratio

    @abc.abstractmethod
    def _create_broker(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_trader(self, broker_url):
        raise NotImplementedError
