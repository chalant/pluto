import abc

from zipline import protocol

from pluto.control.controllable import synchronization_states as ss

from protos import clock_pb2

class Market(abc.ABC):
    @abc.abstractmethod
    def add_blotter(self, session_id):
        raise NotImplementedError

    @abc.abstractmethod
    def get_transactions(self, dt, evt, signals):
        raise NotImplementedError

    @abc.abstractmethod
    def get_blotter(self, session_id):
        '''

        Parameters
        ----------
        session_id

        Returns
        -------

        '''
        raise NotImplementedError

class NoopMarket(Market):
    def add_blotter(self, session_id):
        pass

    def get_transactions(self, dt, evt, signals):
        return

    def get_blotter(self, session_id):
        return

class LiveSimulationMarket(Market):
    def __init__(self, data_portal, calendars, universe, blotter_factory):
        '''

        Parameters
        ----------
        data_portal
        calendars
        universe
        blotter_factory: pluto.control.modes.market.blotter_factory.SimulationBlotterFactory
        '''
        self._dp = dtp = data_portal
        self._sst = ss.Tracker(calendars)

        self._blotter_factory = blotter_factory
        self._current_dt = None

        self._universe = universe
        self._current_data = protocol.BarData(
            data_portal=dtp,
            simulation_dt_func=self.current_dt
        )

        super(LiveSimulationMarket, self).__init__()

    def current_dt(self):
        return self._current_dt

    def get_transactions(self, dt, evt, signals):
        s = self._sst.aggregate(dt, evt, signals)
        if s:
            dt, evt, exchanges = s
            self._current_dt = dt

            #only simulate on each bar or trade_end event
            if evt == clock_pb2.BAR or evt == clock_pb2.TRADE_END:
                for blotter in self._blotter_factory.blotters:
                    new_transactions, new_commissions, closed_orders = \
                        blotter.get_transactions(self._current_data)
                    blotter.prune_orders(closed_orders)
                    yield new_transactions, new_commissions

    def add_blotter(self, session_id):
        self._blotter_factory.add_blotter(
            session_id,
            self._universe)

    def get_blotter(self, session_id):
        return self._blotter_factory.get_blotter(session_id)

#whats this?
class MarketAggregate(Market):
    def __init__(self):
        self._markets = []

    def add_market(self, market):
        self._markets.append(market)

    def get_transactions(self, dt, evt, signals):
        for market in self._markets:
            yield market.get_transactions(dt, evt, signals)

    def add_blotter(self, session_id):
        for market in self._markets:
            market.add_blotter(session_id)