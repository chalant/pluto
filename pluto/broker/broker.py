import math
import abc
import uuid

class Broker(abc.ABC):
    @abc.abstractmethod
    def compute_capital(self, ratio):
        raise NotImplementedError

    @abc.abstractmethod
    def adjust_max_leverage(self, max_leverage):
        raise NotImplementedError

    def update(self, dt, evt, signals):
        return self._update(dt, evt, signals)

    @abc.abstractmethod
    def _update(self, dt, evt, signals):
        raise NotImplementedError

    @abc.abstractmethod
    def add_market(self, session_id, data_frequency, start, end, universe_name):
        raise NotImplementedError

    @abc.abstractmethod
    def order(self, session_id, order):
        raise NotImplementedError

    @abc.abstractmethod
    def cancel(self, session_id, order_id, relay_status=True):
        raise NotImplementedError

    @abc.abstractmethod
    def cancel_all_orders_for_asset(self, session_id, asset, warn=False, relay_status=True):
        raise NotImplementedError

    @abc.abstractmethod
    def reject(self, session_id, order_id, reason=''):
        raise NotImplementedError

    @abc.abstractmethod
    def hold(self, session_id, order_id, reason=''):
        raise NotImplementedError

    @abc.abstractmethod
    def execute_cancel_policy(self, session_id, event):
        raise NotImplementedError

class SimulationBroker(Broker):
    def __init__(self, capital, max_leverage):
        self._capital = capital
        self._max_leverage = max_leverage

    def compute_capital(self, ratio):
        return math.floor(self._capital * ratio)

    def adjust_max_leverage(self, max_leverage):
        return max_leverage

    def _update(self, dt, evt, signals):
        return

    def add_market(self, session_id, data_frequency, start, end, universe_name):
        pass

    def order(self, session_id, order):
        return

    def cancel(self, session_id, order_id, relay_status=True):
        pass

    def cancel_all_orders_for_asset(self, session_id, asset, warn=False, relay_status=True):
        pass

    def reject(self, session_id, order_id, reason=''):
        pass

    def hold(self, session_id, order_id, reason=''):
        pass

    def execute_cancel_policy(self, session_id, event):
        pass


class LiveBroker(Broker):
    def __init__(self):
        pass

    def compute_capital(self, ratio):
        pass

    def adjust_max_leverage(self, max_leverage):
        pass

class LiveSimulationBroker(Broker):
    def __init__(self, capital, max_leverage, market_factory):
        '''

        Parameters
        ----------
        capital: float
        max_leverage: float
        market_factory: pluto.control.modes.market.factory.MarketFactory
        '''
        self._capital = capital
        self._max_leverage = max_leverage
        self._market_factory = market_factory
        self._session_to_market = {}

    def _update(self, dt, evt, signals):
        return self._market_factory.get_transactions(dt, evt, signals)

    def compute_capital(self, ratio):
        return math.floor(self._capital * ratio)

    def adjust_max_leverage(self, max_leverage):
        pass

    def add_market(self, session_id, data_frequency, start, end, universe_name):
        mkt = self._market_factory.get_market(data_frequency, universe_name, start, end)
        mkt.add_blotter(session_id)
        self._session_to_market[session_id] = mkt

    def order(self, session_id, order):
        blotter = self._get_blotter(session_id)
        amount = order.amount

        ms = blotter.max_shares

        if amount == 0:
            return None

        elif amount > ms:
            raise OverflowError(
                "Can't order more than {}".format(ms))

        #generate id for the order
        order.id = uuid.uuid4().hex
        blotter.open_orders[order.asset].append(order)
        blotter.orders[order.id] = order
        blotter.new_orders.append(order)

    def cancel(self, session_id, order_id, relay_status=True):
        self._get_blotter(session_id).cancel(order_id, relay_status)

    def cancel_all_orders_for_asset(self, session_id, asset, warn=False, relay_status=True):
        self._get_blotter(session_id).cancel_all_orders_for_asset(
            asset,
            warn,
            relay_status)

    def reject(self, session_id, order_id, reason=''):
        self._get_blotter(session_id).reject(order_id, reason)

    def hold(self, session_id, order_id, reason=''):
        self._get_blotter(session_id).hold(order_id, reason)

    def execute_cancel_policy(self, session_id, event):
        self._get_blotter(session_id).execute_cancel_policy(
            session_id,
            event)

    def _get_blotter(self, session_id):
        return self._session_to_market[session_id].get_blotter(session_id)
