import abc
from contrib.control import domain

class SessionParams(object):
    __slots__ = ['capital_ratio', 'max_leverage']

    def __init__(self, capital_ratio, max_leverage):
        self.capital_ratio = capital_ratio
        self.max_leverage = max_leverage

class Process(object):
    def start(self, client_url, server_url):
        pass

class ControlMode(abc.ABC):
    def __init__(self):
        self._broker = broker = self._create_broker()
        self._trader = self._create_trader(broker.address)

        self._domain_filters = {}

    @property
    def sessions(self):
        return self._broker.sessions

    def liquidate(self, session):
        self._domain_filters[session.domain_id].liquidate(session)

    @property
    @abc.abstractmethod
    def name(self):
        raise NotImplementedError

    def update(self, dt):
        for filter_ in self._domain_filters.values():
            filter_.update(dt)

    def update_session(self, session, params):
        self._domain_filters[session.domain_id].update_session((session, params))

    def clock_update(self, clock_evt):
        for filter_ in self._domain_filters.values():
            filter_.clock_update(clock_evt)

    def get_domain_filter(self, dom_def, clocks, exchange_mappings):
        id_ = domain.domain_id(dom_def)
        t_filters = self._domain_filters
        filter_ = t_filters.get(id_, None)
        broker = self._broker
        if not filter_:
            self._domain_filters[id_] = filter_ = self._create_domain_filter(
                dom_def,
                exchange_mappings,
                clocks,
                id_,
                broker)
        return filter_

    @abc.abstractmethod
    def _create_domain_filter(self, dom_def, clocks, exchange_mappings, domain_id, broker):
        raise NotImplementedError

    def get_trader(self):
        self._create_trader(self._broker.address)

    @abc.abstractmethod
    def _create_broker(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_trader(self, broker_url):
        raise NotImplementedError
