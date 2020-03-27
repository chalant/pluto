import abc

from zipline.data import data_portal as dp

from pluto.control.modes.market import market
from pluto.data.universes import universes

from protos import broker_pb2


class MarketFactory(abc.ABC):
    @abc.abstractmethod
    def get_market(self, start, end, universe_name):
        '''

        Parameters
        ----------
        start
        end
        universe_name

        Returns
        -------
        pluto.control.modes.market.market.Market
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def get_transactions(self, dt, evt, signals):
        raise NotImplementedError


class NoopMarketFactory(MarketFactory):
    def __init__(self):
        self._market = market.NoopMarket()

    def get_market(self, start, end, universe_name):
        return self._market

    def get_transactions(self, dt, evt, signals):
        return


class LiveSimulationMarketFactory(MarketFactory):
    def __init__(self, blotter_factory):
        self._calendars_cache = {}
        self._markets = {}
        self._blotter_factory = blotter_factory

    def get_market(self, start, end, universe_name):
        try:
            calendar_name = self._calendars_cache[universe_name]
            return self._markets.get(calendar_name)
        except KeyError:
            uni = universes.get_universe(universe_name)
            bundle = uni.load_bundle()
            try:
                return self._markets[uni.calendar_name]
            except KeyError:
                calendar = uni.get_calendar(start, end)
                last_session = calendar.last_session
                mkt = market.LiveSimulationMarket(
                    dp.DataPortal(
                        asset_finder=bundle.asset_finder,
                        trading_calendar=calendar,
                        first_trading_day=bundle.first_trading_day,
                        equity_minute_reader=bundle.equity_minute_bar_reader,
                        equity_daily_reader=bundle.equity_daily_bar_reader,
                        adjustment_reader=bundle.adjustment_reader,
                        last_available_session=last_session,
                        last_available_minute=calendar.minutes_for_session(last_session)[-1]),
                    calendar,
                    self._blotter_factory)
                self._markets[calendar.name] = mkt
                return mkt

    def get_transactions(self, dt, evt, signals):
        transactions = []
        commissions = []
        for txn, cms in self._chain_transactions(dt, evt, signals):
            transactions.extend(txn)
            commissions.extend(cms)
        return broker_pb2.BrokerState(
            transactions=transactions,
            commissions=commissions)

    def _chain_transactions(self, dt, evt, signals):
        for mkt in self._markets.values():
            yield mkt.get_transactions(dt, evt, signals)
