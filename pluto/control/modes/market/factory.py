import abc

from zipline.data import data_portal as dp

from pluto.control.modes.market import market
from pluto.data.universes import universes
from pluto.coms.utils import conversions

from protos import broker_pb2
from protos import clock_pb2


class MarketFactory(abc.ABC):
    @abc.abstractmethod
    def get_market(self, data_frequency, universe_name, start, end):
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

    def get_market(self, data_frequency, universe_name, start, end):
        return self._market

    def get_transactions(self, dt, evt, signals):
        return


class LiveSimulationMarketFactory(MarketFactory):
    def __init__(self, blotter_factory):
        self._calendars_cache = {}
        self._markets = {'daily': {}, 'minute': {}}
        self._blotter_factory = blotter_factory

    def _create_market(self, data_frequency, universe, calendar_name, start, end):
        bundle = universe.load_bundle()
        calendar = universe.get_calendar(start, end)
        last_session = calendar.last_session
        mkt = market.LiveSimulationMarket(
            dp.DataPortal(
                asset_finder=bundle.asset_finder,
                trading_calendar=calendar,
                first_trading_day=calendar.first_session,
                equity_minute_reader=bundle.equity_minute_bar_reader,
                equity_daily_reader=bundle.equity_daily_bar_reader,
                adjustment_reader=bundle.adjustment_reader,
                last_available_session=last_session,
                last_available_minute=calendar.minutes_for_session(last_session)[-1]),
            data_frequency,
            universe,
            calendar,
            self._blotter_factory)
        self._markets[data_frequency][calendar_name] = mkt
        return mkt

    def get_market(self, data_frequency, universe_name, start, end):
        try:
            return self._markets[data_frequency][universe_name]
        except KeyError:
            uni = universes.get_universe(universe_name)
            calendars = uni.calendars
            if len(calendars) > 1:
                agg_mkt = market.MarketAggregate()
                for name in calendars:
                    mkt = self._markets[data_frequency].get(name, None)
                    if not mkt:
                        mkt = self._create_market(data_frequency, uni, name, start, end)
                        agg_mkt.add_market(mkt)
                self._markets[data_frequency][universe_name] = agg_mkt
                return agg_mkt
            else:
                calendar = calendars[0]
                try:
                    return self._markets[calendar]
                except KeyError:
                    return self._create_market(data_frequency, uni, calendar, start, end)

    def get_transactions(self, dt, evt, signals):
        transactions = []
        commissions = []
        
        #todo we need to pass the signal to update

        markets = []
        mkt = self._markets
        markets.extend(mkt['daily'].values())
        markets.extend(mkt['minute'].values())
        for txn, cms in self._chain_transactions(
                dt,
                evt,
                markets,
                signals):
            transactions.extend(
                conversions.to_proto_transaction(
                    t.to_dict())
                for t in txn)
            commissions.extend(
                conversions.to_proto_commission(c)
                for c in cms)
        return broker_pb2.BrokerState(
            transactions=transactions,
            commissions=commissions)

    def _chain_transactions(self, dt, evt, markets, signals):
        for mkt in markets:
            for transactions in mkt.get_transactions(dt, evt, signals):
                if transactions:
                    yield transactions
