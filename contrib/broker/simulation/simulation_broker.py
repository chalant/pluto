from copy import copy
from collections import defaultdict

import pandas as pd

from logbook import Processor

from zipline.assets import Equity, Future, Asset
from zipline.data import data_portal as dp
from zipline.protocol import BarData
from zipline.utils.pandas_utils import normalize_date

from zipline.utils.input_validation import expect_types
from zipline.finance.order import Order
from zipline.finance.cancel_policy import NeverCancel
from zipline.finance.blotter import simulation_blotter
from zipline.finance.metrics import tracker, load
from zipline.finance.slippage import (
    DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
    VolatilityVolumeShare,
    FixedBasisPointsSlippage,
)
from zipline.finance.commission import (
    DEFAULT_PER_CONTRACT_COST,
    FUTURE_EXCHANGE_FEES_BY_SYMBOL,
    PerContract,
    PerShare,
)

from contrib.control import clock


class SimulationBroker(object):
    def __init__(self, equity_slippage=None, future_slippage=None,
                 equity_commission=None, future_commission=None,cancel_policy=None):
        self._cancel_policy = cancel_policy if cancel_policy else NeverCancel()
        self._open_orders = defaultdict(list)

        self._orders = {}

        self._new_orders = []

        self._max_shares = int(1e+11)

        self._slippage_models = {
            Equity: equity_slippage or FixedBasisPointsSlippage(),
            Future: future_slippage or VolatilityVolumeShare(
                volume_limit=DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
            ),
        }
        self._commission_models = {
            Equity: equity_commission or PerShare(),
            Future: future_commission or PerContract(
                cost=DEFAULT_PER_CONTRACT_COST,
                exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
            ),
        }

        self._data_portal = None

        self._current_data = None
        self._asset_finder = None

        self._run_dt = None

        # Processor function for injecting the algo_dt into
        # user prints/logs.
        def inject_algo_dt(record):
            if 'algo_dt' not in record.extra:
                record.extra['algo_dt'] = self._get_run_dt

        self._processor = Processor(inject_algo_dt)

        self._last_sync_time = pd.NaT

        self._metrics_tracker = None

        self._transactions = []

    def _create_bar_data(self, universe_func, data_portal, get_dt, data_frequency, calendar, restrictions):
        return BarData(
            data_portal=data_portal,
            simulation_dt_func=get_dt,
            data_frequency=data_frequency,
            trading_calendar=calendar,
            restrictions=restrictions,
            universe_func=universe_func)

    def _get_run_dt(self):
        return self._run_dt

    # todo: we need an offset: each of these events must be generated BEFORE the events of the client-side (say
    # a minute)
    def run(self, bundler, trading_calendar, capital_base, start_dt,
            end_dt, restrictions, universe_func, emission_rate='daily'):
        """

        Parameters
        ----------
        bundler
        trading_calendar : trading_calendars.TradingCalendar
        capital_base : float
        start_dt : pandas.Timestamp
        end_dt : pandas.Timestamp
        restrictions
        universe_func
        emission_rate

        Returns
        -------

        """

        cl = clock.MinuteSimulationClock(trading_calendar, start_dt, end_dt)
        for dt, evt in cl:
            if evt == clock.INITIALIZE:
                self._load_attributes(
                    bundler.load(), cl.calendar, normalize_date(start_dt), normalize_date(end_dt),
                    capital_base, restrictions, universe_func, emission_rate, bundler.data_frequency
                )
            elif evt == clock.SESSION_END:
                #todo: update the state of the broker...
                self._cleanup_expired_assets(dt, )
            elif evt == clock.BAR:
                self._update(self._current_data, self._blotter, self._metrics_tracker)

    def _load_data_portal(self, calendar, asset_finder, first_trading_day,
                          equity_minute_bar_reader, equity_daily_bar_reader, adjustment_reader):
        return dp.DataPortal(
            asset_finder, calendar, first_trading_day,
            equity_daily_bar_reader, equity_minute_bar_reader, adjustment_reader
        )

    def _load_attributes(self, bundle, calendar, start_dt, end_dt, capital_base,
                         restrictions, universe_func, emission_rate, data_frequency):

        equity_minute_reader = bundle.equity_minute_bar_reader
        self._asset_finder = asset_finder = bundle.asset_finder

        self._current_data = self._create_bar_data(
            universe_func,
            self._load_data_portal(
                calendar, asset_finder, equity_minute_reader.first_trading_day,
                equity_minute_reader, bundle.equity_daily_bar_reader, bundle.adjustment_reader
            ),
            self._get_run_dt,
            data_frequency,
            calendar,
            restrictions
        )

        self._metrics_tracker = tracker.MetricsTracker(
            calendar, start_dt, end_dt, capital_base, emission_rate,
            data_frequency, asset_finder, load('default')
        )

    def _update(self, bar_data, blotter, metrics_tracker):
        """

        Parameters
        ----------
        bar_data : BarData
        blotter : simulation_blotter.SimulationBlotter
        metrics_tracker : tracker.MetricsTracker

        """
        new_transactions, new_commissions, closed_orders = blotter.get_transactions(bar_data)
        blotter.prune_orders(closed_orders)

        for transaction in new_transactions:
            metrics_tracker.process_transaction(transaction)

            # since this order was modified, record it
            order = blotter.orders[transaction.order_id]
            metrics_tracker.process_order(order)
            self._transactions.append(transaction)

        for commission in new_commissions:
            metrics_tracker.process_commission(commission)

    def transactions(self):
        return self._transactions


    def account(self):
        self._update(self._current_data, self._blotter, self._metrics_tracker)
        return self._metrics_tracker.account

    @expect_types(asset=Asset)
    def order(self, asset, amount, style, order_id=None):
        #todo: must update current dt.
        """Place an order.

        Parameters
        ----------
        asset : zipline.assets.Asset
            The asset that this order is for.
        amount : int
            The amount of shares to order. If ``amount`` is positive, this is
            the number of shares to buy or cover. If ``amount`` is negative,
            this is the number of shares to sell or short.
        style : zipline.finance.execution.ExecutionStyle
            The execution style for the order.
        order_id : str, optional
            The unique identifier for this order.

        Returns
        -------
        order_id : str or None
            The unique identifier for this order, or None if no order was
            placed.

        Notes
        -----
        amount > 0 :: Buy/Cover
        amount < 0 :: Sell/Short
        Market order:    order(asset, amount)
        Limit order:     order(asset, amount, style=LimitOrder(limit_price))
        Stop order:      order(asset, amount, style=StopOrder(stop_price))
        StopLimit order: order(asset, amount, style=StopLimitOrder(limit_price,
                               stop_price))
        """
        # something could be done with amount to further divide
        # between buy by share count OR buy shares up to a dollar amount
        # numeric == share count  AND  "$dollar.cents" == cost amount

        if amount == 0:
            # Don't bother placing orders for 0 shares.
            return None
        elif amount > self._max_shares:
            # Arbitrary limit of 100 billion (US) shares will never be
            # exceeded except by a buggy algorithm.
            raise OverflowError("Can't order more than %d shares" %
                                self._max_shares)

        is_buy = (amount > 0)
        order = Order(
            #todo: datetime
            dt=self._current_dt,
            asset=asset,
            amount=amount,
            stop=style.get_stop_price(is_buy),
            limit=style.get_limit_price(is_buy),
            id=order_id
        )

        open_orders = self._open_orders[order.asset]
        open_orders.append(asset)
        self._orders[order.id] = order
        self._new_orders.append(order)

        asset_type = type(asset)
        slippage = self._slippage_models[asset_type]
        commission = self._commission_models[asset_type]

        commissions = []

        for order, txn in slippage.simulate(self._current_data, asset, open_orders):
            additional_commission = commission.calculate(order, txn)

            if additional_commission > 0:
                commissions.append({
                    "asset": order.asset,
                    "order": order,
                    "cost": additional_commission
                })

            order.filled += txn.amount
            order.commission += additional_commission

            order.dt = txn.dt

            self._transactions.append(txn)

            if not order.open:
                try:
                    open_orders.remove(order)
                except ValueError:
                    pass
            if len(open_orders) == 0:
                del self._open_orders[asset]

        return order.id

    def portfolio(self):
        self._update(self._current_data, self._blotter, self._metrics_tracker)
        return self._metrics_tracker.portfolio

    def subscribe_to_market_data(self, asset):
        pass

    def get_realtime_bars(self, assets, frequency):
        pass

    def subscribed_assets(self):
        pass

    def get_last_traded_dt(self, asset):
        pass

    def cancel_order(self, order_id, relay_status=True):
        """

        Parameters
        ----------
        order_id : int

        Returns
        -------
        None

        """
        self._blotter.cancel(relay_status)

    def get_spot_value(self, assets, field, dt, data_frequency):
        pass

    def _cleanup_expired_assets(self, dt, position_assets, data_portal, blotter, metrics_tracker):
        """
        Clear out any assets that have expired before starting a new sim day.

        Performs two functions:

        1. Finds all assets for which we have open orders and clears any
           orders whose assets are on or after their auto_close_date.

        2. Finds all assets for which we have positions and generates
           close_position events for any assets that have reached their
           auto_close_date.
        """

        def past_auto_close_date(asset):
            acd = asset.auto_close_date
            return acd is not None and acd <= dt

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [asset for asset in position_assets if past_auto_close_date(asset)]
        for asset in assets_to_clear:
            metrics_tracker.process_close_position(asset, dt, data_portal)

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.
        assets_to_cancel = [
            asset for asset in [order.asset for order in self._orders_by_id.values() if order.open]
            if past_auto_close_date(asset)
        ]
        for asset in assets_to_cancel:
            blotter.cancel_all_orders_for_asset(asset)

        # Make a copy here so that we are not modifying the list that is being
        # iterated over.
        for order in copy(blotter.new_orders):
            if order.status == ORDER_STATUS.CANCELLED:
                metrics_tracker.process_order(order)
                blotter.new_orders.remove(order)
