from collections import OrderedDict
from functools import partial

import numpy as np
import pandas as pd
from google.protobuf.empty_pb2 import Empty
from logbook import Logger

from protos import broker_pb2 as broker_msg, broker_pb2_grpc as broker_rpc

from zipline.finance import position, order
from zipline import protocol as prt
from zipline.assets import Future
from zipline.finance.execution import (
    MarketOrder,
    LimitOrder,
    StopOrder,
    StopLimitOrder
)
from zipline.finance._finance_ext import (
    calculate_position_tracker_stats,
    PositionStats,
    update_position_last_sale_prices,
)

from pluto.coms.utils import conversions as cv
from pluto.utils import saving
from pluto.coms.client.protos import account_state_pb2 as acc

log = Logger("ZiplineLog")

#todo: is called externally by the controllable... keeps track of broker data, and used by the account
class BrokerClient(object):
    '''A client to a broker service. Converts zipline objects into proto messages and vice-versa...'''

    def __init__(self, channel):
        self._stub = broker_rpc.BrokerStub(channel)
        self._transactions = []
        self._orders = {}

    def order(self, asset, amount, style, order_id=None):
        return cv.to_zp_asset(self._stub.SingleOrder(order_params = self._create_order_param(asset, style, amount)))

    @property
    def account(self):
        raise NotImplementedError

    @property
    def orders(self):
        return self._orders

    @property
    def transactions(self):
        for transaction in self._transactions:
            yield transaction

    def get_transactions(self, dt):
        """returns transactions starting from dt"""
        for resp in self._stub.Transactions(dt=cv.to_proto_timestamp(dt)), Empty():
            yield cv.to_zp_transaction(resp)


    def cancel(self, order_id, relay_status=True):
        raise NotImplementedError

    def cancel_all_orders_for_asset(self, asset, warn=False, relay_status=True):
        raise NotImplementedError

    def _create_order_param(self, asset, style, amount):
        # todo: check the "get_stop_price" methods
        t = type(style)
        asset_ = cv.to_proto_asset(asset)
        if t is MarketOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.MARKET_ORDER,
                amount=amount
            )
        elif t is LimitOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.LIMIT_ORDER,
                amount=amount,
                limit_price=t.get_limit_price(True)
            )
        elif t is StopOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.STOP_ORDER,
                amount=amount,
                stop_price=t.get_stop_price(True)
            )
        elif t is StopLimitOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.STOP_LIMIT_ORDER,
                amount=amount,
                limit_price=t.get_limit_price(True),
                stop_price=t.get_stop_price(True)
            )
        else:
            raise NotImplementedError

    #todo: this is called to update the broker client state
    def update(self, broker_data):
        """updates the state of the broker, this is a remote method and must be called before
        everything else."""
        for transaction in broker_data.transactions:
            self._transactions.append(cv.to_zp_transaction(transaction))

        for order in broker_data.orders:
            #over-writes the current orders states
            self._orders[order.order_id] = order

        self._account = cv.to_zp_account(broker_data.account)

    def clear_transactions(self):
        """this method is called after the transactions have been processed"""
        self._transactions.clear()


class Ledger(saving.Savable):
    """

    Sits on top of the broker in order to "isolate" each virtual account from
    the main account...
    keeps track of its own portfolio etc.
    this account is called externally by the clock.
    """

    def __init__(self, blotter):
        """

        Parameters
        ----------
        broker: BrokerClient
        """
        self._blotter = blotter

        # keep track of the orders of this account
        self._orders_by_id = OrderedDict()
        self._orders_by_modified = {}

        # to keep track of executed orders
        self._processed_transactions = {}

        self._positions = {}

        self._start_dt = None

        self._im_port = None
        self._portfolio = None

        self._stats = PositionStats.new()

        self._im_account = prt.Account()
        self._account = prt.MutableView(self._im_account)

        self._unpaid_stock_dividends = {}
        self._unpaid_dividends = {}

        self._payout_last_sale_prices = {}

        self._previous_total_returns = 0

        # todo: we need to load the series from some file, since this must be persisted
        self._daily_returns_series = None

        self._new_session = False

        self._last_checkpoint = None

    def process_order(self, order):
        dt = order.dt
        id_ = order.id
        orders_by_id = self._orders_by_id
        orders_by_modified = self._orders_by_modified
        try:
            dt_orders = orders_by_modified[order.dt]
        except KeyError:
            orders_by_modified[dt] = OrderedDict([(id_, order)])
            orders_by_id[id_] = order
        else:
            orders_by_id[id_] = dt_orders[id_] = order
            # to preserve the order of the orders by modified date
            dt_orders.move_to_end(id_, last=True)

        orders_by_id.move_to_end(id_, last=True)

    def orders(self, dt=None):
        """
        Retrieve the dict-form of all of the orders in a given bar or for the whole simulation.

        Parameters
        ----------
        dt : pd.Timestamp or None, optional
            The particular datetime to look up order for. If not passed, or
            None is explicitly passed, all of the orders will be returned.

        Returns
        -------
        orders : list[dict]
            The order information.
        """
        if dt is None:
            # orders by id is already flattened
            return [o.to_dict() for o in self._orders_by_id.values()]

        return [o.to_dict() for o in self._get_dict_values(self._orders_by_modified.get(dt, {}))]

    def process_transaction(self, transaction):
        asset = transaction.asset

    def update_acount(self, master_account):
        pass

    def _get_dict_values(self, dict_):
        return dict_.values()

    @property
    def capital_base(self):
        return self._im_port.starting_cash

    @property
    def first_session(self):
        return self._start_dt

    @property
    def stats(self):
        return self._compute_stats(self._positions)

    @property
    def todays_returns(self):
        return ((self._portfolio.returns + 1) / (self._previous_total_returns + 1) - 1)

    @property
    def daily_returns(self):
        return self._daily_returns_series.values

    def start_of_session(self, session_label):
        self._processed_transactions.clear()
        self._orders_by_modified.clear()
        self._orders_by_id.clear()

        self._previous_total_returns = self._portfolio.returns

        self._new_session = True

    def end_of_bar(self, dt):
        # called before end_of_session
        drs = self._daily_returns_series
        if not self._new_session:
            drs = drs.drop(drs.tail(1).index)
        self._daily_returns_series = drs.append(pd.Series([self.todays_returns], index=[dt]))

    def end_of_session(self, dt):
        self._daily_returns_series[dt] = self.todays_returns
        self._new_session = False

    def on_initialize(self, dt, capital):
        self._start_dt = dt

        self._im_port = prt.Portfolio(dt, capital)
        self._portfolio = prt.MutableView(self._im_port)

        self._daily_returns_series = pd.Series()

    def on_stop(self, dt):
        # todo: save anything that needs to be stored (daily returns series)
        pass

    def on_liquidate(self, dt):
        # todo: handle liquidation
        pass

    def get_state(self, dt):
        return acc.AccountState(
            portfolio=cv.to_proto_portfolio(self._im_port),
            account=cv.to_proto_account(self._im_account),
            last_checkpoint=cv.to_proto_timestamp(dt),
            orders=[order.to_dict() for order in self._orders_by_id.values()],
            first_session=cv.to_proto_timestamp(self._start_dt.to_datetime()),
            daily_returns=[acc.Return(timestamp=ts, value=val) for ts, val in
                           self._daily_returns_series.items()]
        ).SerializeToString()

    def _restore_state(self, state):
        account = acc.AccountState()
        account.ParseFromString(state)

        self._daily_returns_series = pd.Series(
            {pd.Timestamp(cv.to_datetime(r.timestamp)): r.value for r in account.daily_returns}
        )
        self._start_dt = pd.Timestamp(cv.to_datetime(account.first_session), tz='UTC')
        self._im_port = portfolio = cv.to_zp_portfolio(account.portfolio)
        self._portfolio = prt.MutableView(portfolio)
        self._im_account = im_account = cv.to_zp_account(account.account)
        self._account = prt.MutableView(im_account)
        # todo: we should optimise memory usage here...
        self._positions = positions = portfolio.positions

        self._orders_by_id = {order.id: cv.to_zp_order(order) for order in state.orders}

        last_checkpoint = account.last_checkpoint

        # note: the broker keeps the whole history of transactions server-side
        # server side.
        blotter = self._blotter

        for transaction in blotter.get_transactions(last_checkpoint):
            # only consider the transaction that occurred after the last checkpoint
            self._update_per_transaction(transaction, positions, portfolio, blotter.orders)

    def _update_per_transaction(self, transaction, positions, portfolio, all_orders):
        # only consider transactions from orders placed by this account
        orders_by_id = self._orders_by_id

        order_id = transaction.order_id
        o = orders_by_id.get(order_id)
        if o:
            order = all_orders[order_id]
            commission = transaction.commission
            asset = order.asset
            # remove any closed order
            if not order.open:
                del orders_by_id[order_id]
            # update our orders with the new values
            else:
                # update the state of the order from the broker.
                orders_by_id[order_id] = order
                self._update_position(positions, asset, transaction)
            self._update_cash(asset, transaction, positions, portfolio)
            transaction_dict = transaction.to_dict()
            trx_dt = transaction.dt
            try:
                self._append_to_list(self._processed_transactions[trx_dt], transaction_dict)
            except KeyError:
                self._processed_transactions[trx_dt] = [transaction_dict]
            self._process_commission(positions, asset, commission)

    def _order_from_dict(self, dict_order):
        return order.Order(
            dict_order['dt'], dict_order['asset'], dict_order['amount'], dict_order['stop'],
            dict_order['limit'], dict_order['filled'], dict_order['commission'], dict_order['id']
        )

    @property
    def portfolio(self):
        return self._im_port

    @property
    def positions(self):
        # we will set the last_trade_price to the amount we paid for the transaction.
        # in the future we will use data directly fetched from realtime streaming.
        # so, when we call the get process_transactions method everything gets updated
        # account, positions, portfolio etc.
        return [pos.to_dict() for pos in self._positions.values() if pos.amount != 0]

    def transactions(self, dt):
        """Retrieve the dict-form of all of the transactions in a given bar or
                for the whole simulation.

                Parameters
                ----------
                dt : pd.Timestamp or None, optional
                    The particular datetime to look up transactions for. If not passed,
                    or None is explicitly passed, all of the transactions will be
                    returned.

                Returns
                -------
                transactions : list[dict]
                    The transaction information.
                """
        if dt is None:
            # flatten the by-day transactions
            return [
                txn
                for by_day in self._processed_transactions.values()
                for txn in by_day
            ]

        return self._processed_transactions.get(dt, [])

    def update(self,
               dt, data_portal,
               trading_calendar,
               transactions, orders,
               commissions, target_capital=None,
               portfolio_value_adjustment=0.0,
               handle_non_market_minutes=False):

        '''Updates this account'''
        #todo: this function gets called by the controllable (trader) to update the account data...
        # receives broker state... the broker is a structure that is a copy of the server-side
        # data...

        # todo: maybe store the last update datetime?
        # this function is called externally at some emission rate
        portfolio = self._portfolio

        # syncs with last_sale_price
        # todo: if the data frequency is bigger than the emission_rate, don't sync until we've reached
        # the next session with an offset of period "data_frequency"

        #todo: why do we need to handle non market minutes?

        #FIXME: we should set the latest trading price as an argument in the update
        # method
        if handle_non_market_minutes:
            previous_minute = trading_calendar.previous_minute(dt)
            get_price = partial(
                data_portal.get_adjusted_value,
                field='price',
                dt=previous_minute,
                perspective_dt=dt,
                data_frequency=self._data_frequency,
            )

        else:
            get_price = partial(
                data_portal.get_scalar_asset_spot_value,
                field='price',
                dt=dt,
                data_frequency=data_frequency,
            )

        update_position_last_sale_prices(self.positions, get_price, dt)

        self._update_ledger(portfolio, transactions, commissions, orders)

        # update capital after updating the portfolio
        if target_capital is not None:
            capital_change_amount = target_capital - (portfolio.portfolio_value - portfolio_value_adjustment)

            portfolio.portfolio_value += capital_change_amount
            portfolio.cash += capital_change_amount

            log.info('Processing capital change to target %s at %s. Capital '
                     'change delta is %s' % (target_capital, dt,
                                             capital_change_amount))

            #todo: we return the capital change summary after updating the cash, this should be
            # added in the performance packet
            return {
                'capital_change':
                    {'date': dt,
                     'type': 'cash',
                     'target': target_capital,
                     'delta': capital_change_amount}
            }

        #todo: we also need to handle max leverage updates...

    def _update_ledger(self, portfolio, transactions, commissions, orders):
        """Updates portfolio, account and positions"""
        positions = self._positions

        for transaction in transactions:
            self._update_per_transaction(transaction, positions, portfolio, broker.orders)
        # update everything after processing transactions, commissions etc.

        for order in orders:
            self.process_order(order)



        self._update_all(portfolio, positions, broker.account)

    def _update_cash(self, asset, transaction, positions, portfolio):
        price = transaction.price
        amount = transaction.amount
        if isinstance(asset, Future):
            payout_last_sale_prices = self._payout_last_sale_prices
            try:
                old_price = payout_last_sale_prices[asset]
            except KeyError:
                payout_last_sale_prices[asset] = price
            else:
                position = positions[asset]
                pos_amount = position.amount

                self._cash_flow(
                    portfolio,
                    self._calculate_payout(
                        asset.price_multiplier,
                        pos_amount,
                        old_price,
                        price,
                    )
                )
                if pos_amount + amount == 0:
                    del payout_last_sale_prices[asset]
                else:
                    payout_last_sale_prices[asset] = price
        else:
            # update the cash flow with the price, number of shares and commission
            # todo: what about taxes and fees?
            self._cash_flow(
                portfolio,
                -((price * amount) - transaction.commission)
            )

    @staticmethod
    def _calculate_payout(multiplier, amount, old_price, price):
        return (price - old_price) * multiplier * amount

    @staticmethod
    def _append_to_list(list_, element):
        list_.append(element)

    def _update_all(self, portfolio, positions, account):
        stats = self._compute_stats(positions)
        self._update_portfolio(portfolio, stats, positions)
        self._update_zp_account(account, portfolio, stats)

    def _process_commission(self, positions, asset, cost):
        if asset in positions:
            positions[asset].adjust_commission_cost_basis(asset, cost)

    # this function is called once a day (called externally by the trade_control or metrics tracker?)
    def handle_market_open(self, midnight_dt, data_portal):
        positions = self._positions
        splits = data_portal.get_splits(tuple(positions.keys()), midnight_dt)
        portfolio = self._portfolio
        self._process_splits(portfolio, positions, splits)
        self._process_dividends(
            portfolio,
            positions,
            midnight_dt,
            data_portal.asset_finder,
            data_portal.adjustment_reader
        )
        # update everything
        self._update_all(portfolio, positions, self._account)

    def _process_splits(self, portfolio, positions, splits):
        if splits:
            total_leftover_cash = 0

            for asset, ratio in splits:
                if asset in positions:
                    # Make the position object handle the split. It returns the
                    # leftover cash from a fractional share, if there is any.
                    pos = positions[asset]
                    leftover_cash = pos.handle_split(asset, ratio)
                    total_leftover_cash += leftover_cash
            if total_leftover_cash > 0:
                self._cash_flow(portfolio, total_leftover_cash)

    def _process_dividends(self, portfolio, positions, next_session, asset_finder, adjustment_reader):
        # Earn dividends whose ex_date is the next trading day. We need to
        # check if we own any of these stocks so we know to pay them out when
        # the pay date comes.
        unpaid_dividends = self._unpaid_dividends
        unpaid_stock_dividends = self._unpaid_stock_dividends

        held_assets = set(positions)
        if held_assets:
            cash_dividends = adjustment_reader.get_dividends_with_ex_date(
                held_assets,
                next_session,
                asset_finder
            )
            stock_dividends = (
                adjustment_reader.get_stock_dividends_with_ex_date(
                    held_assets,
                    next_session,
                    asset_finder
                )
            )

            # Earning a dividend just marks that we need to get paid out on
            # the dividend's pay-date. This does not affect our cash yet.
            self._earn_dividends(
                positions,
                cash_dividends,
                stock_dividends,
                unpaid_dividends,
                unpaid_stock_dividends
            )

        # Pay out the dividends whose pay-date is the next session. This does
        # affect out cash.
        self._cash_flow(
            portfolio,
            self._pay_dividends(
                positions,
                next_session,
                unpaid_stock_dividends,
                unpaid_stock_dividends
            )
        )

    def _earn_dividends(self, positions, cash_dividends, stock_dividends, unpaid_dividends, unpaid_stock_dividends):
        """Given a list of dividends whose ex_dates are all the next trading
                day, calculate and store the cash and/or stock payments to be paid on
                each dividend's pay date.

                Parameters
                ----------
                cash_dividends : iterable of (asset, amount, pay_date) namedtuples

                stock_dividends: iterable of (asset, payment_asset, ratio, pay_date)
                    namedtuples.
                """
        for cash_dividend in cash_dividends:
            # self._dirty_stats = True  # only mark dirty if we pay a dividend

            # Store the earned dividends so that they can be paid on the
            # dividends' pay_dates.
            div_owed = positions[cash_dividend.asset].earn_dividend(
                cash_dividend,
            )
            try:
                unpaid_dividends[cash_dividend.pay_date].append(div_owed)
            except KeyError:
                unpaid_dividends[cash_dividend.pay_date] = [div_owed]

        for stock_dividend in stock_dividends:

            div_owed = positions[
                stock_dividend.asset
            ].earn_stock_dividend(stock_dividend)
            try:
                unpaid_stock_dividends[stock_dividend.pay_date].append(
                    div_owed,
                )
            except KeyError:
                unpaid_stock_dividends[stock_dividend.pay_date] = [
                    div_owed,
                ]

    def _pay_dividends(self, positions, next_trading_day, unpaid_dividends, unpaid_stock_dividends):
        """
                Returns a cash payment based on the dividends that should be paid out
                according to the accumulated bookkeeping of earned, unpaid, and stock
                dividends.
                """
        net_cash_payment = 0.0

        try:
            payments = unpaid_dividends[next_trading_day]
            # Mark these dividends as paid by dropping them from our unpaid
            del unpaid_dividends[next_trading_day]
        except KeyError:
            payments = []

        # representing the fact that we're required to reimburse the owner of
        # the stock for any dividends paid while borrowing.
        for payment in payments:
            net_cash_payment += payment['amount']

        # Add stock for any stock dividends paid.  Again, the values here may
        # be negative in the case of short positions.
        try:
            stock_payments = unpaid_stock_dividends[next_trading_day]
        except KeyError:
            stock_payments = []

        for stock_payment in stock_payments:
            payment_asset = stock_payment['payment_asset']
            share_count = stock_payment['share_count']
            # note we create a Position for stock dividend if we don't
            # already own the asset
            if payment_asset in positions:
                pos = positions[payment_asset]
            else:
                pos = positions[payment_asset] = position.Position(
                    payment_asset,
                )

            pos.amount += share_count

        return net_cash_payment

    def _update_portfolio(self, portfolio, stats, positions):
        start_value = portfolio.portfolio_value

        portfolio.positions = positions
        # todo: the're some additional values to compute( net_value - futures maintenance margins?)
        portfolio.positions_value = position_value = stats.net_value
        portfolio.position_exposure = stats.net_exposure
        portfolio.portfolio_value = end_value = portfolio.cash + position_value

        pnl = end_value - start_value
        if start_value != 0:
            returns = pnl / start_value
        else:
            returns = 0.0

        portfolio.pnl += pnl
        portfolio.returns = ((1 + portfolio.returns) * (1 + returns) - 1)

    # todo: no need to update the account. We will be using data from the overall account.
    # just display the main account. (not the sub-account (since there is no way to determine
    # margin requirements accuretly.
    # todo: we still need to update leverage data etc.
    # note: it is used in metrics to track the maximum leverage.
    # note: the account is meant to be used by the strategy developper to track some values, and put
    # some logic based on those values. It is not used in the metrics tracker or else where...
    def _update_zp_account(self, main_account, portfolio, stats):
        # todo: this only applies to cash account or reg T margin accounts. Risk-Based margin accounts
        # are more complex to handle.
        # todo: need to handle
        # todo: what about sma ? (is used for computing liquidations etc.)
        # todo: what about buying power?
        sub_account = self._account
        portfolio_value = portfolio.portfolio_value
        cash = portfolio.cash

        sub_account.settled_cash = cash
        # sub_account.accrued_interest = ?
        sub_account.equity_with_loan = portfolio_value
        sub_account.total_positions_value = portfolio_value - cash
        sub_account.total_positions_exposure = portfolio.positions_exposure

        # positions_value = portfolio.positions_value
        #
        # main_positions_value = main_account.total_positions_value

        # #fixme: we assume that we are subjected to rule based margins... with risk based margins,
        # # each sub-portfolio has its own margin requirements (based on its risk) which adds up
        # # to the margin requirements of the main portfolio. TIMS or SPAN
        # sub_account.regt_margin = (main_account.regt_margin * positions_value) / main_positions_value
        # init_marg_pr = main_account.initial_margin_requirement / main_positions_value
        # maint_marg_pr = main_account.maintenance_margin_requirement / main_positions_value
        #
        # sub_account.initial_margin_requirement = init_mr = init_marg_pr * positions_value
        # sub_account.maintenance_margin_requirement = maint_marg_pr * positions_value
        #
        # prev_pv = 0.0 #todo: we need the previous portfolio_value.
        # #fixme: checks if it is a margin account of not? or we need to specify it externally?
        # if init_mr == 0:
        #     sub_account.buying_power = min(portfolio_value,prev_pv) - init_mr
        # else:
        #     sub_account.buying_power = (min(portfolio_value,prev_pv) - init_mr) * 4

        sub_account.available_funds = cash
        sub_account.excess_liquidity = cash
        sub_account.cushion = cash / portfolio_value if portfolio_value else np.nan

        sub_account.day_trades_remaining = main_account.day_trades_remaining #todo: ?

        # this is used in the metrics module.
        sub_account.net_liquidation = portfolio_value
        sub_account.gross_leverage, sub_account.net_leverage = self._calculate_period_stats(
            portfolio_value,
            stats
        )
        sub_account.leverage = sub_account.gross_leverage

    def _calculate_period_stats(self, portfolio_value, stats):
        if portfolio_value == 0:
            gross_leverage = net_leverage = np.inf
        else:
            gross_leverage = stats.gross_exposure / portfolio_value
            net_leverage = stats.net_exposure / portfolio_value

        return gross_leverage, net_leverage

    def _update_position(self, positions, asset, transaction):
        if asset not in positions:
            pos = position.Position(asset)
            positions[asset] = pos
        else:
            pos = positions[asset]
        pos.update(transaction)

        if pos.amount == 0:
            del positions[asset]

    def _cash_flow(self, portfolio, amount):
        portfolio.cash_flow += amount
        portfolio.cash += amount

    def _compute_stats(self, positions):
        calculate_position_tracker_stats(positions, self._stats)
        return self._stats
