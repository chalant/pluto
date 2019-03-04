from collections import OrderedDict

import numpy as np

import pandas as pd

from google.protobuf.empty_pb2 import Empty

from contrib.coms.protos import broker_pb2 as broker_msg
from contrib.coms.protos import broker_pb2_grpc as broker_rpc
from contrib.coms.utils import conversions as cv

from zipline.finance import position
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
    PositionStats
)


class Broker(object):
    '''Zipline-Server client. Converts zipline objects into proto messages and vice-versa...'''

    def __init__(self, channel, token):
        self._stub = broker_rpc.BrokerStub(channel)
        self._token = token

    def _with_metadata(self, rpc, params):
        '''If we're not registered, an RpcError will be raised. Registration is handled
        externally.'''
        return rpc(params, metadata=(('Token', self._token)))

    def order(self, asset, amount, style, order_id=None):
        return cv.to_zp_asset(
            self._with_metadata(
                self._stub.SingleOrder,
                self._create_order_param(asset, style, amount)
            )
        )

    @property
    def account(self):
        raise NotImplementedError

    @property
    def orders(self):
        orders = {}
        for resp in self._with_metadata(self._stub.Orders, Empty()):
            order = cv.to_zp_asset(resp.message)
            orders[order.id] = order
        return orders

    @property
    def transactions(self):
        return [cv.to_zp_transaction(resp.message) for resp in self._with_metadata(self._stub.Transactions, Empty())]

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


# TODO: we need some immutable of the account, portfolio, positions etc. for the strategy developer
class Account(object):
    '''Sits on top of the broker in order to "isolate" each virtual account from
    the main account...
    keeps track of its own portfolio etc.
    this account is called externally by the clock.'''

    def __init__(self, token, start_dt, capital, broker_channel, data_portal):
        self._broker = Broker(broker_channel, token)

        # keep track of the orders of this account
        self._orders_by_id = OrderedDict()
        self._orders_by_modified = {}

        self._new_orders = []

        # to keep track of executed orders
        self._processed_transactions = {}

        self._closed_orders = {}

        self._positions = {}

        self._im_port = prt.Portfolio(start_dt, capital)
        self._portfolio = prt.MutableView(self._im_port)

        self._stats = PositionStats.new()

        self._im_account = prt.Account()
        self._account = prt.MutableView(self._im_account)

        self._data_portal = data_portal

        self._unpaid_stock_dividends = {}
        self._unpaid_dividends = {}

        self._payout_last_sale_prices = {}

        self._previous_total_returns = 0

        #todo: we need to load the series from some file, since this must be persisted
        self._daily_returns_series = pd.Series(np.nan, )
        self._daily_returns_array = {}

    def _load_daily_returns_series(self):
        return pd.Series(np.nan, )

    def order(self, asset, amount, style, order_id=None):
        order = self._broker.order(asset, amount, style)
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

        return id_

    def orders(self, dt=None):
        """Retrieve the dict-form of all of the orders in a given bar or for
                the whole simulation.

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

    def _get_dict_values(self, dict_):
        return dict_.values()

    @property
    def stats(self):
        return self._compute_stats(self._positions)

    @property
    def todays_returns(self):
        return ((self._portfolio.returns + 1) / (self._previous_total_returns + 1) - 1)

    def start_of_session(self, session_label):
        #todo: before clearing everything, save everything in a file.
        # (use the session_label as index)
        self._processed_transactions.clear()
        self._orders_by_modified.clear()
        self._orders_by_id.clear()

        self._previous_total_returns = self._portfolio.returns

    def end_of_bar(self, session_ix):
        self._daily_returns_array[session_ix] = self.todays_returns

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
                for by_day in self._processed_transactions
                for txn in by_day
            ]

        return self._processed_transactions.get(dt, [])

    def update(self, dt):
        '''Updates this account'''
        # todo: maybe store the last update datetime?
        # this function is called externally at some emission rate
        self._update_account()

    def _update_account(self):
        broker = self._broker
        '''filters transactions that concern this account.'''
        transactions = broker.transactions
        '''Updates the orders positions and transactions of this account'''

        orders = broker.orders
        portfolio = self._portfolio
        positions = self._positions
        orders_by_id = self._orders_by_id

        for transaction in transactions:
            order = orders.get(transaction.order_id, None)
            # only consider the orders placed from this account
            if order is not None:
                commission = transaction.commission
                order_id = order.id
                asset = order.asset

                if order_id in orders_by_id:
                    # remove any closed order
                    if not order.open:
                        del orders_by_id[order_id]
                        # todo: save the closed orders? why?
                        self._closed_orders[order_id] = order
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
        # update everything after processing transactions, commissions etc.
        self._update_all(portfolio, positions)

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

    def _update_all(self, portfolio, positions):
        stats = self._compute_stats(positions)
        self._update_portfolio(portfolio, stats)
        self._update_zp_account(self._broker.account, portfolio, stats)

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
        self._update_all(portfolio, positions)

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

    def _update_portfolio(self, portfolio, stats):
        start_value = portfolio.portfolio_value

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

        sub_account.day_trades_remaining = main_account.day_trades_remaining

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

    def capital_change(self, amount):
        portfolio = self._portfolio

        portfolio.portfolio_value += amount
        portfolio.cash += amount
