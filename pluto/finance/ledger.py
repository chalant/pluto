import collections

import numpy as np

from zipline.assets import Future
from zipline.finance import ledger
from zipline import protocol
from zipline.finance._finance_ext import (
    calculate_position_tracker_stats,
    PositionStats
)

from protos import ledger_state_pb2 as acc

from pluto.coms.utils import conversions as cv
from pluto.utils import saving


class LiveLedger(saving.Savable):
    def __init__(self, capital, data_frequency, start_date, look_back):
        self._look_back = look_back

        # metric only uses array
        self.daily_returns_array = collections.deque()

        self._stats = PositionStats.new()
        self._positions = None

        self._immutable_account = account = protocol.Account()
        self._account = protocol.MutableView(account)

        # an immutable portfolio is necessary to prevent the user from overriding values
        self._immutable_portfolio = ip = protocol.Portfolio(
            start_date,
            capital_base=capital)

        self._portfolio = protocol.MutableView(ip)

        self._position_tracker = ledger.PositionTracker(data_frequency)

        self._processed_transactions = {}

        self._orders_by_modified = {}
        self._orders_by_id = collections.OrderedDict()

        self._payout_last_sale_prices = {}

        self._start_dt = start_date
        self._session_count = -1

        self._dirty_portfolio_ = False
        self._dirty_account = True

        self._last_checkpoint = None

    @property
    def first_session(self):
        return self._start_dt

    @property
    def _dirty_portfolio(self):
        return self._dirty_portfolio_

    @_dirty_portfolio.setter
    def _dirty_portfolio(self, value):
        if value:
            self._dirty_account = self._dirty_portfolio_ = value
        else:
            self._dirty_portfolio_ = value

    @property
    def portfolio(self):
        self._update_portfolio()
        return self._immutable_portfolio

    @property
    def daily_returns(self):
        return np.array(self.daily_returns_array)

    @property
    def account(self):
        return self._immutable_account

    def update_account(self, main_account):
        # todo: this is called by the controller loop: passes

        sub_account = self._account
        portfolio = self._portfolio
        stats = self._position_tracker.stats

        # todo: this only applies to cash account or reg T margin accounts. Risk-Based margin accounts
        # are more complex to handle.
        # todo: need to handle
        # todo: what about sma ? (is used for computing liquidations etc.)
        # todo: what about buying power?
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

        sub_account.day_trades_remaining = main_account.day_trades_remaining  # todo: ?

        # this is used in the metrics module.
        sub_account.net_liquidation = portfolio_value
        sub_account.gross_leverage, sub_account.net_leverage = self._calculate_period_stats(
            portfolio_value,
            stats
        )
        sub_account.leverage = sub_account.gross_leverage

    def _update_portfolio(self):
        """Force a computation of the current portfolio state.
                """
        if not self._dirty_portfolio:
            return

        portfolio = self._portfolio
        pt = self._position_tracker

        portfolio.positions = pt.get_positions()
        position_stats = pt.stats

        portfolio.positions_value = position_value = (
            position_stats.net_value
        )
        portfolio.positions_exposure = position_stats.net_exposure
        self._cash_flow(self._get_payout_total(pt.positions))

        start_value = portfolio.portfolio_value

        # update the new starting value
        portfolio.portfolio_value = end_value = portfolio.cash + position_value

        pnl = end_value - start_value
        if start_value != 0:
            returns = pnl / start_value
        else:
            returns = 0.0

        portfolio.pnl += pnl
        portfolio.returns = (
                (1 + portfolio.returns) *
                (1 + returns) -
                1
        )

        # the portfolio has been fully synced
        self._dirty_portfolio = False

    def _calculate_period_stats(self, portfolio_value, position_stats):
        if portfolio_value == 0:
            gross_leverage = net_leverage = np.inf
        else:
            gross_leverage = position_stats.gross_exposure / portfolio_value
            net_leverage = position_stats.net_exposure / portfolio_value

        return gross_leverage, net_leverage

    def sync_last_sale_prices(self,
                              dt,
                              data_portal,
                              handle_non_market_minutes=False):
        self._position_tracker.sync_last_sale_prices(
            dt,
            data_portal,
            handle_non_market_minutes=handle_non_market_minutes,
        )
        self._dirty_portfolio = True

    @staticmethod
    def _calculate_payout(multiplier, amount, old_price, price):
        return (price - old_price) * multiplier * amount

    def _cash_flow(self, amount):
        self._dirty_portfolio = True
        p = self._portfolio
        p.cash_flow += amount
        p.cash += amount

    def process_transaction(self, transaction):
        """Add a transaction to ledger, updating the current state as needed.

                Parameters
                ----------
                transaction : zp.Transaction
                    The transaction to execute.
                """
        asset = transaction.asset
        if isinstance(asset, Future):
            try:
                old_price = self._payout_last_sale_prices[asset]
            except KeyError:
                self._payout_last_sale_prices[asset] = transaction.price
            else:
                position = self._position_tracker.positions[asset]
                amount = position.amount
                price = transaction.price

                self._cash_flow(
                    self._calculate_payout(
                        asset.price_multiplier,
                        amount,
                        old_price,
                        price,
                    ),
                )

                if amount + transaction.amount == 0:
                    del self._payout_last_sale_prices[asset]
                else:
                    self._payout_last_sale_prices[asset] = price
        else:
            self._cash_flow(-(transaction.price * transaction.amount))

        self._position_tracker.execute_transaction(transaction)

        # we only ever want the dict form from now on
        transaction_dict = transaction.to_dict()
        try:
            self._processed_transactions[transaction.dt].append(
                transaction_dict,
            )
        except KeyError:
            self._processed_transactions[transaction.dt] = [transaction_dict]

    def calculate_period_stats(self):
        position_stats = self._position_tracker.stats
        portfolio_value = self._portfolio.portfolio_value

        return portfolio_value, self._calculate_period_stats(portfolio_value, position_stats)

    def process_splits(self, splits):
        """Processes a list of splits by modifying any positions as needed.

                Parameters
                ----------
                splits: list[(Asset, float)]
                    A list of splits. Each split is a tuple of (asset, ratio).
                """
        leftover_cash = self._position_tracker.handle_splits(splits)
        if leftover_cash > 0:
            self._cash_flow(leftover_cash)

    def process_order(self, order):
        """Keep track of an order that was placed.

                Parameters
                ----------
                order : zp.Order
                    The order to record.
                """
        try:
            dt_orders = self._orders_by_modified[order.dt]
        except KeyError:
            self._orders_by_modified[order.dt] = collections.OrderedDict([
                (order.id, order),
            ])
            self._orders_by_id[order.id] = order
        else:
            self._orders_by_id[order.id] = dt_orders[order.id] = order
            # to preserve the order of the orders by modified date
            dt_orders.move_to_end(order.id, last=True)

        self._orders_by_id.move_to_end(order.id, last=True)

    def process_commission(self, commission):
        """Process the commission.

                Parameters
                ----------
                commission : zp.Event
                    The commission being paid.
                """
        asset = commission['asset']
        cost = commission['cost']

        self._position_tracker.handle_commission(asset, cost)
        self._cash_flow(-cost)

    def close_position(self, asset, dt, data_portal):
        txn = self._position_tracker.maybe_create_close_position_transaction(
            asset,
            dt,
            data_portal,
        )
        if txn is not None:
            self.process_transaction(txn)

    def process_dividends(self, next_session, asset_finder, adjustment_reader):
        """Process dividends for the next session.

        This will earn us any dividends whose ex-date is the next session as
        well as paying out any dividends whose pay-date is the next session
        """
        position_tracker = self._position_tracker

        # Earn dividends whose ex_date is the next trading day. We need to
        # check if we own any of these stocks so we know to pay them out when
        # the pay date comes.
        held_sids = set(position_tracker.positions)
        if held_sids:
            cash_dividends = adjustment_reader.get_dividends_with_ex_date(
                held_sids,
                next_session,
                asset_finder
            )
            stock_dividends = (
                adjustment_reader.get_stock_dividends_with_ex_date(
                    held_sids,
                    next_session,
                    asset_finder
                )
            )

            # Earning a dividend just marks that we need to get paid out on
            # the dividend's pay-date. This does not affect our cash yet.
            position_tracker.earn_dividends(
                cash_dividends,
                stock_dividends,
            )

        # Pay out the dividends whose pay-date is the next session. This does
        # affect out cash.
        self._cash_flow(
            position_tracker.pay_dividends(
                next_session,
            ),
        )

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

        return [
            o.to_dict()
            for o in self._orders_by_modified.get(dt, {}).values()
        ]

    @property
    def positions(self):
        return self._position_tracker.get_position_list()

    def _get_payout_total(self, positions):
        calculate_payout = self._calculate_payout
        payout_last_sale_prices = self._payout_last_sale_prices

        total = 0
        for asset, old_price in payout_last_sale_prices.items():
            position = positions[asset]
            payout_last_sale_prices[asset] = price = position.last_sale_price
            amount = position.amount
            total += calculate_payout(
                asset.price_multiplier,
                amount,
                old_price,
                price,
            )
        return total

    def todays_returns(self):
        return ((self._portfolio.returns + 1) /
                (self._previous_total_returns + 1) -
                1)

    def start_of_session(self, session_label):
        self._previous_total_returns = self._portfolio.returns

        # remove the earliest return each start of session if any
        # in live, every x time, (days between end_dt and start_dt), we pop the left-most element
        # this will never happen in simulation mode, so it is safe for both to share the same
        # implementation.

        self._session_count += 1

    def end_of_bar(self, sessions):
        # add returns each bar
        returns = self.daily_returns_array
        returns.append(self.todays_returns())
        if self._session_count == self._look_back:
            returns.popleft()
            self._session_count = 0
        self._sessions = sessions

    def end_of_session(self, sessions):
        pass

    # called by the algorithm object
    def capital_change(self, change_amount):
        self._update_portfolio()
        portfolio = self._portfolio
        # we update the cash and total value so this is not dirty
        portfolio.portfolio_value += change_amount
        portfolio.cash += change_amount

    def transactions(self, dt=None):
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


    def get_state(self, dt):
        state = acc.LedgerState(
            session_count=self._session_count,
            portfolio=cv.to_proto_portfolio(self._immutable_portfolio),
            account=cv.to_proto_account(self._immutable_account),
            last_checkpoint=cv.to_proto_timestamp(dt),
            orders=[cv.to_proto_order(order) for order in self._orders_by_id.values()],
            first_session=cv.to_proto_timestamp(self._start_dt.to_datetime()),
            daily_returns=[acc.Return(timestamp=ts, value=val) for ts, val in
                           zip(self._sessions, self.daily_returns_array)])
        self._last_checkpoint = dt
        return state.SerializeToString()

    def restore_state(self, state):
        self._restored = True

        l = acc.LedgerState()
        l.ParseFromString(state)
        self._portfolio = cv.to_zp_portfolio(l.portfolio)
        self._account = cv.to_zp_account(l.account)
        self._session_count = l.session_count
        for order in [cv.to_zp_order(order) for order in l.orders]:
            self.process_order(order)
        dra = self.daily_returns_array
        for ret in l.daily_returns:
            dra.append(ret.value)
        self._start_dt = l.first_session
        self._last_checkpoint = l.last_checkpoint
