from collections import OrderedDict
from functools import partial
from math import isnan

from zipline.finance import transaction
from zipline.finance import position as pos

from zipline.finance._finance_ext import (
    PositionStats,
    calculate_position_tracker_stats,
    update_position_last_sale_prices,
)


class PositionTracker(object):
    """The current state of the positions held.

    Parameters
    ----------
    data_frequency : {'daily', 'minute'}
        The data frequency of the simulation.
    """
    def __init__(self, positions):
        self.positions = OrderedDict()

        self._unpaid_dividends = {}
        self._unpaid_stock_dividends = {}
        self._positions_store = positions

        # cache the stats until something alters our positions
        self._dirty_stats = True
        self._stats = PositionStats.new()

    def update_position(self,
                        asset,
                        amount=None,
                        last_sale_price=None,
                        last_sale_date=None,
                        cost_basis=None):
        self._dirty_stats = True

        if asset not in self.positions:
            position = pos.Position(asset)
            self.positions[asset] = position
        else:
            position = self.positions[asset]

        if amount is not None:
            position.amount = amount
        if last_sale_price is not None:
            position.last_sale_price = last_sale_price
        if last_sale_date is not None:
            position.last_sale_date = last_sale_date
        if cost_basis is not None:
            position.cost_basis = cost_basis

    def execute_transaction(self, txn):
        self._dirty_stats = True

        asset = txn.asset

        if asset not in self.positions:
            position = pos.Position(asset)
            self.positions[asset] = position
        else:
            position = self.positions[asset]

        position.update(txn)

        if position.amount == 0:
            del self.positions[asset]

            try:
                # if this position exists in our user-facing dictionary,
                # remove it as well.
                del self._positions_store[asset]
            except KeyError:
                pass

    def handle_commission(self, asset, cost):
        # Adjust the cost basis of the stock if we own it
        if asset in self.positions:
            self._dirty_stats = True
            self.positions[asset].adjust_commission_cost_basis(asset, cost)

    def handle_splits(self, splits):
        """Processes a list of splits by modifying any positions as needed.

        Parameters
        ----------
        splits: list
            A list of splits.  Each split is a tuple of (asset, ratio).

        Returns
        -------
        int: The leftover cash from fractional shares after modifying each
            position.
        """
        total_leftover_cash = 0

        for asset, ratio in splits:
            if asset in self.positions:
                self._dirty_stats = True

                # Make the position object handle the split. It returns the
                # leftover cash from a fractional share, if there is any.
                position = self.positions[asset]
                leftover_cash = position.handle_split(asset, ratio)
                total_leftover_cash += leftover_cash

        return total_leftover_cash

    def earn_dividends(self, cash_dividends, stock_dividends):
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
            self._dirty_stats = True  # only mark dirty if we pay a dividend

            # Store the earned dividends so that they can be paid on the
            # dividends' pay_dates.
            div_owed = self.positions[cash_dividend.asset].earn_dividend(
                cash_dividend,
            )
            try:
                self._unpaid_dividends[cash_dividend.pay_date].append(div_owed)
            except KeyError:
                self._unpaid_dividends[cash_dividend.pay_date] = [div_owed]

        for stock_dividend in stock_dividends:
            self._dirty_stats = True  # only mark dirty if we pay a dividend

            div_owed = self.positions[
                stock_dividend.asset
            ].earn_stock_dividend(stock_dividend)
            try:
                self._unpaid_stock_dividends[stock_dividend.pay_date].append(
                    div_owed,
                )
            except KeyError:
                self._unpaid_stock_dividends[stock_dividend.pay_date] = [
                    div_owed,
                ]

    def pay_dividends(self, next_trading_day):
        """
        Returns a cash payment based on the dividends that should be paid out
        according to the accumulated bookkeeping of earned, unpaid, and stock
        dividends.
        """
        net_cash_payment = 0.0

        try:
            payments = self._unpaid_dividends[next_trading_day]
            # Mark these dividends as paid by dropping them from our unpaid
            del self._unpaid_dividends[next_trading_day]
        except KeyError:
            payments = []

        # representing the fact that we're required to reimburse the owner of
        # the stock for any dividends paid while borrowing.
        for payment in payments:
            net_cash_payment += payment['amount']

        # Add stock for any stock dividends paid.  Again, the values here may
        # be negative in the case of short positions.
        try:
            stock_payments = self._unpaid_stock_dividends[next_trading_day]
        except KeyError:
            stock_payments = []

        for stock_payment in stock_payments:
            payment_asset = stock_payment['payment_asset']
            share_count = stock_payment['share_count']
            # note we create a Position for stock dividend if we don't
            # already own the asset
            if payment_asset in self.positions:
                position = self.positions[payment_asset]
            else:
                position = self.positions[payment_asset] = pos.Position(
                    payment_asset,
                )

            position.amount += share_count

        return net_cash_payment

    def maybe_create_close_position_transaction(
            self,
            asset,
            dt,
            data_portal,
            data_frequency):

        if not self.positions.get(asset):
            return None

        position = self.positions.get(asset)

        # amount = position.amount
        # price = data_portal.get_spot_value(
        #     asset, 'price', dt, data_frequency)
        #
        # # Get the last traded price if price is no longer available
        # if isnan(price):
        # price = position.last_sale_price

        return transaction.Transaction(
            asset=asset,
            amount=-position.amount,
            dt=dt,
            price=position.last_sale_price,
            order_id=None,
        )

    def get_positions(self):
        positions = self._positions_store

        for asset, pos in self.positions.items():
            # Adds the new position if we didn't have one before, or overwrite
            # one we have currently
            positions[asset] = pos.protocol_position

        return positions

    def get_position_list(self):
        return [
            pos.to_dict()
            for asset, pos in self.positions.items()
            if pos.amount != 0
        ]

    def sync_last_sale_prices(self,
                              dt,
                              data_portal,
                              data_frequency,
                              handle_non_market_minutes=False):
        self._dirty_stats = True

        if handle_non_market_minutes:
            previous_minute = data_portal.trading_calendar.previous_minute(dt)
            get_price = partial(
                data_portal.get_adjusted_value,
                field='price',
                dt=previous_minute,
                perspective_dt=dt,
                data_frequency=data_frequency,
            )

        else:
            get_price = partial(
                data_portal.get_scalar_asset_spot_value,
                field='price',
                dt=dt,
                data_frequency=data_frequency,
            )

        update_position_last_sale_prices(self.positions, get_price, dt)

    @property
    def stats(self):
        """The current status of the positions.

        Returns
        -------
        stats : PositionStats
            The current stats position stats.

        Notes
        -----
        This is cached, repeated access will not recompute the stats until
        the stats may have changed.
        """
        if self._dirty_stats:
            calculate_position_tracker_stats(self.positions, self._stats)
            self._dirty_stats = False

        return self._stats