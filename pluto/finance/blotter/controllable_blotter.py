from collections import defaultdict

from zipline.assets import Asset
from zipline.finance.cancel_policy import NeverCancel
from zipline.finance.blotter import blotter
from zipline.finance.order import Order
from zipline.utils.input_validation import expect_types

from pluto.coms.utils import conversions


class ControllableBlotter(blotter.Blotter):
    def __init__(self, cancel_policy=None):

        self._orders = {}
        self._new_orders = []
        self._open_orders = defaultdict(list)

        self._transactions = []
        self._closed_orders = []
        self._commissions = []

        self._orders_placed = False
        self._max_shares = int(1e+11)

        # self._metadata = (('session_id', session_id),)

        self.current_dt = None
        self.cancel_policy = cancel_policy if cancel_policy else NeverCancel()

    def set_date(self, dt):
        self.current_dt = dt

    @property
    def open_orders(self):
        return self._open_orders

    @expect_types(asset=Asset)
    def order(self, asset, amount, style, session_id, order_id=None):
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
        session_id : str
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
            session_id=session_id,
            dt=self.current_dt,
            asset=asset,
            amount=amount,
            stop=style.get_stop_price(is_buy),
            limit=style.get_limit_price(is_buy),
            id=order_id
        )

        self._open_orders[order.asset].append(order)
        self._orders[order.id] = order
        self._new_orders.append(order)

        return order.id

    def cancel(self, order_id, relay_status=True):
        if order_id not in self._orders:
            return

        cur_order = self._orders[order_id]

        if cur_order.open:
            order_list = self._open_orders[cur_order.asset]
            if cur_order in order_list:
                order_list.remove(cur_order)

            if cur_order in self._new_orders:
                self._new_orders.remove(cur_order)
            cur_order.cancel()
            cur_order.dt = self.current_dt

            if relay_status:
                # we want this order's new status to be relayed out
                # along with newly placed orders.
                self._new_orders.append(cur_order)

    def cancel_all_orders_for_asset(self, asset, warn=False, relay_status=True):
        """
        Cancel all open orders for a given asset.
        """
        # (sadly) open_orders is a defaultdict, so this will always succeed.
        orders = self._open_orders[asset]

        # We're making a copy here because `cancel` mutates the list of open
        # orders in place.  The right thing to do here would be to make
        # self.open_orders no longer a defaultdict.  If we do that, then we
        # should just remove the orders once here and be done with the matter.
        for order in orders[:]:
            self.cancel(order.id, relay_status)

        assert not orders
        del self._open_orders[asset]

    def prune_orders(self, closed_orders):
        """
        Removes all given orders from the blotter's open_orders list.

        Parameters
        ----------
        closed_orders: iterable of orders that are closed.

        Returns
        -------
        None
        """
        # remove all closed orders from our open_orders dict
        for order in closed_orders:
            asset = order.asset
            asset_orders = self._open_orders[asset]
            try:
                asset_orders.remove(order)
            except ValueError:
                continue

        # now clear out the assets from our open_orders dict that have
        # zero open orders
        for asset in list(self._open_orders.keys()):
            if len(self._open_orders[asset]) == 0:
                del self._open_orders[asset]

    def update_blotter(self, broker_data):
        # filters orders

        commissions = self._commissions
        closed_orders = self._closed_orders
        transactions = self._transactions

        orders = self._orders

        for transaction in broker_data.transactions:
            order_id = transaction.order_id
            o = orders.get(order_id, None)

            if o:
                transactions.append(
                    conversions.to_zp_transaction(transaction))

        for commission in broker_data.commissions:
            order = conversions.to_zp_order(commission.order)
            order_id = order.id
            o = orders.get(order_id, None)

            if o:
                commissions.append({
                    'asset': conversions.to_zp_asset(commission.asset),
                    'cost': commission.cost,
                    'order': order
                })

            orders[order_id] = order  # update the order state
            if not order.open:
                closed_orders.append(order)

    def get_transactions(self):
        return (self._transactions,
                self._commissions,
                self._closed_orders)

    def clear(self):
        self._transactions.clear()
        self._commissions.clear()
        self._closed_orders.clear()
