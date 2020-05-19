from collections import defaultdict

from zipline.finance.blotter import blotter

from pluto.finance import order as odr
from pluto.coms.utils import conversions

from protos import broker_pb2


class LiveBlotter(blotter.Blotter):
    def __init__(self, session_id, broker, cancel_policy=None):
        '''

        Parameters
        ----------
        broker: pluto.broker.broker_stub.BrokerStub
        cancel_policy
        '''
        super(LiveBlotter, self).__init__(cancel_policy)

        self._broker = broker
        self._orders = {}
        self._new_orders = []
        self._open_orders = defaultdict(list)

        self._transactions = []
        self._closed_orders = []
        self._commissions = []

        self._orders_placed = False
        self._max_shares = int(1e+11)

        self._metadata = (('session_id', session_id),)

    @property
    def open_orders(self):
        return self._open_orders

    @property
    def new_orders(self):
        # return a tuple so that values can't be changed
        # resets the new orders
        if not self._orders_placed:
            # send orders buffered orders to the broker once before resetting
            def generator(orders):
                for order in orders:
                    yield conversions.to_proto_order(order.to_dict())

            # send orders to broker before resetting
            orders = self._orders
            for order in self._broker.PlaceOrders(
                    generator(self._new_orders),
                    self._metadata):
                orders[order.id] = order
                if order.open:
                    self._open_orders[order.asset].append(order)
            self._orders_placed = True
        return tuple(self._new_orders)

    @new_orders.setter
    def new_orders(self, value):
        self._new_orders = value

    def process_splits(self, splits):
        # handled by the broker
        pass

    def get_transactions(self, bar_data):
        return self._transactions, self._commissions, self._closed_orders

    @property
    def orders(self):
        return self._orders

    def order(self, asset, amount, style, order_id=None):
        max_shares = self._max_shares
        if amount == 0:
            # Don't bother placing orders for 0 shares.
            return None

        elif amount > max_shares:
            # Arbitrary limit of 100 billion (US) shares will never be
            # exceeded except by a buggy algorithm.
            raise OverflowError(
                "Can't order more than {}d shares".format(
                    max_shares))

        is_buy = (amount > 0)
        self._new_orders.append(
            odr.Order(
                dt=self.current_dt,
                asset=asset,
                amount=amount,
                stop=style.get_stop_price(is_buy),
            limit=style.get_limit_price(is_buy)
            )
        )
        # mark so that the new orders can be sent to the broker
        self._orders_placed = False

    def hold(self, order_id, reason=''):
        pass

    def reject(self, order_id, reason=''):
        pass

    def cancel(self, order_id, relay_status=True):
        try:
            # attempt to cancel new orders that have not been placed yet
            self._new_orders.remove(order_id)
        except ValueError:
            # send cancel request to broker
            self._broker.CancelOrder(
                broker_pb2.CancelRequest(order_id=order_id),
                self._metadata)

    def cancel_all_orders_for_asset(self, asset, warn=False, relay_status=True):
        self._broker.CancelAllOrdersForAsset(
            conversions.to_proto_asset(asset),
            self._metadata
        )

    def execute_cancel_policy(self, event):
        self._broker.ExecuteCancelPolicy(
            broker_pb2.CancelEvent(event_type=event),
            self._metadata)

    def update(self, broker_data):
        # called each minute
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

    def prune_orders(self, closed_orders):
        for order in closed_orders:
            asset = order.asset
            asset_orders = self._open_orders[asset]
            try:
                asset_orders.remove(order)
            except ValueError:
                pass

            try:
                self._orders.pop(order.id)
            except KeyError:
                pass

        # now clear out the assets from our open_orders dict that have
        # zero open orders
        for asset in list(self.open_orders.keys()):
            if len(self.open_orders[asset]) == 0:
                del self.open_orders[asset]

        self._transactions.clear()
        self._commissions.clear()
        self._closed_orders.clear()
