from collections import defaultdict
from pluto.coms.utils import conversions


class Blotter(object):
    def __init__(self, broker, session_id):
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

    def update_blotter(self, broker_data):
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
