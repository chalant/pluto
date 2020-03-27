from zipline.finance.blotter import blotter

class LiveBlotter(blotter.Blotter):
    def __init__(self, broker, cancel_policy=None):
        super(LiveBlotter, self).__init__(cancel_policy)

        self._broker = broker
        self._orders = {}

        self._transactions = []
        self._closed_orders = []
        self._commissions = []

    @property
    def new_orders(self):
        return self._broker.new_orders

    @new_orders.setter
    def new_orders(self, value):
        self._broker.new_orders = value

    def get_transactions(self, bar_data):
        return self._transactions, self._commissions, self._closed_orders

    def orders(self):
        return self._orders

    def order(self, asset, amount, style, order_id=None):
        order = self._broker.order(asset, amount, style, order_id)
        self._orders[order.id] = order
        return order.id

    def hold(self, order_id, reason=''):
        pass

    def reject(self, order_id, reason=''):
        pass

    def cancel(self, order_id, relay_status=True):
        self._broker.cancel(order_id, relay_status)

    def cancel_all_orders_for_asset(self, asset, warn=False, relay_status=True):
        self._broker.cancel_all_orders_for_asset(asset, warn, relay_status)

    def execute_cancel_policy(self, event):
        self._broker.execute_cancel_policy(event)

    def process_splits(self, splits):
        pass

    def update(self, dt, broker_data):
        #called each minute
        commissions = self._commissions
        closed_orders = self._closed_orders
        transactions = self._transactions

        orders = self._orders
        for transaction in broker_data.transactions:
            order_id = transaction.order_id
            o = orders.get(order_id, None)
            if o:
                transactions.append(transaction)

        for commission in broker_data.commissions:
            order = commission.order
            order_id = order.order_id
            o = orders.get(order_id, None)
            if o:
                commissions.append({
                    'asset': commission.asset,
                    'cost': commission.cost,
                    'order': order
                })

            orders[order_id] = order  # update the order state
            if not order.open:
                closed_orders.append(order)


    def prune_orders(self, closed_orders):
        orders = self._orders
        for order in closed_orders:
            try:
                del orders[order.order_id]
            except KeyError:
                pass

        self._transactions.clear()
        self._commissions.clear()
        self._closed_orders.clear()