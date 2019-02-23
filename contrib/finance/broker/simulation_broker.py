from zipline.gens.brokers.broker import Broker

from zipline.finance import metrics
from zipline.finance.blotter import simulation_blotter

class DummyBroker(Broker):
    def transactions(self):
        pass

    def account(self):
        pass

    def order(self, asset, amount, style):
        pass

    def time_skew(self):
        pass

    def portfolio(self):
        pass

    def subscribe_to_market_data(self, asset):
        pass

    def get_realtime_bars(self, assets, frequency):
        pass

    def subscribed_assets(self):
        pass

    def get_last_traded_dt(self, asset):
        pass

    def cancel_order(self, order_param):
        pass

    def get_spot_value(self, assets, field, dt, data_frequency):
        pass

    def update(self, dt):
        blotter = self._blotter

        new_transactions, new_commissions, closed_orders = blotter.get_transactions()
        #todo: update metrics tracker using the blotter and metrics tracker
        #this function gets called by an external clock...
        orders = self._blotter.orders[transaction.order_id]


