from functools import partial

from zipline.protocol import BarData

from pluto.finance.markets import core


class MarketSimulation(core.AbstractMarket):
    def __init__(self, engine_setup, data_frequency):
        self.engine_setup = engine_setup
        self._data_frequency = data_frequency

        self._bar_data_dict = {}
        self._per_calendar_dt = {}

    def set_calendar_timestamp(self, calendar_name, ts):
        self._per_calendar_dt[calendar_name] = ts

    def add_bar_data(self, data_portal, asset_restrictions):
        data_frequency = self._data_frequency

        trading_calendar = data_portal.trading_calendar
        calendar_name = trading_calendar.name
        bar_data_dict = self._bar_data_dict

        if calendar_name not in bar_data_dict:
            # calendar_frequencies = self._calendar_frequencies[calendar_name]
            #
            # calendar_frequencies.append(data_frequency)

            # todo: refactor bar data class such that we only have one bar data object
            #  per calendar.

            self._bar_data_dict[calendar_name] = self._create_bar_data(
                calendar_name,
                data_frequency,
                data_portal,
                asset_restrictions)

    def place_orders(self, orders, strategy_manager):
        strategy_manager.place_orders(orders)

    def get_transactions(self, calendar_name, blotter):
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Returns
        -------
        transactions_list: List
            transactions_list: list of transactions resulting from the current
            open orders.  If there were no open orders, an empty list is
            returned.

        closed_orders: List
            closed_orders: list of all the orders that have filled.
        """

        bar_data = self._bar_data_dict[calendar_name]
        transactions = []
        closed_orders = []

        engine_setup = self.engine_setup

        if blotter.open_orders:
            for asset, asset_orders in blotter.open_orders.items():

                slippage_model = engine_setup.get_slippage_model(asset)

                for order, txn in slippage_model.simulate(
                        bar_data,
                        asset,
                        asset_orders):

                    # order_transactions = OrderTransactions(order)

                    commission_model = engine_setup.get_commission_model(asset)

                    additional_commission = commission_model.calculate(order, txn)

                    # if additional_commission > 0:
                    #     order_transactions.commissions.append(
                    #         {
                    #             "asset": order.asset,
                    #             "order": order.to_dict(),
                    #             "cost": additional_commission
                    #         }
                    #     )

                    order.filled += txn.amount
                    order.commission += additional_commission

                    order.dt = txn.dt

                    # order_transactions.transactions.append(
                    #     {'order': order, 'transaction': txn, 'commission': additional_commission}
                    # )

                    if not order.open:
                        closed_orders.append(order)

                    transactions.append({
                        'order': order,
                        'transaction': txn,
                        'commission': additional_commission})

        return transactions, closed_orders

    def _create_bar_data(
            self,
            calendar_name,
            data_frequency,
            data_portal,
            asset_restrictions):

        return BarData(
            data_portal=data_portal,
            simulation_dt_func=partial(self._get_current_dt, calendar_name=calendar_name),
            data_frequency=data_frequency,
            trading_calendar=data_portal.trading_calendar,
            restrictions=asset_restrictions)

    def _get_current_dt(self, calendar_name):
        return self._per_calendar_dt[calendar_name]

