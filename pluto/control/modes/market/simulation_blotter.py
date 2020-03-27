from zipline.finance.blotter import simulation_blotter

from pluto.coms.utils import conversions

class SimulationBlotter(simulation_blotter.SimulationBlotter):
    def get_transactions(self, bar_data):
        """
                Creates a list of transactions based on the current open orders,
                slippage model, and commission model.

                Parameters
                ----------
                bar_data: zipline._protocol.BarData

                Notes
                -----
                This method book-keeps the blotter's open_orders dictionary, so that
                 it is accurate by the time we're done processing open orders.

                Returns
                -------
                transactions_list: List
                    transactions_list: list of transactions resulting from the current
                    open orders.  If there were no open orders, an empty list is
                    returned.

                commissions_list: List
                    commissions_list: list of commissions resulting from filling the
                    open orders.  A commission is an object with "asset" and "cost"
                    parameters.

                closed_orders: List
                    closed_orders: list of all the orders that have filled.
                """

        closed_orders = []
        transactions = []
        commissions = []

        if self.open_orders:
            for asset, asset_orders in self.open_orders.items():
                slippage = self.slippage_models[type(asset)]

                for order, txn in \
                        slippage.simulate(bar_data, asset, asset_orders):
                    commission = self.commission_models[type(asset)]
                    additional_commission = commission.calculate(order, txn)

                    if additional_commission > 0:
                        commissions.append(
                            conversions.to_proto_commission(
                                {"asset": order.asset,
                                 "order": order,
                                 "cost": additional_commission
                                 }))

                    order.filled += txn.amount
                    order.commission += additional_commission

                    order.dt = txn.dt

                    transactions.append(
                        conversions.to_proto_transaction(txn))

                    if not order.open:
                        closed_orders.append(order)

        return transactions, commissions, closed_orders