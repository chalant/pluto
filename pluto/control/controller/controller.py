import collections

import logbook

from pluto.finance.markets import core
import pluto.control.controller.process_manager
from pluto.data import data_source as ds
from pluto.coms.utils import conversions

from protos.clock_pb2 import (
    BAR,
    TRADE_END,
    SESSION_END,
    SESSION_START,
    MINUTE_END,
    BEFORE_TRADING_START)

from protos import broker_pb2

log = logbook.Logger("Controller")


class Controller(object):
    def __init__(
            self,
            strategy_manager: pluto.control.controller.process_manager.StrategyManager,
            market: core.AbstractMarket,
            metrics_tracker,
            data_source: ds.AbstractDataSource):

        self._metrics_tracker = metrics_tracker
        self._strategy_manager = strategy_manager
        self._orders = 0

        self._update_flag = True
        self._data_source = data_source
        self._market = market

    def clock_update(self, ts, evt, signals):
        strategy_manager = self._strategy_manager
        market = self._market

        strategies = strategy_manager.strategies
        broker_states = strategy_manager.broker_states
        # set timestamp ahead of time so that we know which events to wait for...
        strategy_manager.set_global_timestamp(ts)

        metrics_tracker = self._metrics_tracker

        # to_update = [
        #     (ts, cts, calendar.name)
        #     for cts, c_evt, calendar in signals
        #     if c_evt == BAR]

        # ingest data at each start of bar event
        # ingestion writes streamed data in the database in a format that
        # can be red by the readers.
        # we have a separate process that stores streamed data.
        # we then call ingest to create bar data of the most recent stored data.

        data_source = self._data_source

        # if to_update:
        #     data_ingester.ingest_bar_data(to_update)

        # todo: if the event is bar, download all quotes data for all our
        #  current positions and open orders...
        #  if event is session_end, download historical data for all assets.

        last_sale_prices = data_source.get_last_sale_prices([
            (ts, cts, calendar.name, strategy_manager.get_positions(calendar.name))
            for cts, c_evt, calendar in signals
            if c_evt == BAR or c_evt == MINUTE_END or c_evt == SESSION_END or c_evt == BEFORE_TRADING_START]
        )

        # populate broker state to send to controllable
        for cts, c_evt, calendar in signals:
            calendar_name = calendar.name
            market.set_calendar_timestamp(calendar_name, cts)

            # bar events
            if c_evt == BAR or c_evt == TRADE_END:
                strategy_manager.sync_last_sale_prices(calendar_name, last_sale_prices[calendar_name], cts)
                # fetch executed transactions, commissions etc.
                # todo: need to apply tax models...
                blotter = strategy_manager.get_blotter(calendar_name)

                transactions, closed_orders = market.get_transactions(
                    calendar_name, blotter)

                # transactions, closed_orders = strategy_manager.get_transactions(calendar_name)

                # update states
                for transaction in transactions:
                    order = transaction['order']

                    strategy_manager.process_transaction(
                        calendar_name,
                        transaction['transaction'])

                    if transaction['commission'] > 0:
                        strategy_manager.process_commission(
                            calendar_name,
                            {
                                'asset': order.asset,
                                'order': order.to_dict(),
                                'cost': transaction['commission']
                            })

                    broker_states[order.session_id].transactions.append(
                        broker_pb2.TransactionCommission(
                            transaction=conversions.to_proto_transaction(transaction['transaction'].to_dict()),
                            commission=transaction['commission']
                        ))

                blotter.prune_orders(closed_orders)
                # note: here we force synchronization of sale prices
                # strategy_manager.sync_last_sale_prices(calendar_name, last_sale_prices[calendar_name], cts)

            elif c_evt == MINUTE_END:
                strategy_manager.sync_last_sale_prices(
                    calendar_name,
                    last_sale_prices[calendar_name],
                    cts)

                metrics_tracker.end_of_bar(
                    cts,
                    calendar_name,
                    strategy_manager.get_ledger(calendar_name))

            elif c_evt == SESSION_START:
                # todo: each session start check if leverage have changed
                ledger = strategy_manager.get_ledger(calendar_name)

                data_portal = strategy_manager.get_data_portal(calendar_name)

                strategy_manager.process_dividends(
                    cts,
                    calendar_name,
                    data_portal.adjustment_reader)

                positions = set(strategy_manager.get_positions(calendar_name))
                open_orders = strategy_manager.get_open_orders(calendar_name)

                strategy_manager.handle_market_open(ts, calendar_name)

                assets_we_care_about = positions | open_orders.keys()

                if assets_we_care_about:
                    splits = data_portal.get_splits(assets_we_care_about, cts)

                    if splits:
                        strategy_manager.process_splits(
                            calendar_name,
                            splits)

                # ledger.start_of_session(0)

                metrics_tracker.start_of_session(
                    cts,
                    calendar_name,
                    ledger)

            elif c_evt == SESSION_END:
                strategy_manager.sync_last_sale_prices(
                    calendar_name,
                    last_sale_prices[calendar_name],
                    cts)
                strategy_manager.cleanup_expired_assets(calendar_name, cts)

                metrics_tracker.end_of_session(
                    cts,
                    calendar_name,
                    strategy_manager.get_ledger(calendar_name))

            elif c_evt == BEFORE_TRADING_START:
                strategy_manager.sync_last_sale_prices(
                    calendar_name,
                    last_sale_prices[calendar_name],
                    cts)

        # flatten last sale prices to send to
        values = []

        for pairs in last_sale_prices.values():
            values.extend([
                broker_pb2.AssetPrice(sid=sid, price=price)
                for sid, price in pairs])

        # todo: send last sale prices here (in the broker state)
        # fixme: a process might crash, and we would be waiting for too long...
        # update all processes
        for strategy in strategies:
            brs = broker_states[strategy.process_id]
            brs.last_sale_prices.extend(values)
            strategy.account_update(brs)

        # advance all processes
        for strategy in strategies:
            strategy.clock_update(ts, evt)

        # print("Waiting for orders", ts)
        new_orders = strategy_manager.get_orders()

        # todo: catch any account control errors here...
        #  given the new orders and the current portfolio and
        #  the current open orders and the last sale price, determine
        #  if we go above the maximum leverage

        # check if strategies don't go above allocated capital and leverage

        # new_orders_batches = strategy_manager.get_orders_batches()
        #
        # for ts, cts, calendar in signals:
        #     for batch in new_orders_batches:
        #         portfolio = batch.portfolio
        #         positions = batch.positions
        #         open_orders = batch.open_orders

        # todo: call the controller script
        # self._controller_script.update()

        # todo: put some additional logic for leverage control, portfolio
        #  optimization here...
        market.place_orders(new_orders, strategy_manager)
        # strategy_manager.place_orders(new_orders)
        # clear all variables
        strategy_manager.clear()

        # to_update = [
        #     (ts, cts, calendar.name)
        #     for cts, c_evt, calendar in signals
        #     if c_evt == SESSION_END]

        # update assets database at the end of session.
        # if to_update:
        #     data_source.update_assets_database(to_update)

        # reset update flag
        self._update_flag = True

        return 1

        # todo: process the orders.
        # self._process_orders(new_orders)

        # problem: when should we execute the new orders? (the controllables
        # will place an empty order (or a pass) to indicate that they are not making
        # and order.

        # once all the processes have made an order, we can process the orders.
        # this must be done asap! so we do not wait for the next loop!
