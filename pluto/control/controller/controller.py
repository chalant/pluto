import typing

import logbook
import pandas

import pluto.control.controller.process_manager
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


class NullDataIngester(object):
    def ingest_bar_data(self, arr: typing.List[typing.Tuple[pandas.Timestamp, pandas.Timestamp, str]]):
        pass

    def update_asset_database(self, arr: typing.List[typing.Tuple[pandas.Timestamp, pandas.Timestamp, str]]):
        pass


class Controller(object):
    def __init__(
            self,
            strategy_manager: pluto.control.controller.process_manager.StrategyManager,
            market,
            metrics_tracker,
            data_ingester=None):

        self._metrics_tracker = metrics_tracker
        self._strategy_manager = strategy_manager
        self._orders = 0

        self._update_flag = True
        self._data_ingester = data_ingester if data_ingester else NullDataIngester()
        self._market = market

    def clock_update(self, ts, evt, signals):
        strategy_manager = self._strategy_manager
        market = self._market

        strategies = strategy_manager.strategies
        broker_states = strategy_manager.broker_states
        # set timestamp ahead of time so that we know which events to wait for...
        strategy_manager.set_global_timestamp(ts)

        metrics_tracker = self._metrics_tracker

        to_update = [
            (ts, cts, calendar.name)
            for cts, c_evt, calendar in signals
            if c_evt == BAR or c_evt == TRADE_END]

        # ingest data at each start of bar event
        # ingestion writes streamed data in the database in a format that
        # can be red by the readers.
        # we have a separate process that stores streamed data.
        # we then call ingest to create bar data of the most recent stored data.

        data_ingester = self._data_ingester

        if to_update:
            data_ingester.ingest_bar_data(to_update)

        # populate broker state to send to controllable
        for cts, c_evt, calendar in signals:
            calendar_name = calendar.name
            market.set_calendar_timestamp(calendar_name, cts)

            # bar events
            if c_evt == BAR or c_evt == TRADE_END:
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
                strategy_manager.sync_last_sale_prices(calendar_name, cts)

            elif c_evt == MINUTE_END:
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
                # open_orders = controllable_manager.get_open_orders(calendar_name)
                strategy_manager.cleanup_expired_assets(calendar_name, cts)

                metrics_tracker.end_of_bar(
                    cts,
                    calendar_name,
                    strategy_manager.get_ledger(calendar_name))

        # fixme: a process might crash, and we would be waiting for too long...
        # update all processes
        for strategy in strategies:
            strategy.account_update(broker_states[strategy.process_id])

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

        to_update = [
            (ts, cts, calendar.name)
            for cts, c_evt, calendar in signals
            if c_evt == SESSION_END]

        # update assets database at the end of session.
        if to_update:
            data_ingester.update_assets_database(to_update)

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
