import pandas as pd

from zipline import protocol

from dev import events

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


class Controller(object):
    def __init__(self, process_manager: pluto.control.controller.process_manager.StrategyManager):
        self._process_manager = process_manager
        self._orders = 0

    def clock_update(self, ts, evt, signals):
        controllable_manager = self._process_manager

        processes = self._process_manager.processes

        broker_states = self._process_manager.broker_states

        # set timestamp ahead of time so that we know which events to wait for...
        controllable_manager.set_global_timestamp(ts)

        # populate broker state to send to controllables
        for cts, c_evt, calendar in signals:
            # set current datetime

            # # skip
            # if calendar_name not in self._base_calendars:
            #     continue

            calendar_name = calendar.name

            controllable_manager.set_calendar_timestamp(calendar_name, cts)

            # bar events
            if c_evt == BAR:
                # fetch executed transactions, commissions etc.

                transactions, closed_orders = controllable_manager.get_transactions(
                    calendar_name, 'minute')

                # update states

                for transaction in transactions:
                    order = transaction['order']

                    bs = broker_states[order.session_id]

                    controllable_manager.process_transaction(
                        calendar_name,
                        transaction['transaction'])

                    if transaction['commission'] > 0:
                        controllable_manager.process_commission({
                            'asset': order.asset,
                            'order': order.to_dict(),
                            'cost': transaction['commission']
                        })

                    bs.transactions.append(
                        broker_pb2.TransactionCommission(
                            transaction=conversions.to_proto_transaction(transaction['transaction'].to_dict()),
                            commission=transaction['commission']
                        ))

                controllable_manager.prune_orders(
                    calendar_name,
                    closed_orders)

            elif c_evt == TRADE_END:
                # update for all data frequencies...
                for df in controllable_manager.get_frequencies(calendar_name):
                    transactions, closed_orders = controllable_manager.get_transactions(
                        calendar_name, df)

                    # update states

                    controllable_manager.prune_orders(
                        calendar_name,
                        closed_orders)

                    for transaction in transactions:
                        order = transaction['order']

                        bs = broker_states[order.session_id]

                        controllable_manager.process_transaction(
                            calendar_name,
                            transaction['transaction'])

                        if transaction['commission'] > 0:
                            controllable_manager.process_commission({
                                'asset': order.asset,
                                'order': order.to_dict(),
                                'cost': transaction['commission']
                            })

                        bs.transactions.append(
                            broker_pb2.TransactionCommission(
                                transaction=conversions.to_proto_transaction(transaction['transaction'].to_dict()),
                                commission=transaction['commission']
                            ))

                        # for commission in order_transactions.commissions:
                        #     controllable_manager.process_commission(commission)
                        #     bs.commissions.append(conversions.to_proto_commission(commission))
                    data_portal = controllable_manager.get_data_portal(calendar_name)

                    controllable_manager.sync_last_sale_prices(
                        cts, data_portal, df
                    )

            elif c_evt == SESSION_START:
                data_portal = controllable_manager.get_data_portal(calendar_name)

                controllable_manager.process_dividends(
                    cts,
                    data_portal.adjustment_reader)

                positions = set(controllable_manager.get_positions(calendar_name))
                open_orders = controllable_manager.get_open_orders(calendar_name)

                controllable_manager.handle_market_open(ts, calendar_name)

                assets_we_care_about = positions | open_orders.keys()

                if assets_we_care_about:
                    splits = data_portal.get_splits(
                        assets_we_care_about,
                        cts)

                    if splits:
                        controllable_manager.process_splits(
                            calendar_name,
                            splits)

            elif c_evt == SESSION_END:
                # open_orders = controllable_manager.get_open_orders(calendar_name)

                for df in controllable_manager.get_frequencies(calendar_name):
                    controllable_manager.cleanup_expired_assets(
                        calendar_name, cts, df)

        # fixme: a process might crash, and we would be waiting for too long...
        # update all processes
        for process in processes:
            process.account_update(broker_states[process.process_id])

        # advance all processes
        for process in processes:
            process.clock_update(ts, evt)

        # for process in processes:
        #     process.clock_update(ts, evt)

        # blocks until all processes have placed orders

        # if evt == BAR or evt == TRADE_END:
        # if running > 0:
        # print("Waiting for orders", ts)
        new_orders = controllable_manager.get_orders()

        # todo: put some additional logic for leverage control, portfolio
        #  optimization here...
        controllable_manager.place_orders(new_orders)
        # clear all variables
        controllable_manager.clear()

        # todo: process the orders.
        # self._process_orders(new_orders)

        # problem: when should we execute the new orders? (the controllables
        # will place an empty order (or a pass) to indicate that they are not making
        # and order.

        # once all the processes have made an order, we can process the orders.
        # this must be done asap! so we do not wait for the next loop!

    def watch(self, handler):
        pass

