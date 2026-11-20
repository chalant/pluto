import threading
import typing

from collections import defaultdict

import numpy as np
from trading_calendars import calendar_utils

import zipline
from zipline import protocol
from zipline.finance.order import ORDER_STATUS

import pluto.assets.assets
from pluto.finance import ledger, position_tracker
from pluto.setup import run_setup

from pluto.coms.utils import conversions
from pluto.interface.utils import service_access

from protos import broker_pb2_grpc as brk_rpc
from protos import broker_pb2


class StrategyManager(brk_rpc.BrokerServicer):
    def __init__(self, asset_finder: pluto.assets.assets.AssetFinder, stp: run_setup.RunSetup):

        self._processes = {}

        self._last_sync_dt = None

        self._setup = stp

        self._start_dt = stp.start

        self._portfolio = protocol.Portfolio(stp.start, stp.capital)
        self._positions = positions = protocol.Positions()
        self._account = protocol.Account()
        self._asset_finder = asset_finder

        # todo: we need to change the sync last sale prices function
        #  the price should be injected externally by the controller.
        #  the controller sends the sale prices to the controllables.
        self._position_tracker = position_tracker.PositionTracker(positions)

        self._data_frequency = stp.data_frequency

        # self._ledger = ledger.Ledger(
        #     start_dt,
        #     portfolio,
        #     emission_rate)

        self._ledgers = {}
        self._position_trackers = {}
        self._bar_data_dict = {}
        self._blotters = {}
        self._data_portals = {}

        self._global_ts = None

        self._sync_times = {}

        self._broker_states = {}

        self._new_orders = []
        self._orders_batches = []

        self._watchlist_counter = 0
        self._watchlist = set()

        self._watchlist_complete = threading.Event()

        self._aliases = calendar_utils._default_calendar_aliases

        self._order_counter = 0
        self._orders_complete = threading.Event()

        self._order_lock = threading.Lock()
        self._watchlist_lock = threading.Lock()

        self._active_processes = 0

    @property
    def max_leverage(self):
        return self._setup.leverage

    @max_leverage.setter
    def max_leverage(self, value):
        self._setup.leverage = value

    @property
    def portfolio(self):
        return self._portfolio

    @property
    def account(self):
        return self._account

    @property
    def session_ids(self):
        return self._processes

    @property
    def broker_states(self):
        return self._broker_states

    @property
    def strategies(self):
        return self._processes.values()

    @service_access.framework
    @service_access.per_session
    def PlaceOrders(self, request_iterator, session_id):
        # todo: instead of using a locking mechanism for the strategy manager, we should have multiple
        #  channels, one per strategy, where they can write there states.

        # broker = self._broker
        # todo: we receive a single batch of orders per session that contains
        #  the process id
        new_orders = self._new_orders

        bytes_ = b''

        for chunk in request_iterator:
            bytes_ += chunk.data

        batch = broker_pb2.OrderBatch()
        batch.ParseFromString(bytes_)

        # todo: handle cases where the controllable fails, also add a timeout in case
        #  it is a hardware crash...

        processes = self._processes

        # todo: get portfolio and account data
        # check if it is the event we are waiting for...
        if conversions.to_datetime(batch.ts) == self._global_ts:
            with self._order_lock:
                if session_id in processes:
                    self._order_counter += 1

                for order in batch.orders:
                    # yield conversions.to_proto_order(
                    #     broker.order(
                    #         session_id,
                    #         conversions.to_zp_order(order)).to_dict())

                    new_orders.append(conversions.to_zp_order(order))

                # trigger the event!
                if self._order_counter == len(self._processes):
                    self._orders_complete.set()

    @service_access.framework
    @service_access.per_session
    def Stopped(self, request, session_id):
        with self._order_lock:
            self._active_processes -= 1

            if self._active_processes == 0:
                self._orders_complete.set()

            # print("Stopped!", session_id, self._active_processes)

        # if self._active_processes == 0:
        #     self._orders_complete.set()

    @service_access.framework
    @service_access.per_session
    def CancelAllOrdersForAsset(self, request, session_id):
        # todo
        self._broker.cancel_all_orders_for_asset(
            session_id,
            conversions.to_zp_asset(request))

    @service_access.framework
    @service_access.per_session
    def CancelOrder(self, request, session_id):
        # todo: if the order is placed before the controller sends orders to the
        #  broker, cancel immediately
        self._broker.cancel(
            session_id,
            conversions.to_zp_order(request.order))

    @service_access.framework
    @service_access.per_session
    def ExecuteCancelPolicy(self, request, session_id):
        # todo: this should be handled by the controller.
        # self._broker.execute_cancel_policy(
        #     session_id,
        #     request.event_type)

        # for blotter in self._blotters.values():
        #     blotter.
        pass

    @service_access.framework
    @service_access.per_session
    def SetWatchList(self, request_iterator, session_id):

        bytes_ = b''

        for chunk in request_iterator:
            bytes_ += chunk.data

        watchlist = broker_pb2.WatchList()
        watchlist.ParseFromString(bytes_)

        ts = conversions.to_datetime(watchlist.ts)

        processes = self._processes

        # strategies call this at each clock event,

        if ts == self._global_ts:
            with self._watchlist_lock:
                if session_id in processes:
                    self._watchlist_counter += 1

                # use set to remove redundant assets
                if watchlist.assets:
                    self._watchlist = set(self._watchlist) | set([conversions.to_zp_asset(asset)
                                                                  for asset in watchlist.assets])

                if self._watchlist_counter == len(self._processes):
                    self._watchlist_complete.set()

    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder

    def set_global_timestamp(self, ts):
        self._global_ts = ts

    def sync_last_sale_prices(self, calendar_name, prices, dt):
        ledger_ = self._ledgers[calendar_name]
        tracker = self._position_trackers[calendar_name]

        ledger_._dirty_portfolio = True
        tracker._dirty_stats = True

        for sid, price in prices:
            position = tracker.positions[sid].inner_position

            if not np.isnan(price):
                position.last_sale_price = price
                position.last_sale_date = dt

        # mark portfolio as dirty so that we can recompute read only portfolio

        # if dt != self._sync_times[calendar_name]:
        #
        #     ledger_.sync_last_sale_prices(
        #         dt,
        #         self._data_portals[calendar_name],
        #         self._setup.data_frequency,
        #         handle_non_market_minutes)
        #
        #     self._sync_times[calendar_name] = dt

    def get_orders(self):
        # with self._lock:
        #     print("Active Processes!", self._active_processes)
        #     if self._active_processes == 0:
        #         return self._new_orders

        self._orders_complete.wait()

        return self._new_orders

    def get_watchlist(self) -> typing.Dict[str, typing.List[zipline.assets.Asset]]:
        self._watchlist_complete.wait()

        watchlist = defaultdict(list)

        for asset in self._watchlist:
            watchlist[self._aliases[asset.exchange]].append(asset)

        return watchlist

    def place_orders(self, orders):

        # todo: send orders to the broker service...

        blotters = self._blotters
        ledgers = self._ledgers
        aliases = self._aliases

        for order in orders:
            # fixme: find a better way of getting calendar names (from disk ?)
            calendar_name = aliases[order.asset.exchange]
            blotter = blotters[calendar_name]

            if order.status == ORDER_STATUS.CANCELLED:
                # a cancellation relayed by the strategy's blotter
                # (cancel_order, cancel policy): cancel the controller's copy,
                # as zipline's blotter.cancel; an order placed and cancelled
                # in the same bar is only recorded.
                if order.id in blotter.orders:
                    blotter.cancel(order.id, relay_status=False)
                    order = blotter.orders[order.id]
                else:
                    blotter.orders[order.id] = order
                ledgers[calendar_name].process_order(order)
                continue

            blotter.open_orders[order.asset].append(order)
            blotter.orders[order.id] = order
            # record the order in the period it was placed (as zipline's
            # AlgorithmSimulator does with metrics_tracker.process_order)
            ledgers[calendar_name].process_order(order)

    def get_data_portal(self, calendar_name):
        return self._data_portals[calendar_name]

    # def get_transactions(self, calendar_name):
    #     return self._blotters[calendar_name].transactions_per_order(self._bar_data_dict[calendar_name])

    def get_positions(self, calendar_name):
        for asset, position in self._position_trackers[calendar_name].get_positions().items():
            yield position

    def get_position_assets(self, calendar_name):
        for asset, position in self._position_trackers[calendar_name].get_positions().items():
            yield asset

    def get_open_orders(self, calendar_name):
        return self._blotters[calendar_name].open_orders

    def handle_market_open(self, gts, calendar_name):
        self._ledgers[calendar_name].start_of_session(gts)

    def process_transaction(self, calendar_name, transaction):
        ledger_ = self._ledgers[calendar_name]

        blotter = self._blotters[calendar_name]
        orders = blotter.orders

        ledger_.process_transaction(transaction)
        ledger_.process_order(orders[transaction.order_id])

    def process_commission(self, calendar_name, commission):
        self._ledgers[calendar_name].process_commission(commission)

    def process_splits(self, calendar_name, splits):
        self._blotters[calendar_name].process_splits(splits)
        self._ledgers[calendar_name].process_splits(splits)

    def prune_orders(self, calendar_name, closed_orders):
        self._blotters[calendar_name].prune_orders(
            closed_orders)

    def process_dividends(self, cts, calendar_name, adjustment_reader):
        self._ledgers[calendar_name].process_dividends(
            cts,
            self._asset_finder,
            adjustment_reader)

    def add_strategy(self, process):
        process_id = process.process_id

        self._processes[process_id] = process
        self._broker_states[process_id] = broker_pb2.BrokerState()

        self._active_processes += 1

    def get_blotter(self, calendar_name):
        return self._blotters[calendar_name]

    def add_data_portal(self, calendar_name, data_portal):
        self._data_portals[calendar_name] = data_portal

    def add_blotter(self, calendar_name, blotter):
        self._blotters[calendar_name] = blotter

    def add_ledger(self, calendar_name):
        self._position_trackers[calendar_name] = tracker = position_tracker.PositionTracker(
            self._positions)

        self._ledgers[calendar_name] = ledger.Ledger(
            self._start_dt,
            self._portfolio,
            self._account,
            tracker
        )

        self._sync_times[calendar_name] = None

    def get_ledger(self, calendar_name):
        return self._ledgers[calendar_name]

    def get_orders_batches(self):
        return self._orders_batches

    def cleanup_expired_assets(self, calendar_name, dt):

        """
        Clear out any assets that have expired before starting a new sim day.
        Performs two functions:
        1. Finds all assets for which we have open orders and clears any
           orders whose assets are on or after their auto_close_date.
        2. Finds all assets for which we have positions and generates
           close_position events for any assets that have reached their
           auto_close_date.
        """

        def past_auto_close_date(asset):
            acd = asset.auto_close_date
            return acd is not None and acd <= dt

        blotter = self._blotters[calendar_name]

        data_portal = self._data_portals[calendar_name]
        ledger_ = self._ledgers[calendar_name]

        # todo: check if this block of code works with real-time data...

        position_assets = self._asset_finder.retrieve_all([position['sid'] for position in ledger_.positions])

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [asset for asset in position_assets if past_auto_close_date(asset)]

        data_frequency = self._data_frequency

        for asset in assets_to_clear:
            ledger_.close_position(asset, dt, data_portal, data_frequency)

        # Remove open orders for any sids that have reached their auto close
        # date. These orders get processed immediately because otherwise they
        # would not be processed until the first bar of the next day.

        assets_to_cancel = [
            asset for asset in blotter.open_orders
            if past_auto_close_date(asset)
        ]

        for asset in assets_to_cancel:
            blotter.cancel_all_orders_for_asset(asset)

        # Make a copy here so that we are not modifying the list that is being
        # iterated over.
        for order in blotter.new_orders.copy():
            if order.status == ORDER_STATUS.CANCELLED:
                ledger_.process_order(order)
                blotter.new_orders.remove(order)

    def clear(self):
        self._order_counter = 0
        self._watchlist_counter = 0

        self._orders_complete.clear()
        self._watchlist_complete.clear()
        self._new_orders.clear()
        self._watchlist.clear()

        broker_states = self._broker_states

        for process_id in broker_states:
            broker_states[process_id] = broker_pb2.BrokerState()

