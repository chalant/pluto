import threading

from collections import defaultdict

from functools import partial

from trading_calendars import calendar_utils

from zipline import protocol
from zipline.protocol import BarData
from zipline.finance.order import ORDER_STATUS

from pluto.finance import ledger
from pluto.finance.metrics import tracker

from pluto.coms.utils import conversions
from pluto.interface.utils import service_access

from protos import broker_pb2_grpc as brk_rpc
from protos import broker_pb2


class OrderBatch(object):
    def __init__(self, portfolio, account, orders):
        self.portfolio = portfolio
        self.account = account
        self.orders = orders


class StrategyManager(brk_rpc.BrokerServicer):
    def __init__(self,
                 asset_finder,
                 stp):

        self._processes = {}

        self._last_sync_dt = None

        self._setup = stp

        self._start_dt = stp.start

        self._portfolio = protocol.Portfolio(stp.start, stp.capital)
        self._positions = protocol.Positions()
        self._account = protocol.Account()
        self._asset_finder = asset_finder

        # self._ledger = ledger.Ledger(
        #     start_dt,
        #     portfolio,
        #     emission_rate)

        self._ledgers = {}
        self._bar_data_dict = {}
        self._blotters = {}
        self._data_portals = {}

        self._global_ts = None

        self._per_calendar_dt = {}
        self._sync_times = {}

        self._broker_states = {}

        self._new_orders = []
        self._orders_batches = []

        self._calendar_frequencies = defaultdict(list)

        self._aliases = calendar_utils._default_calendar_aliases

        self._order_counter = 0
        self._orders_complete = threading.Event()

        self._lock = threading.Lock()

        self._active_processes = 0

    @property
    def max_leverage(self):
        return self._setup.max_leverage

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
        # broker = self._broker
        # todo: we receive a single batch of orders per session that contains
        #  the process id
        new_orders = self._new_orders

        bytes_ = b''

        for chunk in request_iterator:
            bytes_ += chunk.data

        batch = broker_pb2.OrderBatch()
        batch.ParseFromString(bytes_)

        orders_batches = self._orders_batches

        # todo: handle cases where the controllable fails, also add a timeout in case
        #  it is a hardware crash...

        processes = self._processes

        # todo: get portfolio and account data
        # check if it is the event we are waiting for...
        if conversions.to_datetime(batch.ts) == self._global_ts:
            if session_id in processes:
                self._order_counter += 1

            if batch.orders:
                orders_batches.append(batch)

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
        with self._lock:
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

    def set_asset_finder(self, asset_finder):
        self._asset_finder = asset_finder

    def set_global_timestamp(self, ts):
        self._global_ts = ts

    def set_calendar_timestamp(self, calendar_name, ts):
        self._per_calendar_dt[calendar_name] = ts

    def sync_last_sale_prices(
            self,
            calendar_name,
            dt,
            data_portal,
            data_frequency,
            handle_non_market_minutes=False):

        ledger_ = self._ledgers[calendar_name]

        if dt != self._sync_times[calendar_name]:
            ledger_.sync_last_sale_prices(
                dt,
                data_portal,
                data_frequency,
                handle_non_market_minutes)

            self._sync_times[calendar_name] = dt

    def get_orders(self):
        # with self._lock:
        #     print("Active Processes!", self._active_processes)
        #     if self._active_processes == 0:
        #         return self._new_orders

        self._orders_complete.wait()

        return self._new_orders

    def place_orders(self, orders):

        # todo: send orders to the broker service...

        blotters = self._blotters
        aliases = self._aliases

        for order in orders:
            # fixme: find a better way of getting calendar names (from disk ?)
            blotter = blotters[aliases[order.asset.exchange]]

            blotter.open_orders[order.asset].append(order)
            blotter.orders[order.id] = order
            blotter.new_orders.append(order)

    def get_data_portal(self, calendar_name):
        return self._data_portals[calendar_name]

    def get_transactions(self, calendar_name, data_frequency):
        return self._blotters[calendar_name].transactions_per_order(
            self._bar_data_dict[calendar_name + data_frequency])

    def get_positions(self, calendar_name):
        for position in self._ledgers[calendar_name].position_tracker.get_positions():
            yield position

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
            adjustment_reader
        )

    def add_strategy(self, process):
        process_id = process.process_id

        self._processes[process_id] = process
        self._broker_states[process_id] = broker_pb2.BrokerState()

        self._active_processes += 1

    def add_bar_data(
            self,
            data_frequency,
            data_portal,
            asset_restrictions):

        trading_calendar = data_portal.trading_calendar
        calendar_name = trading_calendar.name
        key = calendar_name + data_frequency

        if key not in self._bar_data_dict:
            calendar_frequencies = self._calendar_frequencies[calendar_name]

            calendar_frequencies.append(data_frequency)

            # todo: refactor bar data class such that we only have one bar data object
            #  per calendar.

            self._bar_data_dict[key] = self._create_bar_data(
                calendar_name,
                data_frequency,
                data_portal,
                asset_restrictions
            )

    def add_data_portal(self, calendar_name, data_portal):
        self._data_portals[calendar_name] = data_portal

    def add_blotter(self, calendar_name, blotter):
        self._blotters[calendar_name] = blotter

    def add_ledger(self, calendar_name):
        self._ledgers[calendar_name] = ledger.Ledger(
            self._start_dt,
            self._portfolio,
            self._account,
            self._positions
        )

        self._sync_times[calendar_name] = None

    def get_frequencies(self, calendar_name):
        return self._calendar_frequencies[calendar_name]

    def get_ledger(self, calendar_name):
        return self._ledgers[calendar_name]

    def execute_orders(self, orders):
        pass

    def get_orders_batches(self):
        return self._orders_batches

    def cleanup_expired_assets(self, calendar_name, dt, data_frequency):

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

        for asset in assets_to_clear:
            ledger_.close_position(
                asset,
                dt,
                data_portal,
                data_frequency)

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

    def _get_current_dt(self, calendar_name):
        return self._per_calendar_dt[calendar_name]

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

    def clear(self):
        self._order_counter = 0

        self._orders_complete.clear()
        self._new_orders.clear()

        broker_states = self._broker_states

        for process_id in broker_states:
            broker_states[process_id] = broker_pb2.BrokerState()

