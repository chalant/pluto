import threading

from collections import defaultdict

from functools import partial

from trading_calendars import calendar_utils

from zipline.protocol import BarData
from zipline.finance.order import ORDER_STATUS

import pluto.finance.ledger
from pluto.interface.utils import service_access
from pluto.coms.utils import conversions

from protos import broker_pb2_grpc as brk_rpc
from protos import broker_pb2


class ProcessManager(brk_rpc.BrokerServicer):
    def __init__(self,
                 asset_finder,
                 ledger: pluto.finance.ledger.Ledger):

        self._processes = {}

        self._asset_finder = asset_finder
        self._ledger = ledger

        self._bar_data_dict = {}
        self._blotters = {}
        self._data_portals = {}

        self._global_ts = None

        self._per_calendar_dt = {}

        self._broker_states = {}

        self._new_orders = []

        self._calendar_frequencies = defaultdict(list)

        self._aliases = calendar_utils._default_calendar_aliases

        self._order_counter = 0
        self._orders_complete = threading.Event()

        self._lock = threading.Lock()

        self._active_processes = 0

    @property
    def session_ids(self):
        return self._processes

    @property
    def broker_states(self):
        return self._broker_states

    @property
    def processes(self):
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

        # todo: handle cases where the controllable fails, also add a timeout in case
        #  it is a hardware crash...

        # check if it is the event we are waiting for...
        if conversions.to_datetime(batch.ts) == self._global_ts:
            if session_id in self._processes:
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
        with self._lock:
            self._active_processes -= 1

            if self._active_processes == 0:
                self._orders_complete.set()

            print("Stopped!", session_id, self._active_processes)

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

    def set_global_timestamp(self, ts):
        self._global_ts = ts

    def set_calendar_timestamp(self, calendar_name, ts):
        self._per_calendar_dt[calendar_name] = ts

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
        aliases = self._aliases

        for position in self._ledger.position_tracker.get_positions():
            if aliases[position.exchange] == calendar_name:
                yield position

    def get_open_orders(self, calendar_name):
        return self._blotters[calendar_name].open_orders

    def handle_market_open(self, gts, calendar_name):
        self._ledger.start_of_session(
            gts)

    def process_transaction(self, calendar_name, transaction):
        ledger = self._ledger
        blotter = self._blotters[calendar_name]
        orders = blotter.orders

        ledger.process_transaction(transaction)
        ledger.process_order(orders[transaction.order_id])

    def process_commission(self, commission):
        self._ledger.process_commission(commission)

    def process_splits(self, calendar_name, splits):
        self._ledger.process_splits(splits)

    def prune_orders(self, calendar_name, closed_orders):
        self._blotters[calendar_name].prune_orders(
            closed_orders)

    def process_dividends(self, cts, adjustment_reader):
        self._ledger.process_dividends(
            cts,
            self._asset_finder,
            adjustment_reader
        )

    def add_process(self, process):
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

    def get_frequencies(self, calendar_name):
        return self._calendar_frequencies[calendar_name]

    def execute_orders(self, orders):
        pass

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
        ledger = self._ledger

        # get all positions for the calendar
        aliases = self._aliases

        # todo: check if this block of code works with real-time data...

        position_assets = self._asset_finder.retrieve_all([
            position['sid']
            for position in self._ledger.positions
            if aliases[position['sid'].exchange] == calendar_name
        ])

        # Remove positions in any sids that have reached their auto_close date.
        assets_to_clear = [
            asset for asset in position_assets
            if past_auto_close_date(asset)]

        for asset in assets_to_clear:
            ledger.close_position(
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
                ledger.process_order(order)
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

