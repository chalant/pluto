import threading
import typing

from collections import defaultdict

from pluto.trading_calendars import calendar_utils as cu

import zipline

import pluto.assets.assets
from pluto.broker_service import events, service
from pluto.bus import Bus, InMemoryBus
from pluto.control import phases
from pluto.control.processes import endpoint
from pluto.setup import run_setup

from pluto.coms.utils import conversions
from pluto.interface.utils import service_access

from protos import broker_pb2_grpc as brk_rpc
from protos import broker_pb2


class StrategyManager(brk_rpc.BrokerServicer):
    def __init__(
            self,
            asset_finder: pluto.assets.assets.AssetFinder,
            stp: run_setup.RunSetup,
            bus: Bus | None = None):

        # the bus of the run: the controller drives the run through it (pluto.control.phases)
        self._bus = InMemoryBus() if bus is None else bus

        self._processes = {}

        self._setup = stp

        # the book of the strategy: written by the broker service, only
        self._service = service.BrokerService(
            asset_finder,
            stp.start,
            stp.capital,
            stp.data_frequency)

        # the fills are the strategies' news: sent with the next broker state
        self._service.subscribe(self._on_book_event)

        # what the strategies' runtimes send back
        self._bus.subscribe(phases.ORDERS_TOPIC, self._on_orders)
        self._bus.subscribe(phases.WATCHLIST_TOPIC, self._on_watchlist)

        self._global_ts = None

        self._broker_states = {}

        self._new_orders = []

        self._watchlist_counter = 0
        self._watchlist = set()

        self._watchlist_complete = threading.Event()

        self._order_lock = threading.Lock()
        self._watchlist_lock = threading.Lock()

        self._active_processes = 0

    @property
    def bus(self) -> Bus:
        return self._bus

    @property
    def broker(self) -> service.BrokerService:
        return self._service

    @property
    def max_leverage(self):
        return self._setup.leverage

    @max_leverage.setter
    def max_leverage(self, value):
        self._setup.leverage = value

    @property
    def session_ids(self):
        return self._processes

    @property
    def broker_states(self):
        return self._broker_states

    def _on_book_event(self, stream_event):
        event = stream_event.event

        if isinstance(event, events.Fill):
            self._broker_states[event.order.session_id].transactions.append(
                broker_pb2.TransactionCommission(
                    transaction=conversions.to_proto_transaction(event.transaction.to_dict()),
                    commission=event.commission
                ))

    def _check_session(self, session_id):
        if session_id not in self._processes:
            raise PermissionError('access to this service is forbidden: unknown session {}'.format(session_id))

    def _on_orders(self, delivery):
        envelope = delivery.envelope
        self._check_session(envelope.key)
        self.receive_orders(envelope.key, envelope.payload)

    def _on_watchlist(self, delivery):
        envelope = delivery.envelope
        self._check_session(envelope.key)
        self.receive_watchlist(envelope.key, envelope.payload)

    @service_access.framework
    @service_access.per_session
    def PlaceOrders(self, request_iterator, session_id):
        bytes_ = b''

        for chunk in request_iterator:
            bytes_ += chunk.data

        batch = broker_pb2.OrderBatch()
        batch.ParseFromString(bytes_)

        self.receive_orders(session_id, batch)

    def receive_orders(self, session_id, batch):
        """The orders of a strategy's batch, appended in the order they were placed (``strategy.orders`` on the bus,
        ``PlaceOrders`` over gRPC). A batch of another clock event than the current one is ignored."""
        # todo: instead of using a locking mechanism for the strategy manager, we should have multiple
        #  channels, one per strategy, where they can write there states.

        # todo: we receive a single batch of orders per session that contains
        #  the process id

        # todo: handle cases where the controllable fails, also add a timeout in case
        #  it is a hardware crash...

        # todo: get portfolio and account data
        # check if it is the event we are waiting for...
        if conversions.to_datetime(batch.ts) == self._global_ts:
            # converted first: a batch is appended whole or not at all
            orders = [conversions.to_zp_order(order) for order in batch.orders]

            with self._order_lock:
                self._new_orders.extend(orders)

    @service_access.framework
    @service_access.per_session
    def Stopped(self, request, session_id):
        with self._order_lock:
            self._active_processes -= 1

    @service_access.framework
    @service_access.per_session
    def CancelAllOrdersForAsset(self, request, session_id):
        raise NotImplementedError(
            'CancelAllOrdersForAsset: not implemented; strategies cancel by relaying'
            ' cancelled orders with their next batch (BrokerService.submit)')

    @service_access.framework
    @service_access.per_session
    def CancelOrder(self, request, session_id):
        raise NotImplementedError(
            'CancelOrder: not implemented; strategies cancel by relaying'
            ' cancelled orders with their next batch (BrokerService.submit)')

    @service_access.framework
    @service_access.per_session
    def ExecuteCancelPolicy(self, request, session_id):
        raise NotImplementedError(
            'ExecuteCancelPolicy: not implemented; the strategy executes its cancel'
            ' policy and relays the cancelled orders')

    @service_access.framework
    @service_access.per_session
    def SetWatchList(self, request_iterator, session_id):

        bytes_ = b''

        for chunk in request_iterator:
            bytes_ += chunk.data

        watchlist = broker_pb2.WatchList()
        watchlist.ParseFromString(bytes_)

        self.receive_watchlist(session_id, watchlist)

    def receive_watchlist(self, session_id, watchlist):
        """The watchlist of a strategy at a clock event (``strategy.watchlist`` on the bus, ``SetWatchList`` over gRPC)."""
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

    def set_global_timestamp(self, ts):
        self._global_ts = ts

    @property
    def new_orders(self):
        """The orders the strategies sent in this tick, in the order they arrived."""
        return self._new_orders

    def get_watchlist(self) -> typing.Dict[str, typing.List[zipline.assets.Asset]]:
        self._watchlist_complete.wait()

        watchlist = defaultdict(list)

        for asset in self._watchlist:
            watchlist[cu.resolve_alias(asset.exchange)].append(asset)

        return watchlist

    def add_strategy(self, process):
        process_id = process.process_id

        self._service.set_strategy(process_id)

        self._processes[process_id] = process
        self._broker_states[process_id] = broker_pb2.BrokerState()

        self._active_processes += 1

        # in process, the strategy's runtime is on this bus: it answers the phases and sends its orders back
        endpoint.RuntimeEndpoint(self._bus, process)

    def add_data_portal(self, calendar_name, data_portal):
        self._service.add_data_portal(calendar_name, data_portal)

    def add_blotter(self, calendar_name, blotter):
        self._service.add_blotter(calendar_name, blotter)

    def add_ledger(self, calendar_name):
        self._service.add_ledger(calendar_name)

    def clear(self):
        self._watchlist_counter = 0

        self._watchlist_complete.clear()
        self._new_orders.clear()
        self._watchlist.clear()

        broker_states = self._broker_states

        for process_id in broker_states:
            broker_states[process_id] = broker_pb2.BrokerState()
