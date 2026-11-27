import pluto.assets.assets
from pluto.broker_service import events, service
from pluto.bus import Bus, Delivery, InMemoryBus
from pluto.control import phases
from pluto.control.processes import endpoint
from pluto.setup import run_setup

from pluto.coms.utils import conversions
from pluto.interface.utils import service_access

from protos import broker_pb2_grpc as brk_rpc
from protos import broker_pb2


_NOT_ON_THE_BUS = (
    '{}: not implemented; the strategy manager takes what the runtimes send on the bus of the run'
    ' (strategy.orders), and only a runtime in this process that runs its clock event'
    ' synchronously (running_behavior.Blocking) is on it: a runtime in another process needs the networked bus'
    ' (a later stage)')


class StrategyManager(brk_rpc.BrokerServicer):
    def __init__(
            self,
            asset_finder: pluto.assets.assets.AssetFinder,
            stp: run_setup.RunSetup,
            bus: Bus | None = None):

        # the bus of the run: the controller drives the run through it (pluto.control.phases)
        self._bus = InMemoryBus() if bus is None else bus

        self._processes = {}
        self._runtimes = {}

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

        self._global_ts = None
        # the last tick that was closed (submitted): batches for it or before are late
        self._closed_ts = None

        self._broker_states = {}

        # the batch of orders of each strategy for this tick, by process id (the bus delivers the batches of different
        # strategies in the order of their producer ids: registration order is rebuilt when they are submitted)
        self._received_orders = {}
        self._received_batches = {}

        # the batches that arrived for a tick that was already closed: (process id, clock time ns, orders)
        self._late_batches = []

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

    def _on_book_event(self, stream_event: events.StreamEvent) -> None:
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

    def _on_orders(self, delivery: Delivery) -> None:
        envelope = delivery.envelope
        self._check_session(envelope.key)
        self.receive_orders(envelope.key, envelope.payload)

    @service_access.framework
    @service_access.per_session
    def PlaceOrders(self, request_iterator, session_id):
        raise NotImplementedError(_NOT_ON_THE_BUS.format('PlaceOrders'))

    def receive_orders(self, session_id, batch):
        """The orders of a strategy's batch, kept in the order they were placed (``strategy.orders`` on the bus). A
        batch of another clock event than the current one is not applied; one for an earlier tick (it arrived after its
        tick closed) is counted, ``take_late_batches`` hands the records over to be reported."""
        # todo: we receive a single batch of orders per session that contains
        #  the process id

        # todo: handle cases where the controllable fails, also add a timeout in case
        #  it is a hardware crash...

        # todo: get portfolio and account data
        # check if it is the event we are waiting for...
        batch_ts = conversions.to_datetime(batch.ts)

        if batch_ts == self._global_ts:
            # converted first: a batch is appended whole or not at all
            orders = [conversions.to_zp_order(order) for order in batch.orders]

            self._received_orders.setdefault(session_id, []).extend(orders)
            self._received_batches[session_id] = self._received_batches.get(session_id, 0) + 1
        elif (self._global_ts is not None and batch_ts < self._global_ts) or (
                self._closed_ts is not None and batch_ts <= self._closed_ts):
            self._late_batches.append((session_id, batch.ts.ToNanoseconds(), len(batch.orders)))

    def take_late_batches(self):
        """The batches that arrived for a tick that was already closed since the last call: (process id, the tick's
        clock time in nanoseconds, number of orders)."""
        late, self._late_batches = self._late_batches, []

        return late

    @service_access.framework
    @service_access.per_session
    def Stopped(self, request, session_id):
        raise NotImplementedError(_NOT_ON_THE_BUS.format('Stopped'))

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

    def set_global_timestamp(self, ts):
        self._global_ts = ts

    @property
    def new_orders(self):
        """The orders the strategies sent in this tick: by strategy in registration order, each in the order it
        placed them."""
        received = self._received_orders

        return [order for process_id in self._processes for order in received.get(process_id, ())]

    def received_orders(self, process_id) -> int:
        """The number of orders the strategy's batches of this tick had."""
        return len(self._received_orders.get(process_id, ()))

    def received_batches(self, process_id) -> int:
        """The number of batches of orders of the strategy in this tick; an empty one counts."""
        return self._received_batches.get(process_id, 0)

    def add_strategy(self, process):
        process_id = process.process_id

        self._service.set_strategy(process_id)

        self._processes[process_id] = process
        self._broker_states[process_id] = broker_pb2.BrokerState()

        # in process, the strategy's runtime is on this bus: it answers the phases and sends its orders back
        self._runtimes[process_id] = endpoint.RuntimeEndpoint(self._bus, process)

    def runtime(self, process_id) -> endpoint.RuntimeEndpoint:
        """The endpoint of a strategy's runtime on the bus: what the runtime sends goes through it."""
        return self._runtimes[process_id]

    def add_data_portal(self, calendar_name, data_portal):
        self._service.add_data_portal(calendar_name, data_portal)

    def add_blotter(self, calendar_name, blotter):
        self._service.add_blotter(calendar_name, blotter)

    def add_ledger(self, calendar_name):
        self._service.add_ledger(calendar_name)

    def clear(self):
        """The tick is over: its orders and broker states are cleared and a batch for it that arrives from now on is
        late (counted, not applied)."""
        self._received_orders.clear()
        self._received_batches.clear()
        self._closed_ts = self._global_ts
        self._global_ts = None

        broker_states = self._broker_states

        for process_id in broker_states:
            broker_states[process_id] = broker_pb2.BrokerState()
