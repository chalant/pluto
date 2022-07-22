import enum

from functools import partial

from pluto.interface.utils import service_access
from pluto.coms.utils import conversions

from protos import broker_pb2_grpc as brk_rpc
from protos import broker_pb2


class _States(enum.Enum):
    ACTIVE = 1
    LIQUIDATION = 2


def _order_generator(orders):
    for order in orders:
        yield conversions.to_zp_order(order)


class BrokerService(brk_rpc.BrokerServicer):
    def __init__(self, order_handler):
        self._order_handler = order_handler

    @service_access.framework
    @service_access.per_session
    def PlaceOrders(self, request_iterator, session_id):
        handler = self._order_handler

        # for order in request_iterator:
        #     # yield conversions.to_proto_order(
        #     #     broker.order(
        #     #         session_id,
        #     #         conversions.to_zp_order(order)).to_dict())
        #
        #     broker.order(
        #         session_id,
        #         conversions.to_zp_order(order)).to_dict()

        bytes_ = b''

        for chunk in request_iterator:
            bytes_ += chunk.data

        batch = broker_pb2.OrderBatch()
        batch.ParseFromString(bytes_)

        handler.place_orders(
            conversions.to_pandas_timestamp(batch.timestamp),
            partial(_order_generator, batch.orders),
            session_id)

    @service_access.framework
    @service_access.per_session
    def CancelAllOrdersForAsset(self, request, session_id):
        self._broker.cancel_all_orders_for_asset(
            session_id,
            conversions.to_zp_asset(request), session_id)

    @service_access.framework
    @service_access.per_session
    def CancelOrder(self, request, session_id):
        self._broker.cancel(
            session_id,
            conversions.to_zp_order(request.order))

    @service_access.framework
    @service_access.per_session
    def ExecuteCancelPolicy(self, request, session_id):
        self._broker.execute_cancel_policy(
            session_id,
            request.event_type)
