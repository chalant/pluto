import enum

from pluto.interface.utils import service_access
from pluto.coms.utils import conversions

from protos import broker_pb2_grpc as brk_rpc


class _States(enum.Enum):
    ACTIVE = 1
    LIQUIDATION = 2


class BrokerService(brk_rpc.BrokerServicer):
    def __init__(self, broker):
        '''

        Parameters
        ----------
        broker: pluto.broker.broker.Broker
        '''
        self._broker = broker

    @service_access.framework
    @service_access.per_session
    def PlaceOrders(self, request_iterator, session_id):
        broker = self._broker
        for order in request_iterator:
            yield conversions.to_proto_order(
                broker.order(
                    session_id,
                    conversions.to_zp_order(order)).to_dict())

    @service_access.framework
    @service_access.per_session
    def CancelAllOrdersForAsset(self, request, session_id):
        self._broker.cancel_all_orders_for_asset(
            session_id,
            conversions.to_zp_asset(request))

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
