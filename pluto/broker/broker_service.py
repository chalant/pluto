from pluto.interface.utils import service_access
from pluto.coms.utils import conversions

from protos import broker_pb2_grpc as brk_rpc

class BrokerService(brk_rpc.BrokerServicer):
    def __init__(self, broker):
        '''

        Parameters
        ----------
        broker: pluto.broker.broker.Broker
        '''
        self._broker = broker

    def update(self, dt, evt, signals):
        self._broker.update(dt, evt, signals)

    def compute_capital(self, capital_ratio):
        self._broker.compute_capital(capital_ratio)

    def adjust_max_leverage(self, max_leverage):
        self._broker.adjust_max_leverage(max_leverage)

    def add_market(self, session_id, start, end, universe_name):
        self._broker.add_market(session_id, start, end, universe_name)

    def _order(self, broker, request, session_id):
        return broker.order(
            session_id,
            conversions.to_zp_order(request)
        )
        # todo: should we add the order_id in the message?

    @service_access.framework_method
    @service_access.per_session
    def PlaceOrders(self, request_iterator, session_id):
        broker = self._broker
        for param in request_iterator:
            yield self._order(broker, param, session_id)

    @service_access.framework_method
    @service_access.per_session
    def CancelAllOrdersForAsset(self, request, session_id):
        self._broker.cancel_all_orders_for_asset(
            session_id,
            conversions.to_zp_asset(request.asset))

    @service_access.framework_method
    @service_access.per_session
    def CancelOrder(self, request, session_id):
        self._broker.cancel(
            session_id,
            conversions.to_zp_order(request.order.id))

    @service_access.framework_method
    @service_access.per_session
    def ExecuteCancelPolicy(self, request, session_id):
        self._broker.execute_cancel_policy(
            session_id,
            request.event_type)
