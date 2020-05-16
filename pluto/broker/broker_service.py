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
        self._session_ids = {}

    def update(self, dt, evt, signals):
        return self._broker.update(dt, evt, signals)

    def compute_capital(self, capital_ratio):
        return self._broker.compute_capital(capital_ratio)

    def adjust_max_leverage(self, max_leverage):
        return self._broker.adjust_max_leverage(max_leverage)

    def add_market(self, session_id, data_frequency, start, end, universe_name):
        self._broker.add_market(session_id, data_frequency, start, end, universe_name)

    def add_session_id(self, session_id):
        self._session_ids[session_id] = _States.ACTIVE

    def set_to_liquidation(self, session_id):
        self._session_ids[session_id] = _States.LIQUIDATION

    @property
    def session_ids(self):
        return self._session_ids

    @service_access.framework_only
    @service_access.per_session
    def PlaceOrders(self, request_iterator, session_id):
        broker = self._broker
        for order in request_iterator:
            yield conversions.to_proto_order(
                broker.order(
                    session_id,
                    conversions.to_zp_order(order)).to_dict())
        session_ids = self._session_ids

        #remove the session_id from the dict after processing
        if session_ids[session_id] == _States.LIQUIDATION:
            session_ids.pop(session_id)

    @service_access.framework_only
    @service_access.per_session
    def CancelAllOrdersForAsset(self, request, session_id):
        self._broker.cancel_all_orders_for_asset(
            session_id,
            conversions.to_zp_asset(request.asset))

    @service_access.framework_only
    @service_access.per_session
    def CancelOrder(self, request, session_id):
        self._broker.cancel(
            session_id,
            conversions.to_zp_order(request.order))

    @service_access.framework_only
    @service_access.per_session
    def ExecuteCancelPolicy(self, request, session_id):
        self._broker.execute_cancel_policy(
            session_id,
            request.event_type)
