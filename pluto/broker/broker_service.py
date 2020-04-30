import abc

from pluto.interface.utils import method_access
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

    @method_access.framework_method
    @method_access.per_session
    def SingleOrder(self, request, session_id):
        self._broker.order(
            session_id,
            conversions.to_zp_asset(request.asset),
            request.amount,
            conversions.to_zp_execution_style(request.style),
            request)
        # todo: should we add the order_id in the message?

    @method_access.framework_method
    @method_access.per_session
    def BatchOrder(self, request, session_id):
        pass

    @method_access.framework_method
    @method_access.per_session
    def CancelAllOrdersForAsset(self, request, session_id):
        self._broker.cancel_all_orders_for_asset(
            session_id,
            conversions.to_zp_asset(request.asset)
        )

    @method_access.framework_method
    @method_access.per_session
    def CancelOrder(self, request, session_id):
        self._broker.cancel(
            session_id,
            conversions.to_zp_order(request.order.id)
        )

    @method_access.framework_method
    @method_access.per_session
    def AccountState(self, request, session_id):
        pass

    @method_access.framework_method
    @method_access.per_session
    def Transactions(self, request, session_id):
        pass

    @method_access.framework_method
    @method_access.per_session
    def PortfolioState(self, request, session_id):
        pass
