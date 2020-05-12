import grpc

from zipline.finance.execution import MarketOrder, StopLimitOrder, StopOrder, LimitOrder

from protos import broker_pb2_grpc as broker_rpc, broker_pb2 as br_msg, data_bundle_pb2 as dtb
from pluto.coms.utils import conversions as cv

class BrokerServicer(broker_rpc.BrokerServicer):
    '''encapsulates available services per-client'''

    # todo: must check the metadata...

    def __init__(self, bundle_factory, broker_server):
        # the bundle factory is aggregated, for caching purposes.
        self._bundle_factory = bundle_factory
        self._tokens = set()
        self._accounts = {}

        self._broker = broker_server

    def _check_metadata(self, context):
        metadata = dict(context.invocation_metadata())
        token = metadata['Token']
        if not token in self._tokens:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, 'The provided token is incorrect')

    def add_token(self, token):
        self._tokens.add(token)

    def add_account(self, account):
        self._accounts[account.token] = account

    def AccountState(self, request, context):
        # todo: these methods aren't necessary
        raise NotImplementedError

    def PortfolioState(self, request, context):
        raise NotImplementedError

    def Orders(self, request, context):
        self._check_metadata(context)
        for order in self._get_dict_values(self._broker.orders()):
            yield cv.to_proto_order(order)

    def _get_dict_values(self, dict_):
        return dict_.values()

    def BatchOrder(self, request_iterator, context):
        raise NotImplementedError

    def CancelAllOrdersForAsset(self, request, context):
        raise NotImplementedError

    def PositionsState(self, request, context):
        raise NotImplementedError

    def Transactions(self, request, context):
        self._check_metadata(context)
        for trx in self._get_dict_values(self._broker.transactions(cv.to_datetime(request.dt))):
            yield cv.to_proto_transaction(trx)

    def SingleOrder(self, request, context):
        self._check_metadata(context)
        req_style = request.style
        style = None
        if req_style == br_msg.OrderParams.MARKET_ORDER:
            style = MarketOrder()
        elif req_style == br_msg.OrderParams.LIMIT_ORDER:
            style = LimitOrder(request.limit_price)
        elif req_style == br_msg.OrderParams.STOP_ORDER:
            style = StopOrder(request.stop_price)
        elif req_style == br_msg.OrderParams.STOP_LIMIT_ORDER:
            style = StopLimitOrder(request.limit_price, request.stop_price)
        if style:
            return cv.to_proto_order(self._broker.order(cv.to_zp_asset(request.asset), request.amount, style))
        else:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'unsupported order style argument')

    def GetDataBundle(self, request, context):
        '''creates a bundle based on the specified 'domains' and sends the bundle as a stream
        of bytes.'''
        # note: returns data by chunks of 1KB by default.
        self._check_metadata(context)
        for chunk in self._bundle_factory.get_bundle(request.country_code):
            yield dtb.Bundle(data=chunk)