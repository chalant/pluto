from protos import broker_pb2_grpc as brk_rpc
from protos.clock_pb2 import (
    SESSION_START,
    BAR,
    TRADE_END,
    SESSION_END
)

class BrokerService(brk_rpc.BrokerServicer):
    def __init__(self, broker, address):
        self._address = address
        self._broker = broker

    @property
    def address(self):
        return self._address

    def update(self, dt, evt, signals):
        if evt == SESSION_START:
            pass

    def SingleOrder(self, request, context):
        pass

    def BatchOrder(self, request, context):
        pass

    def CancelAllOrdersForAsset(self, request, context):
        pass

    def CancelOrder(self, request, context):
        pass

    def AccountState(self, request, context):
        pass

    def Transactions(self, request, context):
        pass

    def PortfolioState(self, request, context):
        pass

    def PositionsState(self, request, context):
        pass