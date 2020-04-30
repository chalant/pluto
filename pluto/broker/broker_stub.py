import abc

from pluto.interface.utils import method_access

from protos import broker_pb2_grpc

class BrokerStub(abc.ABC):
    def __init__(self, session_id):
        self._session_id = session_id

    @property
    def session_id(self):
        return self._session_id

    @method_access.session_method
    def SingleOrder(self, request, metadata=None):
        self._single_order(request, metadata)

    @method_access.session_method
    def BatchOrder(self, request, metadata=None):
        self._batch_order(request, metadata)

    @method_access.session_method
    def CancelAllOrdersForAsset(self, request, metadata=None):
        self._cancel_all_orders_for_asset(request, metadata)

    @method_access.session_method
    def CancelOrder(self, request, metadata=None):
        self._cancel_order(request, metadata)

    @method_access.session_method
    def AccountState(self, request, metadata=None):
        self._account_state(request, metadata)

    @method_access.session_method
    def Transactions(self, request, metadata=None):
        self._transactions(request, metadata)

    @method_access.session_method
    def PortfolioState(self, request, metadata=None):
        self._portfolio_state(request, metadata)

    @method_access.session_method
    def PositionState(self, request, metadata=None):
        self._position_state(request, metadata)

    @abc.abstractmethod
    def _single_order(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _batch_order(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _cancel_all_orders_for_asset(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _cancel_order(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _account_state(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _transactions(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _portfolio_state(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _position_state(self, request, metadata):
        raise NotImplementedError

class ProcessBrokerStub(BrokerStub):
    def __init__(self, channel, session_id):
        super(BrokerStub, self).__init__(session_id)
        self._stub = broker_pb2_grpc.BrokerStub(
            channel)

    def _single_order(self, request, metadata):
        self._stub.SingleOrder(request, metadata=metadata)

    def _batch_order(self, request, metadata):
        self._stub.BatchOrder(request, metadata=metadata)

    def _cancel_order(self, request, metadata):
        self._stub.CancelOrder(request, metadata=metadata)

    def _cancel_all_orders_for_asset(self, request, metadata):
        self._stub.CancelAllOrdersForAsset(request, metadata=metadata)

    def _transactions(self, request, metadata):
        self._stub.Transactions(request, metadata=metadata)

    def _portfolio_state(self, request, metadata):
        self._stub.PortfolioState(request, metadata=metadata)

    def _account_state(self, request, metadata):
        self._stub.AccountState(request, metadata=metadata)

    def _position_state(self, request, metadata):
        self._stub.PositionsState(request, metadata=metadata)