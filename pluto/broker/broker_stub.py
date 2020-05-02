import abc

from pluto.interface.utils import service_access

from protos import broker_pb2_grpc


class BrokerStub(abc.ABC):
    def __init__(self, session_id):
        self._session_id = session_id

    @property
    def session_id(self):
        return self._session_id

    @service_access.session_method
    def PlaceOrders(self, request, metadata=None):
        self._place_orders(request, metadata)

    @service_access.session_method
    def CancelAllOrdersForAsset(self, request, metadata=None):
        self._cancel_all_orders_for_asset(request, metadata)

    @service_access.session_method
    def CancelOrder(self, request, metadata=None):
        self._cancel_order(request, metadata)

    @service_access.session_method
    def ExecuteCancelPolicy(self, request, metadata=None):
        self._execute_cancel_policy(request, metadata)

    @abc.abstractmethod
    def _execute_cancel_policy(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _place_orders(self, request_iterator, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _cancel_all_orders_for_asset(self, request, metadata):
        raise NotImplementedError

    @abc.abstractmethod
    def _cancel_order(self, request, metadata):
        raise NotImplementedError


class ProcessBrokerStub(BrokerStub):
    def __init__(self, channel, session_id):
        super(BrokerStub, self).__init__(session_id)
        self._stub = broker_pb2_grpc.BrokerStub(
            channel)

    def _place_orders(self, request_iterator, metadata):
        self._stub.PlaceOrders(request_iterator, metadata=metadata)

    def _cancel_order(self, request, metadata):
        self._stub.CancelOrder(request, metadata=metadata)

    def _cancel_all_orders_for_asset(self, request, metadata):
        self._stub.CancelAllOrdersForAsset(request, metadata=metadata)

    def _execute_cancel_policy(self, request, metadata):
        self._stub.ExecuteCancelPolicy(request, metadata=metadata)
