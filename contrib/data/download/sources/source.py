from abc import ABC, abstractmethod
import time

import queue as q
import requests


DATA_TYPES = [
    'Quote',
    'EarningsEstimates',
    'IncomeStatement',
    'BalanceSheet',
    'Sustainability',
    'CashFlow',
    'OptionChains'
]

class DataResponse(object):
    __slots__ = ['_exchange','_data_type', '_value']

    def __init__(self, exchange, data_type, value):
        '''

        Parameters
        ----------
        exchange : str
        data_type : str
        value : dict

        '''
        self._exchange = exchange
        self._data_type = data_type
        self._value = value

    @property
    def exchange(self):
        return self._exchange

    @property
    def data_type(self):
        return self._data_type

    @property
    def value(self):
        #a dict
        return self._value


class FailedRequest(object):
    __slots__ = ['request', 'source']

    def __init__(self, request, source):
        self.request = request
        self.source = source

class DataRequest(object):
    __slots__ = ['_id', '_parameters', '_exchange', '_data_type']

    def __init__(self, exchange, data_type, parameters):
        self._parameters = parameters
        self._id = hash((exchange, data_type))
        self._exchange = exchange
        self._data_type = data_type

    @property
    def exchange(self):
        return self._exchange

    @property
    def data_type(self):
        return self._data_type

    def __hash__(self):
        return self._id

    def __call__(self, item):
        return item.prepare_request(self._parameters)

class Request(ABC):
    def __init__(self, request):
        self._session = requests.Session()
        self._request = request

    def send(self, proxy=None):
        return self._parse_response(
            self._session.send(
                self._request,
                proxies = {proxy.protocol: proxy.address} if proxy else None))

    @abstractmethod
    def _parse_response(self, response):
        raise NotImplementedError


class Item(object):
    @abstractmethod
    def prepare_request(self, parameters):
        raise NotImplementedError


class Source(ABC):
    def __init__(self):
        self._queues = []
        self._resources = self._get_resources()
        self._data_types_per_exchange = None

    def add_queue(self, queue):
        '''

        Parameters
        ----------
        queue : queue.Queue

        Returns
        -------

        '''
        self._queues.append(queue)

    @property
    def resources(self):
        return self._resources.values()

    def _get_resources(self):
        exchanges = self._get_exchanges()
        resources = {}
        for exchange in exchanges:
            for data_type in self._get_data_types(exchange):
                item = self._get_item(data_type)
                if item:
                    resources[(exchange, data_type)] = item
        return resources

    @property
    @abstractmethod
    def data_types_per_exchange(self):
        raise NotImplementedError

    @abstractmethod
    def _get_exchanges(self):
        raise NotImplementedError

    @abstractmethod
    def _get_data_types(self, exchange):
        raise NotImplementedError

    @abstractmethod
    def _get_item(self, data_type):
        raise NotImplementedError

    @abstractmethod
    def _wait_time(self):
        raise NotImplementedError

    def start(self, thread_pool, consumers, failed_callback):
        # todo: what about pacing (bandwidth, number of requests)?, failed requests?
        # todo: we can measure bandwidth with len(request.content)
        for queue in self._queues:
            while True:
                try:
                    request = queue.get_nowait()
                    thread_pool.submit(self._work, consumers, request, failed_callback)
                    time.sleep(self._wait_time())
                except q.Empty:
                    break

    def _work(self, consumers, request, failed_callback):
        # the request if matched to a resource
        value = self._execute(request, self._resources[request])
        # todo: consumers must be thread-safe
        if value:
            data = DataResponse(request.exchange, request.data_type, value)
            for consumer in consumers:
                consumer.ingest(data)
        else:
            # todo: we need a way to manage failed requests...
            # a request could fail for different reasons: server is down, resource not found etc.
            failed_callback(FailedRequest(request, self))

    @abstractmethod
    def _execute(self, request, item):
        pass
