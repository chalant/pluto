from contrib.data.download.sources import source

import queue
import requests

from concurrent.futures import ThreadPoolExecutor

_EXC_TO_DTP = {
    'NYSE' : set(source.DATA_TYPES),
    'NASDAQ': set(source.DATA_TYPES),
    'BRU' : set(source.DATA_TYPES)
}

class QuoteRequest(source.Request):
    def _parse_response(self, response):
        if "Will be right back" in response.text:
            raise RuntimeError
        r = response.json()["quoteResponse"]["result"]
        if r:
            return r[0]
        else:
            raise ValueError

class Quote(source.Item):
    __slots__ = ['_base_url']

    def __init__(self):
        super(Quote, self).__init__()
        self._base_url = 'https://query1.finance.yahoo.com/v7/finance/quote?symbols={}'

    def prepare_request(self, parameters):
        # todo: parameters
        # todo: should format the parameters properly, so that they can be understood by
        #  yahoo api. ex: the ticker.
        p = parameters
        url = self._base_url.format(p.ticker)
        return QuoteRequest(requests.Request('GET', url))


class MinuteHistRequest(source.Request):
    def _parse_response(self, response):
        pass


class DayHistRequest(source.Request):
    def _parse_response(self, response):
        pass


class MinuteHistory(source.Item):
    def __init__(self):
        super(MinuteHistory, self).__init__()
        self._base_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{}'

    def prepare_request(self, parameters):
        p = parameters
        return MinuteHistRequest(
            requests.Request(
                'GET',
                self._base_url.format(p.ticker),
                params={'period1': p.start, 'period2': p.end, 'interval': '1m'}))


class DayHistory(source.Item):
    __slots__ = ['_base_url']

    def __init__(self):
        super(DayHistory, self).__init__()
        self._base_url = 'https://query1.finance.yahoo.com/v8/finance/chart/{}'

    def data_type(self):
        return 'DayHistory'

    def prepare_request(self, parameters):
        p = parameters
        url = self._base_url.format(p.ticker)
        return DayHistRequest(
            requests.Request(
                'GET',
                url,
                params={'period1': p.start, 'period2': p.end, 'interval': '1d'}))


class ProxyPool(object):
    def __init__(self):
        self._queue = queue.Queue()

    def get(self):
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return

    def put(self, proxy):
        self._queue.put(proxy)


class ProxyServer(object):
    __slots__ = ['protocol', 'address']

    def __init__(self, protocol, address):
        self.address = address
        self.protocol = protocol

    def __call__(self, request):
        return request.send(proxy={self.address, self.protocol})


class YahooFinance(source.Source):
    def __init__(self, thread_pool, proxy_pool):
        super(YahooFinance, self).__init__()
        self._thread_pool = thread_pool
        self._proxy_queue = queue.Queue()
        self._proxy_pool = proxy_pool

    def data_types_per_exchange(self):
        return _EXC_TO_DTP

    def _get_exchanges(self):
        return _EXC_TO_DTP.values()

    def _get_data_types(self, exchange):
        return _EXC_TO_DTP[exchange]

    def _get_item(self, data_type):
        if data_type == 'DayHistory':
            return DayHistory()
        elif data_type == 'MinuteHistory':
            return MinuteHistory()
        elif data_type == 'Quote':
            return Quote()
        else:
            return

    def _execute(self, request, item):
        pq = self._proxy_pool
        proxy = pq.get()
        if proxy:
            try:
                # put back the proxy for re-use
                value = proxy(request(item))
                pq.put(proxy)
                return value
            except requests.exceptions.ProxyError:
                # the proxy is probably dead, get the next available one
                return self._execute(request, item)
            except RuntimeError:
                # yahoo might be blocking the proxy server IP => try another proxy
                return self._execute(request, item)
            except ValueError:
                # can't handle the request...
                return
        else:
            return

    def _wait_time(self):
        return 0
