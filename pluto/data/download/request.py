from abc import ABC, abstractmethod
from uuid import uuid4


class Requester(ABC):
    @abstractmethod
    def get_request(self, **kwargs):
        raise NotImplementedError


class Request(ABC):
    __slots__ = ['_saver', '_id']

    def __init__(self, saver):
        self._saver = saver
        self._id = uuid4().hex

    @property
    def id(self):
        return self._id

    def save(self, data):
        return self._saver.save(data)

    def __str__(self):
        return self._str()

    @abstractmethod
    def _str(self):
        raise NotImplementedError


class EquityRequest(Request):
    __slots__ = ['_symbol', '_start_date', '_end_date', '_interval']

    def __init__(self, saver, symbol, start_date, end_date, interval='1D'):
        super(EquityRequest, self).__init__(saver)
        self._start_date = start_date
        self._end_date = end_date
        self._interval = interval
        self._symbol = symbol

    @property
    def symbol(self):
        return self._symbol

    @property
    def interval(self):
        return self._interval

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    def _str(self):
        return "equity request for {0}".format(self._symbol)


class BatchEquityRequest(Request):
    '''downloads all most recent data of a list of symbols'''

    __slots__ = ['_symbols']

    def __init__(self, saver, symbols):
        super(BatchEquityRequest, self).__init__(saver)

        if not isinstance(symbols, list):
            raise TypeError

        self._symbols = symbols

    @property
    def symbols(self):
        return self._symbols


class SandPConstituents(Request):
    def _str(self):
        return "s and p constituents"


class MetaDataRequest(Request):
    __slots__ = ['_ticker']

    def __init__(self, saver, ticker):
        super(MetaDataRequest, self).__init__(saver)
        self._ticker = ticker

    def _str(self):
        return "meta data for {0}".format(self._ticker)

    @property
    def symbol(self):
        return self._ticker


def create_equity_request(saver, symbol, start_date=None, end_date=None, interval='1D'):
    return EquityRequest(saver, symbol, start_date, end_date, interval)


def create_meta_data_request(saver, symbol):
    return MetaDataRequest(saver, symbol)
