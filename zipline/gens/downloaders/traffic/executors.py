import requests
from requests.exceptions import RequestException
from yahoo_historical import Fetcher
from pandas import read_csv
from io import StringIO
import math
from abc import ABC, abstractmethod
from time import sleep
from zipline.gens.downloaders.traffic.requests import EquityRequest
from dateutil.parser import parse


class RequestExecutor(ABC):
	def __init__(self):
		self._cool_down = self._cool_down_time()
		self._used = False
		self._executing = False
		self._dispatcher_set = set()
		self._current_dispatcher = None
		self._prb = None

	def __call__(self):
		if self._dispatcher_set:
			self._executing = True
			if self._current_dispatcher is None:
				self._current_dispatcher = self._dispatcher_set.pop()
			request = self._get_request(self._current_dispatcher)
			if request:
				self._mark_used()
				self._save(request)
			else:
				self._current_dispatcher = None
			self.__call__()
		else:
			self._executing = False

	def _mark_used(self):
		if not self._used:
			self._used = True
		else:
			sleep(self._cool_down)

	def _get_request(self, dispatcher):
		return dispatcher.get_request()

	def _save(self, request):
		result = self._execute(request)
		if result:
			request.save(result)
		if self._prb:
			self._prb.update(1)

	@abstractmethod
	def _cool_down_time(self):
		raise NotImplementedError

	@abstractmethod
	def _execute(self, request):
		raise NotImplementedError

	def update(self, dispatcher):
		self._dispatcher_set.add(dispatcher)

	def set_progressbar(self, progressbar):
		self._prb = progressbar


# TODO: make a singleton of these classes, since we can only have one of these... => get an instance through a factory
# method...
class AlphaVantage(RequestExecutor):
	api_url = "https://www.alphavantage.co/query?function={0}&{1}={2}&outputsize=full&apikey={3}&datatype=csv"

	def __init__(self, api_key):
		super(AlphaVantage, self).__init__()
		self._api_key = api_key

	def _execute(self, request):
		if type(request) is EquityRequest:
			if request.frequency is '1D':
				func = 'TIME_SERIES_DAILY_ADJUSTED'
				url = self.api_url.format(func, 'symbol', request.symbol, self._api_key)
			else:
				raise NotImplementedError
			try:
				data = requests.get(url)
				data.raise_for_status()
				tr = read_csv(StringIO(data.content.decode("utf-8"))).transpose()
				key = tr.index.get_loc
				date = key('timestamp')
				open_ = key('open')
				high = key('high')
				low = key('low')
				close = key('close')
				volume = key('volume')
				dividend = key('dividend_amount')
				split = key('split_coefficient')
				documents = []
				for d in range(tr.shape[1]):
					t = tr[d]
					documents.append({'date': parse(t[date]),
									  'open': t[open_],
									  'high': t[high],
									  'low': t[low],
									  'close': t[close],
									  'volume': t[volume],
									  'dividend': t[dividend],
									  'split': t[split]})
				return {'symbol': request.symbol, 'series': documents}
			except (KeyError, Exception) as e:
				print('unable to download data for {0}'.format(request.symbol), e)
				return None

	def _cool_down_time(self):
		return 2.1


class Quandl(RequestExecutor):
	def _execute(self, request):
		raise NotImplementedError


class Yahoo(RequestExecutor):
	def _execute(self, request):
		start = self._create_date(request.start_date)
		try:
			f = Fetcher(request.symbol, start, self._create_date(request.end_date))
			historical = f.getHistorical()
			dividends = f.getDividends()
			splits = f.getSplits()
			h = historical.set_index(historical.Date)
			s = splits.set_index(splits.Date).drop(['Date'], axis=1)
			d = dividends.set_index(dividends.Date).drop(['Date'], axis=1)
			s.loc[:, 'Stock Splits'] = [eval(i) for i in s.loc[:, 'Stock Splits']]
			l = h.merge(d, left_index=True, right_index=True, how='outer').merge(s, left_index=True, right_index=True,
																				 how='outer')
			tr = l.drop(['Date'], axis=1).reset_index().transpose()
			key = tr.index.get_loc
			date = key('Date')
			open_ = key('Open')
			high = key('High')
			low = key('Low')
			close = key('Close')
			volume = key('Volume')
			dividend = key('Dividends')
			split = key('Stock Splits')
			documents = []
			for d in range(tr.shape[1]):
				t = tr[d]
				s = t[split]
				p = t[dividend]
				if math.isnan(s):
					s = 1.0
				if math.isnan(p):
					p = 0.0
				documents.append(
					{'date': parse(t[date]),
					 'open': t[open_],
					 'high': t[high],
					 'low': t[low],
					 'close': t[close],
					 'volume': t[volume],
					 'dividend': p,
					 'split': s})
			return {'symbol': request.symbol, 'series': documents}
		except RequestException:
			print('unable to download data for {0}'.format(request.symbol))
			return None
		except Exception:
			print('unable to download data for {0}'.format(request.symbol))
			return None

	def _create_date(self, datetime):
		if datetime:
			return [datetime.year, datetime.month, datetime.day]
		else:
			return None

	def _cool_down_time(self):
		return 2.1
