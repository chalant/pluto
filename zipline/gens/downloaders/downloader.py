import pymongo as pm
from time import sleep
from zipline.utils.calendars import get_calendar
from zipline.utils.calendars.calendar_utils import global_calendar_dispatcher
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from collections import deque
from threading import Lock, Condition
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.exceptions import RequestException
from yahoo_historical import Fetcher
from pandas import read_csv, Timestamp
from dateutil.parser import parse
from io import StringIO
import math
from tqdm import tqdm

client = pm.MongoClient()
database = client['Main']

'''a request holds details of a request only the receiver knows how to handle the request... the middle just handles 
traffic between the receiver and the request...'''


class Request(ABC):
	def __init__(self, saver):
		self._saver = saver

	def save(self, result):
		self._saver.save(result)


class Dispatcher:
	'''Receives and dispatches requests from and to different threads...'''

	def __init__(self):
		super(Dispatcher, self).__init__()
		self._queue = deque()
		self._lock = Lock()
		self._not_empty = Condition(self._lock)
		self._listeners = []

	def get_request(self, wait=False):
		with self._not_empty:
			if self._queue:
				request = self._queue.popleft()
				return request
			else:
				if wait:
					while not self._queue:
						self._not_empty.wait()
					item = self._queue.popleft()
				else:
					item = None
				self._not_empty.notify()
				return item

	def add_request(self, request):
		with self._lock:
			if isinstance(request, (list, tuple)):
				self._queue.extend(request)
			else:
				self._queue.append(request)
			self.notify()  # notify observers so that they can make requests...

	def register(self, executor):
		self._listeners.append(executor)

	def notify(self):
		for listener in self._listeners:
			listener.update(self)

	def __len__(self):
		return len(self._queue)


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
				documents.append({'date': parse(t[date]),
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


'''it's the clients job to give the right request to the right dispatcher... 
the client provide the request, the executor and ways of saving the result of the request...'''


class EquityRequest(Request):
	def __init__(self, saver=None):
		super(EquityRequest, self).__init__(saver)
		self._start_date = None
		self._end_date = None

	@property
	def symbol(self):
		return self._symbol

	@symbol.setter
	def symbol(self, value):
		self._symbol = value

	@property
	def frequency(self):
		return self._frequency

	@frequency.setter
	def frequency(self, value):
		self._frequency = value

	@property
	def start_date(self):
		return self._start_date

	@start_date.setter
	def start_date(self, value):
		self._start_date = value

	@property
	def end_date(self):
		return self._end_date

	@end_date.setter
	def end_date(self, value):
		self._end_date = value


class Saver(ABC):

	def __init__(self):
		self._lock = Lock()
		self._prb = None

	def save(self, data):
		with self._lock:
			self._save(data)

	@abstractmethod
	def _save(self, data):
		raise NotImplementedError


class DailyEquityDataSaver(Saver):
	collection = database['equity data']

	def _save(self, data):
		symbol = data['symbol']
		self.collection.update_one({'symbol': symbol}, {'$push': {'series': {'$each': data['series']}}})
		print("saved daily equity data for: {0}".format(symbol))


def create_equity_request(saver, symbol, start_date=None, end_date=None, frequency='1D'):
	request = EquityRequest(saver)
	request.symbol = symbol
	request.frequency = frequency
	request.start_date = start_date
	request.end_date = end_date
	return request


# schedules a queue of requests to be delivered to a dispatcher...
# TODO: handle time_to_execution must be UTC!!! right now the client is the one that provides it... maybe encapsulate
# this in a class...
class Schedule:
	def __init__(self, dispatcher, time_to_execution):
		if isinstance(time_to_execution, datetime):
			self._start_time = time_to_execution  # the datetime for execution... must be utc...
		else:
			raise TypeError("Expected : {0} got : {1}".format(datetime, type(time_to_execution)))
		self._dispatcher = dispatcher
		self._requests = []

	def add_request(self, request):
		self._requests.append(request)

	def __call__(self):
		t = self._start_time - datetime.utcnow()
		sleep_time = t.total_seconds()
		if sleep_time > 0:
			sleep(sleep_time)
		self._dispatcher.add_request(self._requests)


def download():
	# also includes symbol history... if a symbol changed, we need to modify its value...
	# (from:old_symbol,to:new_symbol) => then we query and update our database with the new symbol
	# problem: we never know when a symbol will change... maybe when requesting it we don't find it, then we assume that
	# it has changed, been de-listed(check in another exchange), merged, acquired etc...
	equity_data = database["equity data"]

	saver = DailyEquityDataSaver()  # will be used by requests

	# we classify the symbols by calendars, so that we can schedule symbols together
	exc_dict = {}
	for s in database['company data'].find():
		exc = s['exchange']
		if global_calendar_dispatcher.has_calendar(exc):
			r_exc = global_calendar_dispatcher.resolve_alias(exc)
			if r_exc not in exc_dict:
				symbols = []
				exc_dict[r_exc] = symbols
			else:
				symbols = exc_dict[r_exc]
			sym = s['symbol']
			symbols.append((sym, s['ipo year']))

	av_executor = AlphaVantage("5B3LVTJKR827Y06N")  # api key...
	y_executor = Yahoo()

	daily_hist_req_dispatcher = Dispatcher()  # by default, we get up-to 20 years worth of data...
	bounded_equity_request = Dispatcher()  # for bounded requests (between a given datetime...)
	unknown_ipo_date = Dispatcher()

	# register executors
	daily_hist_req_dispatcher.register(av_executor)
	daily_hist_req_dispatcher.register(y_executor)
	bounded_equity_request.register(y_executor)
	unknown_ipo_date.register(av_executor)

	schedules = []
	dispatchers = []
	dispatchers.extend([daily_hist_req_dispatcher, bounded_equity_request, unknown_ipo_date])
	schedules.extend([av_executor, y_executor])
	pool = ThreadPoolExecutor()
	dt = datetime.utcnow()
	today = dt.date()

	def set_date(ipo):
		dt = datetime(2000, 1, 3)
		if ipo.count('n'):
			return dt
		else:
			d = parse(ipo)
			if d.year >= 2000:
				return None
			else:
				return dt

	for exchange in exc_dict:
		if not equity_data.count({'calendar': exchange}):
			equity_data.insert_many(
				[{'calendar': exchange, 'symbol': symbol, 'ipo': ipo, 'series': []} for symbol, ipo in
				 exc_dict[exchange]])

		cursor = equity_data.find({'calendar': exchange})  # data associated with the exchange
		calendar = get_calendar(exchange)  # we use this calendar to make schedules...
		if calendar.is_session(today):
			start_dt = dt
		else:
			start_dt = calendar.next_close(Timestamp(today)).to_datetime()
		bounded_hist_sch = Schedule(daily_hist_req_dispatcher, start_dt)
		schedules.append(bounded_hist_sch)
		for document in cursor:
			symbol = document['symbol']
			if not ('^' in symbol or '.' in symbol):
				series = document['series']
				if not series:
					ipo = set_date(document['ipo'])
					if not ipo:
						unknown_ipo_date.add_request(create_equity_request(saver, symbol))
					else:
						daily_hist_req_dispatcher.add_request(create_equity_request(saver, symbol, ipo))

				else:
					last_dt = sorted(i['date'] for i in series)[-1].date()  # returns a datetime object in utc
					one_day = timedelta(days=1)
					start = last_dt + one_day
					end = today - one_day
					if start < today - one_day:
						bounded_equity_request.add_request(
							create_equity_request(saver, symbol, start_date=start, end_date=end))
					# schedule for future execution
					bounded_hist_sch.add_request(create_equity_request(saver, symbol, start_date=today))
	m = 0
	for d in dispatchers:
		m += len(d)
	bar = tqdm(total=m)
	av_executor.set_progressbar(bar)
	y_executor.set_progressbar(bar)
	for schedule in schedules:
		pool.submit(schedule)

def main():
	download()


if __name__ == '__main__':
	main()
