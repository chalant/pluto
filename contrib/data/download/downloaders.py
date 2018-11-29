from datetime import datetime, date, time, timedelta

from itertools import chain

from trading_calendars.calendar_utils import global_calendar_dispatcher as gcd

from contrib.data.download.executors_factory import order_executor
from contrib.data.download.executors.executor import Schedule
from contrib.data.download.dispatchers import Dispatcher
from contrib.data.ingestion.ingesters import DailyEquityIngester

from logbook import Logger, StreamHandler

from pandas import Timestamp

import tqdm

from abc import ABC, abstractmethod

import sys

StreamHandler(sys.stdout).push_application()
log = Logger('Database')

'''database collections...'''

deq_log = Logger('DailyEquityWriter')

# TODO: handle failed requests correctly...(ship them to other executors...)
# TODO: create a config file for the api keys......
_TII_EXC = [order_executor('Tiingo', full_access=True) for i in range(10)]
_APH_EXC = [order_executor('AlphaVantage')]
_WIKI_EXC = [order_executor('Wikipedia')]


def reformat_datetime(last_update):
	if isinstance(last_update, date):
		last_update = datetime.combine(last_update, time.min)
	elif isinstance(last_update, (Timestamp, datetime)):
		last_update = datetime.combine(last_update.date(), time.min)
	return last_update


class Downloadable(ABC):
	@abstractmethod
	def get_downloader(self):
		raise NotImplementedError


# TODO: each downloader should keep track of its last update, so that it doesn't download stuff that
# it has already downloaded...
class Downloader(object):
	def __init__(self):
		self._delayed = {}
		self._requests = {}

	#TODO: the scheduling should be handled here...
	def download(self, thread_pool,today=None,event=None,show_progress=False):
		#TODO: on this event, schedule execution to today + timedelta
		if event is None:
			pass
		#download right-away (until the last market close... using date as reference )
		if not today:
			today = Timestamp.utcnow()
		self._download(thread_pool, today, self._add_request)
		for executor in self._get_executors(show_progress):
			thread_pool.submit(executor)

	@abstractmethod
	def _download(self, thread_pool, today, add_request):
		raise NotImplementedError

	def _add_request(self, time_to_execution=None, **kwargs):
		if time_to_execution:
			if time_to_execution not in self._delayed:
				d = {}
				self._delayed[time_to_execution] = d
			else:
				d = self._delayed[time_to_execution]
			self._add_request_(d, **kwargs)
		else:
			self._add_request_(self._requests, **kwargs)

	@abstractmethod
	def _add_request_(self, requests, **kwargs):
		raise NotImplementedError

	def _get_executors(self, show_progress):
		delayed = self._delayed
		if delayed:
			length = 0
			schedules = []
			for time_to_execution, requests in delayed.items():
				# fixme: check this...
				for request in requests.values():
					length += len(request.values())
					schedules.append(Schedule(self._create_executors(request), time_to_execution, self._thread_pool))
		else:
			length = sum([len(i) for i in self._requests.values()])
			schedules = self._create_executors(self._requests)
		if show_progress:
			bar = tqdm.tqdm(total=length)
			for schedule in schedules:
				schedule.set_progressbar(bar)
		return schedules

	@abstractmethod
	def _create_executors(self, requests):
		raise NotImplementedError


class GroupDownloader(Downloader):
	def __init__(self, asset_group):
		super(GroupDownloader, self).__init__()
		self._asset_group = asset_group

	def _download(self, thread_pool, today, add_request):
		self._download_by_asset_group(thread_pool, today, add_request, self._asset_group)

	@abstractmethod
	def _download_by_asset_group(self, thread_pool, today, requests, asset_group):
		raise NotImplementedError


# TODO: don't download stuff we've already downloaded...
class AssetsDownloader(GroupDownloader):
	def __init__(self, assets_collection, asset_group):
		super(AssetsDownloader, assets_collection).__init__()
		self._assets = assets_collection
		self._assets_group = asset_group

	def _download_by_asset_group(self, thread_pool, today, requests, asset_group):
		for asset, _ in asset_group:
			requests(self, symbol=asset.symbol, last_update=today)

	def _add_request_(self, requests, last_update, symbol):
		if 'meta' not in self._requests:
			a = []
			requests['meta'] = a
		else:
			a = requests['meta']
		a.append(self._assets.get_request(last_update=last_update, symbol=symbol))

	def _create_executors(self, requests):
		dispatcher = Dispatcher()
		for executor in _TII_EXC:
			dispatcher.register(executor)
		dispatcher.add_request(requests['meta'])
		return _TII_EXC


class MinutelyEODDownloader(Downloader):
	def __init__(self, asset_group):
		super(MinutelyEODDownloader, self).__init__()


class DailyEquityDownloader(GroupDownloader):
	def __init__(self, daily_equity_collection, asset_group):
		super(DailyEquityDownloader, self).__init__(asset_group)
		self._exc_ingest_pairs = None
		self._daily_equity = daily_equity_collection

	def _add_request_(self, requests, **kwargs):
		daily_equity = self._daily_equity
		if 'end_date' not in kwargs:
			end_dt = None
		else:
			end_dt = kwargs.pop('end_date')
		if 'start_date' not in kwargs:
			st_dt = None
		else:
			st_dt = kwargs.pop('start_date')
		lu = kwargs.pop('last_update')
		symbol = kwargs.pop('symbol')
		if end_dt is None and st_dt is None:
			if 'unbounded' not in requests:
				a = []
				requests['unbounded'] = a
			else:
				a = requests['unbounded']
			#fixme: this doesn't look like a good design...
			a.append(daily_equity.get_request(lu, symbol, st_dt, end_dt))
		else:
			if 'bounded' not in requests:
				a = []
				requests['bounded'] = a
			else:
				a = requests['bounded']
			a.append(daily_equity.get_request(lu, symbol, st_dt, end_dt))

	def _create_executors(self, requests):
		unbounded_dispatcher = Dispatcher()
		bounded_dispatcher = Dispatcher()
		for executor in _APH_EXC:
			unbounded_dispatcher.register(executor)
		for executor in _TII_EXC:
			bounded_dispatcher.register(executor)
		unbounded_dispatcher.add_request(requests['unbounded'])
		bounded_dispatcher.add_request(requests['bounded'])
		return _APH_EXC + _TII_EXC

	def _download_by_asset_group(self, thread_pool, today, requests, asset_group):
		get_calendar = gcd.get_calendar
		#TODO: change this method: the method only gets today and doesn't handle scheduling
		#the calendar...
		for asset, exchange in asset_group:
			calendar = get_calendar(exchange)  # we use this calendar to make schedules...
			dt = Timestamp.now(calendar.tz)
			close_time = calendar.close_time
			now = dt.time()
			delta = timedelta(minutes=5)
			start_dt = datetime.combine(today.date(), close_time) + delta
			start_time = start_dt.time()
			prev_dt = datetime.combine(today.date(), now)
			if not asset.de_listed or asset.missing:
				symbol = asset.symbol
				end = calendar.previous_close(dt).date()  # the close before the current date
				last_update = asset.last_update
				one_day = timedelta(days=1)
				if last_update:
					start = calendar.next_open(Timestamp(last_update) + one_day).date()
					# first fill the gap...
					if start <= end:
						requests(symbol=symbol, start_date=start, end_date=end, last_update=end)
						start = end + one_day  # update start date
					if calendar.is_session(today):  # then download today's data if today's a session...
						if start == today:
							if now < start_time:  # if market isn't closed yet, schedule for future execution...
								t = start_dt - prev_dt
								requests(symbol=symbol, start_date=today, last_update=today,
										 time_to_execution=t.total_seconds(),
										 thread_pool=thread_pool)
							else:  # requests will be executed right away...
								requests(symbol=symbol, start_date=today, last_update=today)
				else:
					if now >= start_time or now < calendar.open_time:
						requests(symbol=symbol, last_update=end)
					elif now < start_time:  # schedule for execution at the given time...
						t = start_dt - prev_dt
						requests(symbol=symbol, start_date=today, time_to_execution=t.total_seconds(),
								 thread_pool=thread_pool)

	def get_ingester(self,
					 metadata,
					 environ,
					 asset_db_writer,
					 minute_bar_writer,
					 daily_bar_writer,
					 adjustment_writer,
					 fundamentals_writer,
					 calendar,
					 start_session,
					 end_session,
					 cache,
					 show_progress,
					 output_dir):
		return DailyEquityIngester(metadata,daily_bar_writer,calendar,start_session,end_session,show_progress)


class SP500ConstituentsDownloader(Downloader):
	def __init__(self, sp500_const_collection):
		super(SP500ConstituentsDownloader, self).__init__()
		self._const = sp500_const_collection

	def _download(self, thread_pool, today, add_request):
		last_update = self._const.last_update
		calendar = gcd.get_calendar('NYSE')
		if not self:
			if not calendar.is_session(today):
				add_request(last_update=calendar.previous_close(today))
			else:
				add_request(last_update=today)
		else:
			if last_update < today:
				if calendar.is_session(today):
					add_request(last_update=today)

	def _create_executors(self, requests):
		dispatcher = Dispatcher()
		dispatcher.add_request(requests['sp'])
		for executor in _WIKI_EXC:
			dispatcher.register(executor)
		return _WIKI_EXC

	def _add_request_(self, requests, last_update):
		if 'sp' not in requests:
			a = []
		else:
			a = requests['sp']
		a.append(self._const.get_request(last_update=last_update))


class EquityGroup(ABC):
	def __init__(self, assets_collection):
		self._assets_collection = assets_collection

	def __iter__(self):
		for asset in self._create_generator(self._assets_collection):
			exc = asset.exchange
			if gcd.has_calendar(exc):
				yield asset, gcd.resolve_alias(exc)

	@abstractmethod
	def _create_generator(self, assets_collection):
		raise NotImplementedError

	def _merge_generators(self, func, array):
		chains = []
		i0 = array[0]
		for j in range(1, len(array)):
			if chains:
				chains.append(chain(chains.pop(), func(array[j])))
			else:
				chains.append(chain(func(i0), func(array[j])))
		return chains.pop()


class EquityExchange(EquityGroup):
	def __init__(self, asset_collection, exchange_names):
		super(EquityGroup, self).__init__(asset_collection)
		self._exchanges = exchange_names

	def _create_generator(self, assets_collection):
		return self._merge_generators(assets_collection.find_by_exchange, self._exchanges)


class EquitySymbols(EquityGroup):
	def __init__(self, asset_collection, symbols=None):
		super(EquitySymbols, self).__init__(asset_collection)
		self._symbols = symbols

	def _create_generator(self, assets_collection):
		symbols = self._symbols
		if symbols:
			assets = assets_collection.find_assets(symbols)
		else:
			assets = assets_collection.find_assets()
		return assets


class EquityIndex(EquityGroup):
	def __init__(self, asset_collection, indexes):
		super(EquityIndex, self).__init__(asset_collection)
		self._indexes = indexes

	def _create_generator(self, assets_collection):
		self._merge_generators(assets_collection.find_by_market_index, self._indexes)
