import pymongo as pm
from zipline.utils.calendars import get_calendar
from zipline.utils.calendars.calendar_utils import global_calendar_dispatcher
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pandas import Timestamp
from dateutil.parser import parse
from tqdm import tqdm
from zipline.gens.downloaders.traffic.dispatchers import Dispatcher, Schedule
from zipline.gens.downloaders.traffic.executors import order_executor
from zipline.gens.downloaders.traffic.requests import create_equity_request
from abc import ABC, abstractmethod
from threading import Lock

client = pm.MongoClient()
database = client['Main']


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
	def __init__(self, collection):
		super(DailyEquityDataSaver, self).__init__()
		self._collection = collection

	def _save(self, data):
		symbol = data['symbol']
		self._collection.update_one({'symbol': symbol}, {'$push': {'series': {'$each': data['series']}}})
		print("saved daily equity data for: {0}".format(symbol))


def download():
	# also includes symbol history... if a symbol changed, we need to modify its value...
	# (from:old_symbol,to:new_symbol) => then we query and update our database with the new symbol
	# problem: we never know when a symbol will change... maybe when requesting it we don't find it, then we assume that
	# it has changed, been de-listed(check in another exchange), merged, acquired etc...
	equity_data = database["equity data"]

	saver = DailyEquityDataSaver(equity_data)  # will be used by requests

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

	av_executor = order_executor('AlphaVantage',"5B3LVTJKR827Y06N")  # api key...
	y_executor = order_executor('Yahoo')

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
