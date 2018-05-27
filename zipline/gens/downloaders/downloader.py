import pymongo as pm
from zipline.utils.calendars import get_calendar
from zipline.utils.calendars.calendar_utils import global_calendar_dispatcher
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from dateutil.parser import parse
from tqdm import tqdm
from zipline.gens.downloaders.traffic.dispatchers import Dispatcher, Schedule
from zipline.gens.downloaders.traffic.executors.factory import order_executor
from zipline.gens.downloaders.traffic.requests import create_equity_request, SandPConstituents
from threading import Lock
from functools import partial
from pandas import Timestamp

# this program is a client to the database...
client = pm.MongoClient()
database = client['Main']


# todo: make this a decorator
class Saver:
	def __init__(self):
		self._lock = Lock()

	def save(self, saving_func):
		with self._lock:
			saving_func()


class SPYConstituentsSaver(Saver):
	def __init__(self, collection):
		super(SPYConstituentsSaver, self).__init__()
		self._collection = collection

	def _save(self, data):
		dt = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
		self._collection.insert_many([{'Date': dt, 'Constituent': d} for d in data])


def compress_dict(dict_list):
	n_dict = {}
	for dct in dict_list:
		for key, item in dct.items():
			if key in n_dict:
				a = n_dict[key]
			else:
				a = []
				n_dict[key] = a
			a.append(item)
	return n_dict


def set_date(ipo):
	dt = datetime(2000, 1, 3)
	if ipo == 'n/a':
		return dt
	else:
		d = parse(ipo)
		if d.year >= 2000:
			return d
		else:
			return dt


# todo: maybe specify the elements that we want replaced...
def clean_name(name):
	return name.replace('!', '').replace("'", '') \
		.replace(',', '').replace('.', '').replace('*', ' ').replace('-', ' ').replace('&', '')


# todo: add class A or class B terms...
def resolve_name(name1, name2):
	name1 = clean_name(name1)
	name2 = clean_name(name2)
	if name1 in name2 or name2 in name1:
		return True
	else:
		# check by word...
		arr1 = [word for word in name1.split(' ') if word and word != 'CORP' and word != 'INC']
		arr2 = [word for word in name2.split(' ') if word and word != 'CORP' and word != 'INC']
		l1 = len(arr1)
		l2 = len(arr2)
		if l1 < l2:
			counter = 0
			for i in range(l1):
				if arr1[i] in arr2:
					counter += 1
			if counter >= 1:
				return True
			else:
				abv = ''.join(arr2)
				for word in arr1:
					if word in abv:
						return True
				else:
					abv = ''.join([word[0] for word in arr2 if word != 'CO'])
					for word in arr1:
						if 'CO' in word:
							word = word.replace('CO', '')
						if abv == word:
							return True
					else:
						return False
		elif l2 < l1:
			counter = 0
			for i in range(l2):
				if arr2[i] in arr1:
					counter += 1
			if counter >= 1:
				return True
			else:
				abv = ''.join(arr1)  # check if the word is split...
				for word in arr2:
					if word in abv:
						return True
				else:
					abv = ''.join([word[0] for word in arr1 if word != 'CO'])  # check if the word is abreviated
					for word in arr2:
						if 'CO' in word:
							word = word.replace('CO', '')
						if abv == word:
							return True
					else:
						return False

		elif l1 == l2:
			for i in range(l1):
				word = arr1[i]
				if word in arr2 and len(word) > 1:
					return True
			else:
				return False
		else:
			return False

#todo: should also put prior tickers in a list...
#todo: use break instead of "found flag"
def get_spy_historical_symbols():
	print('getting historical s&p constituents')
	c = database['S&P Historical Constituents'].find({}, projection={'Dates': 0, '_id': 0, 'CUSIP': 0})
	# maps symbol to name of the company, this is a unique combination...
	d = compress_dict([i for i in c])
	symbols_company_name = {x: y for x, y in zip(d['Ticker'], d['Name'])}
	smb = symbols_company_name.keys()
	tickers = database['Tickers']
	perfect_matches = []
	probable_matches = []
	found = []
	probably_found = []
	def search_related_tickers():
		docs = tickers.find({'Related Tickers': symbol})
		if docs.count():
			for doc in docs:
				name = doc['Name']
				ticker = doc['Ticker']
				prior_tickers = doc['Prior Tickers']
				if resolve_name(symbols_company_name[symbol], name):
					if doc['Exchange'] == 'DELISTED':
						perfect_matches.append(ticker)
						found.append(symbol)
					elif prior_tickers:
						perfect_matches.append(ticker)
						found.append(symbol)
					else:
						related_tickers = doc['Related Tickers']
						if related_tickers:
							related_tickers = [i.replace('.', '') for i in related_tickers]
							if len(related_tickers) > 1:
								ticker = related_tickers[related_tickers.index(symbol)]
							else:
								ticker = related_tickers[0]
							perfect_matches.append(ticker)
							found.append(symbol)
				else:
					if prior_tickers:
						if symbol in prior_tickers:
							probable_matches.append(ticker)
							probably_found.append(symbol)

	for symbol in smb:
		doc = tickers.find_one({'Ticker': symbol})
		if doc:
			if resolve_name(symbols_company_name[symbol], doc['Name']):
				perfect_matches.append(doc['Ticker'])
				found.append(symbol)
			else:
				search_related_tickers()
		else:
			search_related_tickers()
	for pr, s in zip(probable_matches, probably_found):
		if s not in found:
			perfect_matches.append(pr)
			found.append(s)
	# not_found = list(set(smb) - set(found))
	# print('found ',sorted(found))
	# for doc in tickers.find():
	# 	related_tickers = doc['Related Tickers']
	# 	prior_tickers = doc['Prior Tickers']
	# 	sym = doc['Ticker']
	# 	name = doc['Name']
	# 	if sym in smb:
	# 		n = symbols_company_name[sym]
	# 		if resolve_name(n, name):
	# 			perfect_matches.append(sym)
	# 			found.append(sym)
	# 	elif related_tickers:
	# 		for s in [i for i in related_tickers.split(',') if i]:
	# 			if s in smb:
	# 				n = symbols_company_name[s]
	# 				if resolve_name(n, name):  # check name...
	# 					perfect_matches.append(sym)
	# 					found.append(s)
	# 				else:
	# 					if prior_tickers:
	# 						for p in [i for i in prior_tickers.split(',') if i]:
	# 							if p in smb:
	# 								perfect_matches.append(sym)
	# 								found.append(p)
	# 	elif prior_tickers:
	# 		for p in [i for i in prior_tickers.split(',') if i]:
	# 			if p in smb:
	# 				perfect_matches.append(sym)
	# 				found.append(p)
	# 	else:
	# 		pass
	#
	# not_found = sorted(list(set(smb) - set(found)))  # search name...
	# print("not found", not_found)
	# print(len(perfect_matches))
	# return found
	return perfect_matches


def classify_symbols(tickers, collection):
	exc_dict = {}
	for symbol in tickers:
		symbol = symbol.replace('.', '')
		doc = collection.find_one({'Ticker': symbol})
		if doc:
			exc = doc['Exchange']
		else:
			doc = collection.find_one({'Related Tickers': symbol})
			if doc:
				exc = doc['Exchange']
			else:
				exc = None
		if exc:
			if global_calendar_dispatcher.has_calendar(exc):
				r_exc = global_calendar_dispatcher.resolve_alias(exc)
				if r_exc not in exc_dict:
					symbols = []
					exc_dict[r_exc] = symbols
				else:
					symbols = exc_dict[r_exc]
				symbols.append(symbol)
	return exc_dict


def daily_equity_saver(collection, data, last_update):
	symbol = data['symbol']
	print('saving...', symbol)
	if not isinstance(last_update, datetime):
		dt = datetime.combine(last_update, datetime.min.time())
	else:
		dt = last_update
	collection.update_one({'Ticker': symbol},
						  {'$push': {'Series': {'$each': data['series'],'$position':0}},
						   '$set': {'Last Update': dt}}, upsert=True)
	print("saved daily equity data for: {0}".format(symbol))


def spy_constituents_saver(collection, data):
	dt = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
	collection.insert_many([{'Date': dt, 'Constituent': d} for d in data])
	print("saved data for s and p constituents")


# TODO: what if we want 'specific' timeframes ? ex:minute,day, etc... or specific assets (options,futures,etc...)
# todo: maybe create a downloader object, and setup a bunch of executors, dispatchers, map them, etc...
# then call download data, by specifying symbols, asset types, timeframe etc...
# for now we're going to stick with this function...

def download_data(thread_pool, symbols, asset_type=None, interval=None):
	# create executors...
	print("creating executors...")
	apv_executor = order_executor('AlphaVantage', "5B3LVTJKR827Y06N")  # api key...
	yho_executor = order_executor('Yahoo')
	tii_executor = order_executor('Tiingo', "595f4f7db226d74f0d20e6b80880b80e4ae67806", full_access=True)

	print('creating dispatchers')
	bounded_equity_request = Dispatcher()  # for bounded requests (between a given datetime...)
	unbounded_equity_request = Dispatcher()  # for requests for which we don't know the start_date...

	bounded_hist_sch = None
	unbounded_hist_sch = None

	# register executors
	print('registering executors')
	bounded_equity_request.register(yho_executor)
	bounded_equity_request.register(tii_executor)
	unbounded_equity_request.register(apv_executor)

	schedules = []
	dispatchers = []
	dispatchers.extend([bounded_equity_request, unbounded_equity_request])
	schedules.extend([apv_executor,tii_executor,yho_executor])

	# types of requests...
	bounded_requests = []
	unbounded_requests = []

	equity_data = database["equity data"]
	des = Saver()  #
	# requests are built here and passed to executors...
	# todo: subsequently, download minutely data... (after we're up-to-date,only download minutely data...)
	exc_dict = classify_symbols(symbols, database['Tickers'])
	for exchange in exc_dict:
		calendar = get_calendar(exchange)  # we use this calendar to make schedules...
		dt = Timestamp.now(calendar.tz)
		today = dt.date()
		close_time = calendar.close_time
		now = dt.time()
		save_func = partial(daily_equity_saver, collection=equity_data)
		delta = timedelta(minutes=5)
		start_dt = datetime.combine(today, close_time) + delta
		start_time = start_dt.time()
		prev_dt = datetime.combine(today, now)
		for symbol in exc_dict[exchange]:
			data = equity_data.find_one({'Ticker': symbol})
			end = calendar.previous_close(dt).date()  # the close before the current date
			if data:
				last_update = data['Last Update']
				start = calendar.next_open(Timestamp(last_update) + timedelta(days=1)).date()
				# first fill the gap...
				if start <= end:
					sf = partial(save_func, last_update=end)
					bounded_requests.append(create_equity_request(des, sf, symbol, start_date=start, end_date=end))
				if calendar.is_session(today):  # then download today's data if today's a session...
					if start == today:
						sf = partial(save_func, last_update=today)
						if now < start_time:  # if market isn't closed yet, schedule for future execution...
							t = start_dt - prev_dt
							if not bounded_hist_sch:
								bounded_hist_sch = Schedule(bounded_equity_request, t.total_seconds(), thread_pool)
								schedules.append(bounded_hist_sch)  # wrap a dispatcher with a schedule
							bounded_hist_sch.add_request(create_equity_request(des, sf, symbol, start_date=today))
						else:  # requests will be executed right away...
							bounded_requests.append(create_equity_request(des, sf, symbol, start_date=today))
			else:
				if now >= start_time or now < calendar.open_time:
					sf = partial(save_func, last_update=end)
					unbounded_requests.append(create_equity_request(des, sf, symbol))
				elif now < start_time:  # schedule for execution at the given time...
					sf = partial(save_func, last_update=today)
					t = start_dt - prev_dt
					if not unbounded_hist_sch:
						unbounded_hist_sch = Schedule(unbounded_equity_request, t.total_seconds(), thread_pool)
						schedules.append(unbounded_hist_sch)
					unbounded_hist_sch.add_request(create_equity_request(des, sf, symbol))

	unbounded_equity_request.add_request(unbounded_requests)
	bounded_equity_request.add_request(bounded_requests)
	bar = tqdm(total=sum([len(dispatcher) for dispatcher in dispatchers]))
	apv_executor.set_progressbar(bar)
	yho_executor.set_progressbar(bar)
	tii_executor.set_progressbar(bar)

	for schedule in schedules:  # submit all functions to be executed...
		thread_pool.submit(schedule)


# this runs once a day...
if __name__ == '__main__':
	with ThreadPoolExecutor() as pool:
		scc = database['S&P Constituents']
		# scd = Dispatcher()
		# w_exe = order_executor('Wikipedia')
		# scs = Saver()
		# scd.register(w_exe)
		# request = SandPConstituents(scs)
		# request.saving_func = partial(spy_constituents_saver, collection=scc)
		# scd.add_request(request)
		# w_exe()
		symbols = [data['Constituent']['Ticker symbol']
				   for data in scc.find({})]
		# h = get_spy_historical_symbols()
		download_data(pool, ['AABA'])
