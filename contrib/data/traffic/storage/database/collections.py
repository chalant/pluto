import os

from contrib.data.traffic.storage.database.mongo_utils import get_collection
from contrib.data.traffic.download.request import (
	SandPConstituents,
	EquityRequest,
	MetaDataRequest)
from contrib.data.traffic.events.reception import DataReception

from contrib.data.traffic.download.downloaders import (
	SP500ConstituentsDownloader,
	DailyEquityDownloader,
	Downloadable,
	AssetsDownloader)

from zipline.data.bundles.csvdir import csvdir_equities
from zipline.data.bundles import clean, ingest
from zipline.data.bundles import dynamic

import numpy as np

import math

from functools import partial

from itertools import chain

from dateutil.parser import parse

from datetime import datetime,timedelta,date,time

from pandas import (
	Timestamp,
	DataFrame,
	DatetimeIndex)

from calendar import monthrange

from trading_calendars.calendar_utils import global_calendar_dispatcher

from logging import Logger

import tqdm

_DAILY_EQUITY_DATA = get_collection('equity data')
_SP500_HISTORICAL_CONSTITUENTS = get_collection('S&P Historical Constituents')
_SP500_CONSTITUENTS = get_collection('SP500 Constituents')
_META_DATA = get_collection('AvailableTickers')
_TICKERS = get_collection('Tickers')

_INDEXES = ['SP500']
_TYPES = ['equity']
_EXCHANGES = ['NYSE', 'NASDAQ']

deq_log = Logger('DailyEquityWriter')

def merge_generators(func, array):
	chains = []
	i0 = array[0]
	for j in range(1, len(array)):
		if chains:
			chains.append(chain(chains.pop(), func(array[j])))
		else:
			chains.append(chain(func(i0), func(array[j])))
	return chains.pop()

def reformat_datetime(last_update):
	if isinstance(last_update, date):
		last_update = datetime.combine(last_update, time.min)
	elif isinstance(last_update, (Timestamp, datetime)):
		last_update = datetime.combine(last_update.date(), time.min)
	return last_update

class Assets(Downloadable):
	def __init__(self):
		self._observers = []
	# function to update symbols names, etc...
	def _resolve_symbol_conflicts(self):
		with tqdm.tqdm(total=_META_DATA.count()) as progress:
			for data in _META_DATA.find({}):
				ticker = data['Ticker']
				if '-' in ticker or '.' in ticker:
					d = _TICKERS.find({'Related Tickers': ticker.replace('-', '.')})
					for c in d:
						exchange = c['Exchange']
						if exchange == 'DELISTED':
							exchange = c['Delisted From']
							delisted = True
						else:
							delisted = False
						n1 = data['Name']
						t = c['Ticker']
						if '.' in ticker:
							_META_DATA.update_one({'Ticker': ticker},
												  {'$set': {'Ticker': ticker.replace('.', '-'),
															'Exchange': exchange}})
						t0 = ticker.replace('-', '').replace('.', '')
						if t0 != t:
							related_tickers = [i.replace('.', '') for i in c['Related Tickers']]
							t = related_tickers[related_tickers.index(t0)]
						if n1:
							if self._resolve_name(n1, c['Name']):
								progress.set_description('Processing {} {}'.format(ticker, t))
								for m in _META_DATA.find({'Ticker': t}):
									n2 = m['Name']
									if n2:
										if self._resolve_name(n1, n2):
											# update with the rest...
											_META_DATA.update_one({'Ticker': ticker},
																  {'$set': {'Name': n1,
																			'Delisted': delisted,
																			'Exchange': exchange}})
											_DAILY_EQUITY_DATA.update_one({'Ticker': t},
																		  {'$set': {'Ticker': ticker}})
											break
									else:
										_META_DATA.update_one({'Ticker': ticker},
															  {'$set': {'Name': n1,
																		'Delisted': delisted,
																		'Exchange': exchange}})
										_DAILY_EQUITY_DATA.update_one({'Ticker': t},
																	  {'$set': {'Ticker': ticker}})
										break
						else:
							_META_DATA.update_one({'Ticker': ticker},
												  {'$set': {'Name': c['Name'],
															'Delisted': delisted,
															'Exchange': exchange}})
							_DAILY_EQUITY_DATA.update_one({'Ticker': t},
														  {'$set': {'Ticker': ticker}})
							break
				progress.update(1)

	def _find_by_name(self, symbol, name):
		doc = _META_DATA.find_one({'Ticker': symbol})
		if doc:
			if self._resolve_name(name, doc['Name']):
				return self._create_asset(doc)
		else:
			return

	# TODO: should use multiprocessing since this is quite heavy...
	# TODO: when ticker changed, set ticker to new ticker, and prior ticker to ticker
	# watch out: some times we're wrong... be careful...

	def update_available_data(self):
		def ticker_changed(ticker, name):  # checks if a ticker has changed an
			doc = _TICKERS.find_one({'Ticker': ticker})

			def get_delisted(doc):
				exchange = doc['Exchange']
				delisted = False
				if exchange == 'DELISTED':
					exchange = doc['Delisted From']
					delisted = True
				return exchange, delisted

			if doc:
				if self._resolve_name(doc['Name'], name):
					return (ticker,) + get_delisted(doc)
				else:
					rt = _TICKERS.find({'Related Tickers': ticker.replace('-', '.')})
					if rt:
						for r in rt:
							if self._resolve_name(r['Name'], name):
								tic = r['Ticker']
								arr = [i.replace('.', '-') for i in r['Related Tickers'] if
									   i.replace('.', '') == tic]
								if arr:
									return (arr.pop(),) + get_delisted(r)
								else:
									return (tic,) + get_delisted(r)
						return '', '', ''
					else:
						return '', '', ''
			else:
				rt = _TICKERS.find({'Related Tickers': ticker.replace('-', '.')})
				if rt:
					for r in rt:
						if self._resolve_name(r['Name'], name):
							tic = r['Ticker']
							arr = [i.replace('.', '-') for i in r['Related Tickers'] if i.replace('.', '') == tic]
							if arr:
								return (arr.pop(),) + get_delisted(r)
							else:
								return (tic,) + get_delisted(r)
					return '', '', ''
				else:
					return '', '', ''

		'''does some maintenance to ensure that everything is correct...'''
		# TODO: must check if a ticker is in the related tickers... so that we don't get data on the same company
		# TODO: if data is de-listed as-well (in metadata) can help confirm that the ticker has changed (beyond the name)
		with tqdm.tqdm(total=_META_DATA.count()) as progress:
			for data in _META_DATA.find({}):
				ticker = data['Ticker']
				d = _DAILY_EQUITY_DATA.find_one({'Ticker': ticker})
				try:
					name = data['Name']
				except KeyError:
					name = None
				if name:
					if not d:
						changed, exchange, delisted = ticker_changed(ticker, name)
						# if the ticker has not changed or we don't know, mark it as missing...
						if not changed or changed == ticker:
							st_dt = data['StartDate']
							if isinstance(st_dt, str):
								st_dt = parse(st_dt)
							if st_dt:
								end_dt = st_dt - timedelta(days=1)
								_META_DATA.update_one({'Ticker': ticker}, {'$set': {'EndDate': end_dt,
																					'StartDate': st_dt,
																					'Missing': True,
																					'Delisted': delisted}})
							else:
								_META_DATA.update_one({'Ticker': ticker}, {'$set': {'Missing': True,
																					'StartDate': st_dt,
																					'Delisted': delisted}})
						else:
							d = _DAILY_EQUITY_DATA.find_one({'Ticker': changed})
							data = _META_DATA.find_one({'Ticker': changed})
							progress.set_description('Ticker changed from {} to {}'.format(ticker, changed))
							if not d:
								if data:
									if 'StartDate' in data:
										st_dt = data['StartDate']
										if isinstance(st_dt, str):
											st_dt = parse(st_dt)
										if st_dt:
											end_dt = st_dt - timedelta(days=1)
										else:
											end_dt = None
										_META_DATA.update_one({'Ticker': changed}, {'$set': {'EndDate': end_dt,
																							 'StartDate': st_dt,
																							 'Missing': True,
																							 'Delisted': delisted}})
									else:
										_META_DATA.update_one({'Ticker': changed}, {'$set': {'EndDate': None,
																							 'StartDate': None,
																							 'Missing': True,
																							 'Exchange': exchange,
																							 'Delisted': delisted}})

							else:
								series = d['Series']
								if data:
									if series:
										_META_DATA.update_one({'Ticker': changed},
															  {'$set': {
																  'EndDate': sorted([i['Date'] for i in series])[-1],
																  'Missing': False,
																  'Delisted': delisted}})
									else:
										_META_DATA.update_one({'Ticker': changed},
															  {'$set': {
																  'Missing': False,
																  'Delisted': delisted
															  }
															  })
					else:
						series = d['Series']
						dc = _TICKERS.find_one({'Ticker': ticker})
						if dc:
							delisted = dc['Exchange'] == 'DELISTED'
						else:
							delisted = None
						if series:
							_META_DATA.update_one({'Ticker': ticker},
												  {'$set': {'EndDate': sorted([i['Date'] for i in series])[-1],
															'Missing': False,
															'Delisted': delisted}})
						else:
							_META_DATA.update_one({'Ticker': ticker},
												  {'$set': {'Missing': True,
															'Delisted': delisted}})
				progress.update(1)

	def _create_asset(self, doc):
		fields = ['Name', 'Exchange', 'Ticker', 'Delisted', 'StartDate', 'EndDate', 'Missing']
		bools = [field in doc for field in fields]
		for field, b in zip(fields, bools):
			if not b:
				doc[field] = ''
		return Asset(doc['Name'], doc['Exchange'], doc['Ticker'], doc['Delisted'], doc['StartDate'], doc['EndDate'],
					 doc['Missing'])

	# todo: include types...
	def find_assets(self, symbols=None, type='equity'):
		for symbol in symbols:
			doc = _META_DATA.find_one({'Ticker': symbol})
			if doc:
				yield self._create_asset(doc)

	def find_by_exchange(self, exchange):
		if exchange not in _EXCHANGES:
			raise StopIteration
		else:
			cursor = _META_DATA.find({'Exchange': exchange})
			if cursor.count():
				for c in cursor:
					yield self._create_asset(c)
			else:
				raise StopIteration

	def find_by_market_index(self, index_name, datetime=None):
		'''if datetime is not provided, return the most recent..., if datetime is out of range, we return the most
		recent '''
		if index_name not in _INDEXES:
			raise StopIteration
		else:
			if datetime:
				if index_name == 'sp500':
					cursor = _SP500_CONSTITUENTS.find({'Dates': datetime})
					if cursor.count():
						for c in cursor:
							yield self._create_asset(_META_DATA.find_one({'Ticker': c['Ticker']}))
					else:
						cursor = _SP500_CONSTITUENTS.find({'Dates': _SP500_CONSTITUENTS.find_one({})['LastUpdate']})
						for c in cursor:
							ticker = c['Ticker'].replace('.', '')
							doc = _META_DATA.find_one({'Ticker': ticker})
							yield self._create_asset(doc)
				else:
					raise StopIteration
			else:
				cursor = _SP500_CONSTITUENTS.find({'Dates': _SP500_CONSTITUENTS.find_one({})['LastUpdate']})
				if cursor.count():
					for c in cursor:
						# TODO: some modifications must be done to the ticker names...
						ticker = c['Ticker'].replace('.', '')
						doc = _META_DATA.find_one({'Ticker': ticker})
						yield self._create_asset(doc)
				else:
					raise StopIteration

	def find_by_date(self, datetime, index_name=None, exchange=None):
		if index_name:
			if index_name in _INDEXES:
				cursor = _SP500_HISTORICAL_CONSTITUENTS.find(
					{'Dates': self._to_end_month(datetime), 'HaveData': True},
					projection={'Dates': 0, '_id': 0, 'CUSIP': 0})
				if cursor:
					for c in cursor:
						ticker = c['Ticker']
						doc = _META_DATA.find_one({'Ticker': ticker})  # contains the latest tickers...(assumption)
						name = c['Name']
						if doc:
							# if self._resolve_name(name, doc['Name']):  # make sure it is the correct value
							yield self._create_asset(doc)
						else:  # check for previous tickers...
							results = _TICKERS.find({'Related Tickers': ticker})
							for result in results:
								# search for the current corresponding ticker...
								doc = _META_DATA.find_one({'Ticker': result['Ticker']})
								if self._resolve_name(result['Name'], name):
									yield self._create_asset(doc)
				else:
					raise StopIteration
			else:
				raise StopIteration
		elif exchange:
			if exchange in _EXCHANGES:
				cursor = _DAILY_EQUITY_DATA.find({'Exchange': exchange, 'Series.Date': datetime},
												 projection={'Series': 0, 'Last Update': 0, 'End Update': 0})
				if cursor.count():
					for c in cursor:
						yield self._create_asset(_META_DATA.find_one({'Ticker': c['Ticker']}))
				else:
					raise StopIteration
			else:
				raise StopIteration

		elif index_name and exchange:
			raise StopIteration

	def _to_end_month(self, date_):
		if isinstance(date_, date):
			date_ = datetime.combine(date_, time.min)
		date_ = date_.replace(day=monthrange(date_.year, date_.month)[1])  # replace day by last day of the month
		return date_

	def find_asset(self, symbol=None, name=None, type='equity'):  # type (future, equity, ...)
		if not name:
			doc = _META_DATA.find_one({'Ticker': symbol})
			if doc:
				return self._create_asset(doc)
			else:
				doc = _TICKERS.find({'Related Tickers': symbol})
				if doc.count():
					assets = []
					for d in doc:
						ticker = d['Ticker']
						dta = _META_DATA.find_one({'Ticker': ticker})
						if dta:
							assets.append(self._create_asset(dta))
					return assets
		else:
			# FIXME: fix this part...
			return self._find_by_name(symbol, name)

	# perfect_matches = []
	# probable_matches = []
	# found = {}
	# probably_found = []
	#
	# def add_found(searched, found_):
	# 	if searched in found:
	# 		found[searched].append(found_)
	# 	else:
	# 		a = []
	# 		a.append(found_)
	# 		found[searched] = a
	#
	# def search_related_tickers(symbol):
	# 	docs = _tickers.find({'Related Tickers': symbol})
	# 	if docs.count():
	# 		for doc in docs:
	# 			exchange = doc['Exchange']
	# 			if exchange == 'DELISTED':
	# 				exchange = doc['Delisted From']
	# 				delisted = True
	# 			else:
	# 				delisted = False
	# 			name_ = doc['Name']
	# 			ticker = doc['Ticker']
	# 			prior_tickers = doc['Prior Tickers']
	# 			if self._resolve_name(name, name_):
	# 				if delisted:
	# 					perfect_matches.append(ticker)
	# 					add_found(symbol, Asset(name_, exchange, ticker, delisted, None, None, None))
	# 				elif prior_tickers:
	# 					perfect_matches.append(ticker)
	# 					add_found(symbol, Asset(name_, exchange, ticker, delisted, None, None, None))
	# 				else:
	# 					related_tickers = doc['Related Tickers']
	# 					if related_tickers:
	# 						related_tickers = [i.replace('.', '') for i in related_tickers]
	# 						if len(related_tickers) > 1:
	# 							ticker = related_tickers[related_tickers.index(symbol)]
	# 						else:
	# 							ticker = related_tickers[0]
	# 						perfect_matches.append(ticker)
	# 						add_found(symbol, Asset(name_, exchange, ticker, delisted, None,
	# 												None, None))
	# 			else:
	# 				if prior_tickers:
	# 					if symbol in prior_tickers:
	# 						probable_matches.append(
	# 							Asset(name_, exchange, ticker, delisted, None, None,None))
	# 						probably_found.append(symbol)
	#
	# doc = _tickers.find_one({'Ticker': symbol})
	# if doc:
	# 	ticker = doc['Ticker']
	# 	name_ = doc['Name']
	# 	exchange = doc['Exchange']
	# 	if exchange == 'DELISTED':
	# 		delisted = True
	# 		exchange = doc['Delisted From']
	# 	else:
	# 		delisted = False
	# 	if self._resolve_name(name, name_):
	# 		perfect_matches.append(ticker)
	# 		add_found(symbol, Asset(name_, exchange, ticker, delisted, None, None, None))
	# 	else:
	# 		search_related_tickers(symbol)
	# else:
	# 	search_related_tickers(symbol)
	# for pr, s in zip(probable_matches, probably_found):
	# 	if s not in found:
	# 		perfect_matches.append(pr)
	# 		add_found(s, pr)
	# results = []
	# for value in found.values():
	# 	# find unique symbol...
	# 	if len(value) > 1:
	# 		for v in value:
	# 			doc = _daily_equity_data.find_one({'Ticker': v.symbol}, projection={'Series': 0})
	# 			if doc:
	# 				results.append(v)
	# 				break
	# 	else:
	# 		results.append(value[0])
	# return results

	def _clean_name(self, name):
		if '(' and ')' in name:
			name = name[0:name.index('(')] + name[name.index(')') + 1:]
		return name.replace('!', '').replace("'", '') \
			.replace(',', '').replace('.', '').replace('*', ' ').replace('-', ' ').replace('&', '')

	def _resolve_name(self, name1, name2):
		name1 = self._clean_name(name1)
		name2 = self._clean_name(name2)
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
						abv = ''.join(
							[word[0] for word in arr1 if word != 'CO'])  # check if the word is abbreviated
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

	def _meta_data_writer(self, data, last_update):
		dt = reformat_datetime(last_update)
		_META_DATA.update_one({'Ticker': data['Ticker']}, {'$set': {'StartDate': data['StartDate'],
																	'Name': data['Name'],
																	'Exchange': data['Exchange'],
																	'Description': data['Description'],
																	'LastUpdate': dt}})

	def get_request(self,last_update,symbol):
		return MetaDataRequest(Writer(saving_func=partial(self._meta_data_writer,last_update=last_update)),symbol)

	def get_downloader(self,asset_group):
		return AssetsDownloader(self,asset_group)

	def subscribe_reception_event(self,observer):
		'''observer must be a callable that takes-in an event and a the sender
		as parameters'''
		self._observers.append(observer)

	def _on_reception(self,symbol,datetime):
		for observer in self._observers:
			observer(self,DataReception(symbol,datetime))

class Writer:
	__slots__ = ['_saving_func']

	def __init__(self, saving_func):
		self._saving_func = saving_func

	def save(self, data):
		return self._saving_func(data=data)

class DailyEquity(Downloadable):
	def __init__(self):
		self._bundle_names = []
		self._exc_ingest_pairs = None

	# todo: remove data that have a dividend on their first day...(because
	def _clean_data(self, series, calendar, ticker, start_session, progress):
		'''does some clean-up routine, to ensure that the quality of the data is good'''
		if series:
			dates = DatetimeIndex([Timestamp(s['Date'], tz='UTC') for s in series])
			st_dt = dates[0]
			tsr_idx = calendar.sessions_in_range(st_dt, dates[-1])
			duplicates = dates.duplicated()
			# does some cleaning...
			if np.all(duplicates, axis=0):
				self._reset_data(ticker)
				progress.set_description('Reset data for {} due to many duplicates'.format(ticker))
				series = None
			elif np.any(duplicates, axis=0):
				dates = dates.drop_duplicates()
				missing = tsr_idx.difference(dates)
				if len(missing) > 0:
					self._reset_data(ticker)
					progress.set_description('Reset data for {} due to bad quality'.format(ticker))
					series = None
				else:
					# updates the database with cleaned-up data (where we removed duplicates)
					series = DataFrame.from_records(series).drop_duplicates().to_dict('records')
					_DAILY_EQUITY_DATA.update_one({'Ticker': ticker},
												  {'$set': {'Series': series}})
					progress.set_description('Removed duplicates for {}'.format(ticker))
			if series:
				missing = tsr_idx.difference(dates)
				flag = False
				if len(missing) > 0:
					flag = True
					for date in missing:
						series.append({'Date': date.to_datetime(), 'Open': np.nan,
									   'High': np.nan, 'Low': np.nan, 'Close': np.nan,
									   'Volume': np.nan, 'Dividend': np.nan, 'Split': np.nan})
				# add the new dates...
				series = DataFrame.from_records(series)
				series = series.rename(index=str, columns={'Date': 'date',
														   'Open': 'open',
														   'High': 'high',
														   'Low': 'low',
														   'Close': 'close',
														   'Volume': 'volume',
														   'Dividend': 'dividend',
														   'Split': 'split'})
				series.set_index('date', inplace=True)
				series.sort_index(inplace=True)
				if flag:
					series.fillna(method='ffill', inplace=True)
				if st_dt < start_session:  # only consider data that is above the start session
					series = series[series.index.get_loc(start_session):]
				series = self._check_close_volume(self._check_dividend(series))
				if series is None:
					self._reset_data(ticker)
			return series

	'''the ingester cleans data...'''
	def to_directory(self, root_path, start_session, transfer=False):
		exc_ingest_pairs = {}
		if self:
			'''saves file into a directory...'''
			root = os.path.join(root_path, "daily")
			if not os.path.exists(root):
				os.mkdir(root)
			cursor = _DAILY_EQUITY_DATA.find({})
			exchanges = []
			total = _DAILY_EQUITY_DATA.count()
			with tqdm.tqdm(total=total, desc='Transfering to directory...') as progress:
				for data in cursor:
					ticker = data['Ticker']
					doc = _META_DATA.find_one({'Ticker': ticker})
					if doc:
						exc = doc['Exchange']
						if global_calendar_dispatcher.has_calendar(exc):
							calendar_name = global_calendar_dispatcher.resolve_alias(exc)
							exchanges.append(calendar_name)
							if transfer:
								calendar = global_calendar_dispatcher.get_calendar(calendar_name)
								series = self._clean_data(data['Series'], calendar, ticker, start_session, progress)
								self._transfer(root, series, ticker)
					progress.update(1)
			if exchanges:
				for exchange in exchanges:
					exc_ingest_pairs[exchange] = csvdir_equities(('daily',), csvdir=root_path, exchange=exchange)
				self._exc_ingest_pairs = exc_ingest_pairs
			else:
				self._exc_ingest_pairs = None

	def _transfer(self, root_path, series, ticker):
		if series is not None:
			path = os.path.join(root_path, ticker + '.csv')
			series.to_csv(path, columns=['open', 'high', 'low', 'close', 'volume', 'dividend',
										 'split'])

	def _check_dividend(self, series):
		if series is not None:
			dividend = series['dividend'].iloc[0]
			if math.isnan(dividend):
				return
			elif dividend != 0.0:
				return self._check_dividend(series[1:])
			else:
				return series
		else:
			return

	def _check_close_volume(self, series):
		if series is not None:
			for c, v in zip(series['close'], series['volume']):
				if math.isnan(c) or math.isnan(v):
					return
			return series
		else:
			return

	'''the ingester does this in the cleaning routine...'''
	def _reset_data(self, symbol):
		doc = _META_DATA.find_one({'Ticker': symbol})
		start_date = doc['StartDate']
		if start_date:
			end_date = start_date - timedelta(days=1)
		else:
			end_date = None
		_META_DATA.update_one({'Ticker': symbol}, {'$set': {'EndDate': end_date, 'Missing': True}})
		_DAILY_EQUITY_DATA.delete_one({'Ticker': symbol})

	'''ingester does this...'''
	def ingest_data(self, end_session, to_directory=False):
		deq_log.info('Ingesting data...')
		path = os.path.join(os.environ['HOME'], 'FinancialData')
		if not os.path.exists(path):
			os.mkdir(path)
		st_sess = Timestamp(datetime(2008, 1, 2)).tz_localize(tz='UTC')  # this is as far of data we have...
		self.to_directory(path, transfer=to_directory, start_session=st_sess)
		names = []
		for key, func in self._exc_ingest_pairs.items():
			name = '{}_bundle'.format(key)
			self._bundle_names.append(name)
			calendar = global_calendar_dispatcher.get_calendar(key)
			if not calendar.is_session(end_session):
				end_session = Timestamp(
					datetime.combine(calendar.previous_close(end_session).date(), time.min)).tz_localize(tz='UTC')
			dynamic.add_register_func(name, func, key, st_sess, end_session)
			names.append(name)
		dynamic.register_all()
		for name in names:
			clean(name=name, keep_last=0)  # clean everything before ingesting...
			ingest(name=name, show_progress=True)

	@property
	def bundle_names(self):
		return self._bundle_names

	def get_request(self, last_update, symbol, start_date, end_date):
		return EquityRequest(Writer(partial(self._daily_equity_writer, last_update=last_update)),
							 symbol, start_date, end_date)

	def _daily_equity_writer(self, data, last_update):
		symbol = data['symbol']
		try:
			series = sorted([dt['Date'] for dt in data['series']])
			dt = reformat_datetime(last_update)
			'''we make sure that we don't add data that we already have...'''
			doc = _META_DATA.find_one({'Ticker': symbol})
			if doc:
				lu = doc['EndDate']
				if series[0] <= lu:
					data['series'] = [i for i in data['series'] if i > lu]  # keep the same order...

				_DAILY_EQUITY_DATA.update_one({'Ticker': symbol},
											  {'$push': {'Series': {'$each': data['series']}},
											   '$set': {'Last Update': dt}}, upsert=True)
				_META_DATA.update_one({'Ticker': symbol}, {'$set': {'EndDate': dt, 'Missing': False}})
				return "Saved daily equity data for: {0}".format(symbol)
			else:
				return "Couldn't save daily equity data for: {0}".format(symbol)
		# TODO: maybe update meta data about this..., ex: delisted etc...
		except Exception as e:
			return "Couldn't save daily equity data for: {0}, reason: {1}".format(symbol, e)

	def get_downloader(self,asset_group=None):
		return DailyEquityDownloader(self,asset_group)

class SP500Constituents(Downloadable):
	@property
	def last_update(self):
		return _SP500_CONSTITUENTS.find_one({})['LastUpdate']

	def _sp500_constituents_saver(self, data, last_update):
		last_update = reformat_datetime(last_update)
		for d in data:
			_SP500_CONSTITUENTS.update_one({'Ticker': d['Ticker symbol']},
										   {'$push': {'Dates': last_update}}, upsert=True)
		_SP500_CONSTITUENTS.update_many({}, {'$set': {'Last Update': last_update}})
		return 'Saved sp500 constituents'

	def get_request(self,last_update):
		return SandPConstituents(Writer(partial(self._sp500_constituents_saver, last_update)))

	def get_downloader(self,asset_group):
		return SP500ConstituentsDownloader(self)

class Asset:
	__slots__ = ['_name', '_exchange', '_symbol', '_de_listed', '_start_date', '_end_date', '_missing']

	def __init__(self, name, exchange, symbol, de_listed, start_date, end_date, missing):
		self._name = name
		self._exchange = exchange
		self._symbol = symbol
		self._de_listed = de_listed
		self._start_date = start_date
		self._end_date = end_date
		self._missing = missing

	@property
	def missing(self):
		return self._missing

	@property
	def company_name(self):
		return self._name

	@property
	def symbol(self):
		return self._symbol

	@property
	def exchange(self):
		return self._exchange

	@property
	def de_listed(self):
		return self._de_listed

	@property
	def last_update(self):
		return self._end_date

	@property
	def first_date(self):
		return self.first_date

	def __str__(self):
		return self.symbol

