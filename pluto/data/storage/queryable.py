from abc import ABC,abstractmethod

from pandas import DataFrame,DatetimeIndex

from trading_calendars import TradingCalendar

'''abstractions for queryables...'''

BARS = frozenset(['date', 'open', 'high', 'low', 'close', 'volume', 'dividend', 'split'])

class Bars(ABC):
	'''returns a dataframe of ohclvds of a symbol...'''
	def get(self, symbol:str, calendar:TradingCalendar) -> DataFrame:
		if not isinstance(calendar,TradingCalendar):
			raise TypeError('Expected {} got {}'.format(TradingCalendar,type(calendar)))
		return self._ensure_sampling(self._ensure_format(self._query_bars(symbol)),calendar)


	def update_bars(self,symbol:str,df:DataFrame,calendar:TradingCalendar) -> None:
		df = self._ensure_format(df)
		df = self._ensure_sampling(df,calendar)
		self._update_bars(symbol,df)

	@abstractmethod
	def _update_bars(self,symbol:str,df:DataFrame) -> None:
		raise NotImplementedError

	@abstractmethod
	def _query_bars(self, symbol: str) -> DataFrame:
		'''returns a dataframe that must contain specific columns...'''
		raise NotImplementedError

	def _ensure_format(self, df):
		if not isinstance(df, DataFrame):
			raise TypeError('Expected {} got {}'.format(DataFrame, type(df)))
		cols = df.columns
		bars = BARS
		if not bars.issubset(cols) and not set(cols).issubset(bars):
			raise ValueError('DataFrame object must contain {} as columns'.format(list(bars)))
		return df[['open','high','low','close','volume','dividend','split']]

	@abstractmethod
	def _ensure_sampling(self,df:DataFrame,calendar:TradingCalendar):
		raise NotImplementedError

	@abstractmethod
	def get_request(self,symbol,metadata,start_date,end_date,last_update):
		'''input: metadata '''
		pass

class DailyBars(Bars):
	def _query_bars(self, symbol: str):
		self._query_daily_bars(symbol)

	@abstractmethod
	def _query_daily_bars(self,symbol:str) -> DataFrame:
		raise NotImplementedError

	def _ensure_sampling(self,df:DataFrame,calendar:TradingCalendar):
		idx = DatetimeIndex(df['date'])
		range = calendar.sessions_in_range(idx[0], idx[-1])
		assert len(range) == len(idx)
		# TODO: explain why the it failed...
		return df

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

class Constituents(ABC):
	'''returns set of all symbols at a particular datetime, could be an exchange or
	an index'''
	@abstractmethod
	def get_constituents(self,datetime,index=None,exchange=None):
		raise NotImplementedError






