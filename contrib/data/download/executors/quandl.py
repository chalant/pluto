from contrib.data.download import _RequestExecutor
from contrib.data.download import EquityRequest
from datetime import datetime,date
import math
import quandl

class _Quandl(_RequestExecutor):
	def __init__(self, api_key, requests_counter, name=None):
		super(_Quandl,self).__init__(name, requests_counter)
		quandl.ApiConfig.api_key = api_key

	def _format_date(self,dt):
		if isinstance(dt,datetime):
			return dt.date().isoformat()
		elif isinstance(dt,date):
			return dt.isoformat()

	def _execute(self, request):
		'''ticker,date,open,high,low,close,volume,dividends,closeunadj,lastupdated
		AAPL,2008-01-02,28.467,28.609,27.507,27.834,269794700.0,0.0,194.84,2017-11-02'''
		if isinstance(request,EquityRequest):
			start = request.start_date
			end = request.end_date
			delta = end - start
			if delta.days >= 2: #compute split...
				pass
				#pandas dataframe
				data = quandl.get_table('SHARADAR/SEP',date={'gte':self._format_date(request.start_date),
													'lte':self._format_date(request.end_date)}, ticker=request.symbol,
									paginate=True)
			else:
				return None

	def _extract_split(self,p, c):
		if c != 0:
			s = p / c
		else:
			s = 1.0
		if s < 1:
			if p != 0:
				n = c / p
			else:
				n = 1.0
		else:
			n = s
		try:
			ceil = float(math.ceil(n))
			floor = float(math.floor(n))
			d1 = ceil - n
			d2 = n - floor
			if d1 <= d2:
				if s > 1.0:
					return ceil
				elif s < 1.0:
					return 1 / ceil
				else:
					return 1.0
			else:
				if s > 1.0:
					return floor
				elif s < 1.0:
					return 1 / floor
				else:
					return 1.0
		except ValueError:
			return math.nan

