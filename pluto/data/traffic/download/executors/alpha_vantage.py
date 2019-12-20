import requests
from pandas import read_csv
from io import StringIO
from pluto.data.traffic.download.request import EquityRequest
from .executor import _RequestExecutor
from dateutil.parser import parse
from logbook import Logger

log = Logger('Alpha Vantage')

class _AlphaVantage(_RequestExecutor):
	api_url = "https://www.alphavantage.co/query?function={0}&{1}={2}&outputsize=full&apikey={3}&datatype=csv"

	def __init__(self, name, request_counter,api_key):
		super(_AlphaVantage, self).__init__(name,request_counter)
		self._api_key = api_key

	def _execute(self, request):
		if type(request) is EquityRequest:
			symbol = request.symbol.replace('.','-')
			if request.interval is '1D':
				func = 'TIME_SERIES_DAILY_ADJUSTED'
				url = self.api_url.format(func, 'symbol', symbol, self._api_key)
			else:
				return
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
					documents.append({'Date': parse(t[date]),
									  'Open': t[open_],
									  'High': t[high],
									  'Low': t[low],
									  'Close': t[close],
									  'Volume': t[volume],
									  'Dividend': t[dividend],
									  'Split': t[split]})
				return {'symbol': request.symbol, 'series': documents[::-1]}
			except (KeyError, Exception):
				return

	def _cool_down_time(self):
		return 1.1
