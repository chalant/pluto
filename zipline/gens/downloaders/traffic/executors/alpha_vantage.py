import requests
from pandas import read_csv
from io import StringIO
from zipline.gens.downloaders.traffic.requests import EquityRequest
from zipline.gens.downloaders.traffic.executors.executor import _RequestExecutor
from dateutil.parser import parse


class _AlphaVantage(_RequestExecutor):
	api_url = "https://www.alphavantage.co/query?function={0}&{1}={2}&outputsize=full&apikey={3}&datatype=csv"

	def __init__(self, name, api_key):
		super(_AlphaVantage, self).__init__(name)
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
					documents.append({'Date': parse(t[date]),
									  'Open': t[open_],
									  'High': t[high],
									  'Low': t[low],
									  'Close': t[close],
									  'Volume': t[volume],
									  'Dividend': t[dividend],
									  'Split': t[split]})
				print('documents',documents)
				return {'symbol': request.symbol, 'series': documents}
			except (KeyError, Exception) as e:
				print('unable to download data for {0}'.format(request.symbol), e)
				return None

	def _cool_down_time(self):
		return 1.1
