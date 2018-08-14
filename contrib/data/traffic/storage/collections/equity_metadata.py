from contrib.data.traffic.storage.database.mongo_utils import get_collection

_META_DATA = get_collection('AvailableTickers')
_TICKERS = get_collection('Tickers')

'''keeps track of what to download and maintain... and the symbols in general...'''
class MetaData(object):
	def __init__(self):
		self._observers = []

	def sid(self,symbol):
		'''if there is no sids, generate sids for each symbol'''
		pass

	def update(self,symbol,**kwargs):
		pass

	def reset(self,symbol):
		'''the ingester might need to reset'''
		'''resets data...'''
		for observer in self._observers:
			observer.reset(symbol)

	def subscribe(self,equity_collection):
		self._observers.append(equity_collection)


