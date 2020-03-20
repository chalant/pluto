from pluto.data.storage.database import get_collection

_META_DATA = get_collection('AvailableTickers')
_TICKERS = get_collection('Tickers')

'''keeps track of what to download and maintain... and the symbols in general...'''
class MetaData(object):
	def sid(self,symbol):
		'''if there are no sids, generate sids for each symbol'''
		pass


