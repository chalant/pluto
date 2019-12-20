from pluto.data.download.executors import alpha_vantage, wikipedia
from pluto.data.download.executors import tiingo
from pluto.data.download.executors import yahoo
from pluto.data.download.executors.executor import RequestsCounter

#counters
_YAHOO_COUNTER = RequestsCounter(100)
_WIKIPEDIA = None
_TIINGO_COUNTER = RequestsCounter(20000)
_ALPHA_VANTAGE_COUNTER = RequestsCounter(1)
_WIKI_COUNTER = RequestsCounter(1)


# TODO: retrieve api keys from environment... don't hardcode it here...
def order_executor(name, environ, api_key=None, full_access=False):
	global _WIKIPEDIA
	if name is 'AlphaVantage':
		return alpha_vantage._AlphaVantage(
			'alpha vantage',
			_ALPHA_VANTAGE_COUNTER,
			environ['ALPHA_VANTAGE'])

	elif name is 'Yahoo':
		return yahoo._Yahoo(
			'yahoo',
			_YAHOO_COUNTER)

	elif name is 'Wikipedia':
		# ONLY one instance of this...
		if not _WIKIPEDIA:
			return wikipedia.WikipediaExecutor(
				'wiki',
				_WIKI_COUNTER)
		else:
			return _WIKIPEDIA

	elif name is 'Tiingo':
		return tiingo._Tiingo(
			'tiingo',
			environ['TIINGO'],
			_TIINGO_COUNTER,
			paid_account=full_access)

	else:
		raise ValueError(
			'No executor named {}'.format(name))
