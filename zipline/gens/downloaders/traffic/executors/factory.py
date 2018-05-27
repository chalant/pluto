from zipline.gens.downloaders.traffic.executors import alpha_vantage, yahoo, wikipedia, tiingo

_alv = None
_yho = None
_wiki = None
_tii = None


def order_executor(name, api_key=None, full_access=False):
	global _alv, _yho, _wiki
	if name is 'AlphaVantage':
		if not _alv:
			if api_key is None:
				_alv = alpha_vantage._AlphaVantage('al', "5B3LVTJKR827Y06N")
			else:
				_alv = alpha_vantage._AlphaVantage('al', api_key)
		return _alv
	elif name is 'Yahoo':
		if not _yho:
			_yho = yahoo._Yahoo('yho')
		return _yho
	elif name is 'Wikipedia':
		if not _wiki:
			return wikipedia.WikipediaExecutor('wiki')
		else:
			return _wiki
	elif name is 'Tiingo':
		if not api_key:
			raise ValueError('Must provide a token')
		else:
			if not _tii:
				return tiingo._Tiingo('tii', api_key=api_key, paid_account=full_access)
			else:
				return _tii
	else:
		raise NotImplementedError
