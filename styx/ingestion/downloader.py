import requests

class PolygonClient(object):
	def __init__(self, settings) -> None:
		self._session = session = requests.Session()
		session.headers.update({
			'Authorization': settings['token']})
	
	def get_minute_bars(self, asset):
		pass

	def get_asset_list(self, state='any'):
		pass

	def get_splits(self):
		pass

	def get_dividends(self):
		pass

	def get_name_changes(self):
		pass

class AlpacaClient(object):
	def __init__(self, settings) -> None:
		self._session = session = requests.Session()
		session.auth(settings['key'], settings['secret'])
	
	def	get_corporate_actions(self):
		pass

class OpenFIGIClient(object):
	def __init__(self, settings) -> None:
		pass

	def name_changes(self, asset):
		pass

class Downloader(object):
	def __init__(self, settings) -> None:
		#directory for temporary storage
		# todo: pass-in credentials etc.
		self._polygon = PolygonClient(settings['polygon.io'])
		self._alpaca = AlpacaClient(settings['alpaca'])
	
	async def assets_list(self, ts, state='any'):
		return 
	
	async def name_changes(self, start_ts, end_ts, asset, cache):
		pass

	async def corporate_actions(self, start_ts, end_ts, asset, cache):
		pass

	async def minute_bars(self, equity_data, asset, cache):
		# asset symbol.
		data = self._polygon.get_minute_data(asset.symbo)
		if data.error:
			equity_data.set_error('minute_bars', data.error)
		cache.append(data)
	
	async def historical_minute_bars(self, equity_data, asset, cache):
		data = self._alpaca.get()
		if data.error:
			equity_data.set_error(data)
		cache.append(data)

	async def splits(self, start_ts, end_ts, asset, cache):
		return 

	async def dividends(self, start_ts, end_ts, asset, cache):
		return
	
	
	def __exit__(self):
		self._session.close()
