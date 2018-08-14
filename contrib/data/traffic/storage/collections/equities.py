from contrib.data.traffic.storage.queryable import DailyBars
from contrib.data.traffic.storage.database.mongo_utils import get_collection

from pandas import DataFrame

_DAILY_EQUITY_DATA = get_collection('equity data')

'''implementations... knows how to return the requests...'''
class MongoDailyBars(DailyBars):
	def _query_daily_bars(self,symbol):
		data = _DAILY_EQUITY_DATA.find_one({'Ticker':symbol})
		return DataFrame.from_records(data['Series'])

	def _update_bars(self,symbol:str,df:DataFrame):
		_DAILY_EQUITY_DATA.update_one({'Ticker':symbol},
									  {'$push':{'Series':{'$each':DataFrame.to_dict('records')}}})

	def _daily_equity_writer(self, data,meta_data,last_update):
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
