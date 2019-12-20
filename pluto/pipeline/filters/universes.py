from zipline.pipeline.filters.filter import CustomFilter
import numpy as np
from datetime import datetime, date, time
# import zipline.api as api
from zipline.errors import SymbolNotFound

''' A spy constituent filter returns each day's s and p constituent...'''

# finder = db.Assets()

def to_datetime(date_):
	dt = np.array([date_]).astype(datetime)[0]
	if isinstance(dt, date):
		dt = datetime.combine(dt, time.min)
	return dt


def get_spy_historical_symbols():
	pass
	# maps symbol to name of the company, this is a unique combination...
	# d = compress_dict([i for i in c])
	# finder = db.AssetFinder()
	# assets = []
	# for symbol, name in zip(d['Ticker'], d['Name']):
	# 	assets.extend(finder.find_asset(symbol, name))
	# return [s.symbol for s in assets]

# def compress_dict(dict_list):
# 	n_dict = {}
# 	for dct in dict_list:
# 		for key, item in dct.items():
# 			if key in n_dict:
# 				a = n_dict[key]
# 			else:
# 				a = []
# 				n_dict[key] = a
# 			a.append(item)
# 	return n_dict
def get_sids(today):
	sids = []
	for asset in finder.find_by_date(to_datetime(today),index_name='sp500'):
		symbol = asset.symbol
		try:
			sids.append(api.symbol(symbol).sid)
		except SymbolNotFound:
			pass
	return sids

class SP500Constituents(CustomFilter):
	inputs = []  # no inputs...
	window_length = 1

	'''each day, we load the sp500 constituents that are constituents in the same month...(we have month-end data...)'''

	'''we assume that data have been ingested...'''

	def compute(self, today, assets, out):
		out[:] = np.in1d(assets,get_sids(today))
