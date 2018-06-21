from concurrent.futures import ThreadPoolExecutor
from pandas import Timestamp
from zipline.gens.data.traffic import database as db

# this runs once a day...
if __name__ == '__main__':
	asset_finder = db.AssetFinder()
	with ThreadPoolExecutor() as pool:
		today = Timestamp.utcnow()
		daily_eq = db.DailyEquity(asset_finder)
		# daily_eq.download(pool, today, True, exchanges=['NYSE', 'NASDAQ'])
	# 	db.SP500Constituents().download(pool, today)
	# 	asset_finder.download(pool,Timestamp.utcnow(),True,exchanges=['NYSE','NASDAQ'])
	# asset_finder.update_available_data()
	daily_eq.ingest_data(Timestamp('2018,6,19',tz ='UTC'))
