from concurrent.futures import ThreadPoolExecutor

from pandas import Timestamp

from pluto.data.traffic.download import downloaders as dl
from pluto.data.traffic.storage import database as cl

def get_downloader(downloadable,asset_group):
	return downloadable.get_downloader(asset_group)

def download(downloader,thread_pool,today,show_progress=True):
	downloader.download(thread_pool,today,show_progress)

if __name__ == '__main__':
	downloadables  = []
	assets = cl.Assets()
	#TODO: internally, the downloadables know which elements to download...
	#for instance, if we previously downloaded nasdaq data, and then we decide to download
	#a symbol which happens to be in the nasdaq,it won't download it (if the dates match...)
	#...and vice-versa...
	nyse_nasdaq = dl.EquityExchange(assets, ['NYSE', 'NASDAQ'])
	downloadables.extend([assets, cl.DailyEquity(assets), cl.SP500Constituents()])
	with ThreadPoolExecutor() as pool:
		today = Timestamp.utcnow()
		for downloadable in downloadables:
			download(get_downloader(downloadable,nyse_nasdaq),pool,today)

	# 	asset_finder.download(pool,Timestamp.utcnow(),True,exchanges=['NYSE','NASDAQ'])
	# asset_finder.update_available_data()
	# daily_eq.ingest_data(Timestamp('2018,6,19',tz ='UTC')) #ingest data starting from
	#last successfull download...
