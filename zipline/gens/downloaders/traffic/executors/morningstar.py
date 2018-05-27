from zipline.gens.downloaders.traffic.executors.executor import _RequestExecutor
import requests
from pandas import DataFrame
import pandas as pd
from datetime import datetime


class _MorningStar(_RequestExecutor):
	url = "http://globalquote.morningstar.com/globalcomponent/RealtimeHistoricalStockData.ashx"

	def _url_params(self,request):
		#"f" is the frequency, here "d is day [d,w,m] day,week,month respectively
		p = {"range": "|".join(
			[request.start_date.strftime("%Y-%m-%d"), request.end_date.strftime("%Y-%m-%d")]),
			"f": 'd', "curry": "usd",
			"dtype": "his", "showVol": "true",
			"hasF": "true", "isD": "true", "isS": "true",
			"ProdCode": "DIRECT"}
		return p

	def _execute(self, request):
		symbol = request.symbol
		params = self._url_params(request)
		params.update({"ticker": request.symbol})

		try:
			resp = requests.get(self.url, params=params)
			if resp.status_code == requests.codes.ok:
				json_data = resp.json()
				if json_data is None:
					return None
				else:
					jsdata = self._restruct_json(symbol=symbol,
											 jsondata=json_data)


		except Exception:
			return None
		symbols_df = DataFrame(data=symbol_data)
		dfx = symbols_df.set_index(["Symbol", "Date"])
		return dfx


	def _restruct_json(self, symbol, jsondata):
		if jsondata is None:
			return
		divdata = jsondata["DividendData"]

		pricedata = jsondata["PriceDataList"][0]["Datapoints"]
		dateidx = jsondata["PriceDataList"][0]["DateIndexs"]
		volumes = jsondata["VolumeList"]["Datapoints"]

		dates = self._convert_index2date(indexvals=dateidx)
		barss = []
		for p in range(len(pricedata)):
			bar = pricedata[p]
			d = dates[p]
			bardict = {
				"Symbol": symbol, "Date": d, "Close": bar[0], "High": bar[1],
				"Low": bar[2], "Open": bar[3]
			}
			if len(divdata) == 0:
				pass
			else:
				events = []
				for x in divdata:
					delta = (datetime.strptime(x["Date"], "%Y-%m-%d")
							 - d.to_pydatetime())
					if delta.days == 0:
						events.append(x)
				for e in events:
					if e["Type"].find("Div") > -1:
						val = e["Desc"].replace(e["Type"], "")
						bardict.update({"isDividend": val})
					elif e["Type"].find("Split") > -1:
						val = e["Desc"].replace(e["Type"], "")
						bardict.update({"isSplit": val})
					else:
						pass
			bardict.update({"Volume": int(volumes[p] * 1000000)})
			barss.append(bardict)
		return barss

	@staticmethod
	def _convert_index2date(indexvals):
		base = pd.to_datetime('1900-1-1')
		return [base + pd.to_timedelta(iv, unit='d') for iv in indexvals]