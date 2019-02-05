from contrib.data.download.executors.executor import _RequestExecutor
from contrib.data.download.request import EquityRequest, MetaDataRequest
from datetime import datetime, date
import requests
from dateutil.parser import parse

'''{
      "date":"2014-06-04T00:00:00.000Z",
      "close":644.82,
      "high":647.89,
      "low":636.11,
      "open":637.44,
      "volume":11981500,
      "adjClose":88.2435131531,
      "adjHigh":88.6636421587,
      "adjLow":87.0515510558,
      "adjOpen":87.2335613416,
      "adjVolume":83870500,
      "divCash": 0.0,
      "splitFactor":1.0
   }
'''

#TODO: add a way to get the latest data...
class _Tiingo(_RequestExecutor):
    base_url = "https://api.tiingo.com/tiingo/"

    def __init__(self, name, api_key, requests_counter, paid_account=False):
        super(_Tiingo, self).__init__(name, requests_counter)
        self._paid_account = paid_account
        self._headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token {0}'.format(api_key)
        }

    def _execute(self, request):
        if isinstance(request, EquityRequest):
            symbol = request.symbol.replace('.', '-')
            # todo: we can also download minutely data...
            if request.interval == '1D':
                url = self.base_url + "daily/{0}/prices?startDate={1}&endDate={2}"
                url = url.format(symbol, self._reformat_datetime(request.start_date),
                                 self._reformat_datetime(request.end_date))
            else:
                return
            try:
                response = requests.get(url=url, headers=self._headers)
                if response.status_code == requests.codes.ok:
                    return {'symbol': request.symbol, 'series': self._format_equity_data_from_json(response.json())}
                else:
                    return
            except Exception:
                return
        elif isinstance(request, MetaDataRequest):
            symbol = request.symbol.replace('.', '-')
            url = self.base_url +'daily/{}'.format(symbol)
            try:
                response = requests.get(url=url, headers=self._headers)
                if response.status_code == requests.codes.ok:
                    return self._format_meta_data_from_json(response.json())
            except Exception:
                return None
        else:
            raise NotImplementedError

    def _cool_down_time(self):
        return 3600

    def _reformat_datetime(self, dt):
        frt = "{0}-{1}-{2}"
        if isinstance(dt, datetime):
            dte = dt.date()
        elif isinstance(dt, date):
            dte = dt
        else:
            raise TypeError("Expected: {0} got: {1}".format(datetime, type(dt)))
        return frt.format(dte.year, dte.month, dte.day)

    def _format_equity_data_from_json(self, json):
        return [{"Date": parse(j['date']).replace(tzinfo=None), "Open": j['open'], "High": j['high'],
                 "Low": j['low'], "Close": j['close'],
                 "Split": j['splitFactor'], "Dividend": j['divCash'], "Volume": j['volume']} for j in json]

    def _format_meta_data_from_json(self, json):
        return {"Name": json['name'], "StartDate": parse(json['startDate']), "EndDate": parse(json['endDate']),
                'Ticker': json['ticker'],
                'Exchange': json['exchangeCode'], "Description": json['description']}
