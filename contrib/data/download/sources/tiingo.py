from contrib.data.download.sources import source
import time

_HOUR = 3600
_DAY = 86400

_EXCHANGES = [
    'NYSE',
    'NASDAQ'
]

_DATA_TYPES = [
    'DayHistorical',
]

class DayHistory(source.Item):
    pass

class Tiingo(source.Source):
    def __init__(self):
        super(Tiingo, self).__init__()
        self._hour_requests = 0
        self._bandwidth = 0
        self._hour_start = 0
        self._day_requests = 0
        self._day_start = 0


    def _wait_time(self):
        hour_start = self._hour_start
        day_start = self._day_start
        if self._hour_requests == 0:
            hour_start = time.time()
        self._hour_requests += 1
        self._day_requests += 1

        hour_elapsed = time.time() - hour_start
        day_elapsed = time.time() - day_start

        day_requests = self._day_requests
        hour_requests = self._hour_requests

        if day_elapsed < _DAY:
            if day_requests <= 150000:
                if hour_elapsed <= _HOUR:
                    if hour_requests <= 20000:
                        return 0
                    else:
                        return _HOUR - hour_elapsed
                else:
                    self._hour_requests = 0
                    self._hour_start = time.time()
                    return 0
            else:
                return _DAY - day_elapsed
        else:
            self._day_requests = 0
            self._day_start = time.time()
            return 0

    def _execute(self, request, item):
        #todo: measure bandwidth
        pass



