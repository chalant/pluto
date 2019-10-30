import queue
from protos import clock_pb2


class Downloader(object):
    def __init__(self, metadata_database, sources):
        self._metadata_db = metadata_database

        self._consumers = []
        self._signal_handlers = []
        self._queues = queues = {}

        self._sources = sources

        for source in sources:
            for resource in source.resources:
                q = queues.get(resource, None)
                if not q:
                    self._queues[resource] = q = queue.Queue()
                source.add_queue(q)

    def clock_update(self, clock_evt):
        # todo: we assume that we already have initial data up until the end of the previous minute
        # if we don't, we need to download all the historical data up until the end of the previous
        # minute... PROBLEM: not many sources for minutely historical data at best they go back
        # a month => download as much as possible given the resources
        # todo: what if we don't have historical data? => we need to download all historical data...
        for signal_handler in self._signal_handlers:
            signal_handler.clock_update(clock_evt)

        # todo: we download quote data every minute... => this is specified by the item we want
        #
        # todo: what about historical data?

        #todo: assuming we have the latest historical data, only quotes are downloaded
        # at each end of minute...

        if clock_evt.event == clock_pb2.MINUTE_END:
            #todo: download minutely occuring data such as quotes
            self._download(clock_evt.dt, clock_evt.exchange, self._consumers)
        elif clock_evt.event == clock_pb2.SESSION_END:
            #todo: download daily recurring data, such as earnings calendars etc.
            self._download(clock_evt.dt, clock_evt.exchange, self._consumers)

    def add_clock_signal_handler(self, handler):
        self._signal_handlers.append(handler)

    def add_data_consumer(self, consumer):
        self._consumers.append(consumer)

    def _download(self, timestamp, exchange, consumers):
        requests = self._metadata_db.create_requests(timestamp, exchange)
        queues = self._queues
        for request in requests:
            q = queues.get(request)
            q.put(request)

        for source in self._sources:
            source.start(consumers)

        for queue in queues.values():
            queue.join()
