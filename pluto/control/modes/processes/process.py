import abc

from pluto.utils import stream
from pluto.coms.utils import conversions

from protos import controllable_pb2 as cbl

class Process(object):
    __slots__ = ['_controllable']
    def __init__(self, framework_url):
        self._controllable = self._create_controllable(framework_url)

    def initialize(self,
                   start,
                   end,
                   universe,
                   strategy,
                   capital,
                   max_leverage,
                   data_frequency,
                   look_back):
        start = conversions.to_proto_timestamp(start)
        end = conversions.to_proto_timestamp(end)
        params = cbl.InitParams(
            start=start,
            end=end,
            universe=universe,
            strategy=strategy,
            capital=capital,
            max_leverage=max_leverage,
            data_frequency=data_frequency,
            look_back=look_back
        ).SerializeToString()
        #send parameters to the controllable as a stream of bytes
        self._controllable.Initialize(stream.chunk_bytes(params))

    def parameter_update(self, capital, max_leverage):
        pass

    def clock_update(self, dt, evt, signals):
        #todo: this has priority
        pass

    def stop(self):
        self._controllable.Stop()
        self._stop()

    @abc.abstractmethod
    def _stop(self):
        pass

    @abc.abstractmethod
    def _create_controllable(self, framework_url):
        raise NotImplementedError(self._create_controllable.__name__)
