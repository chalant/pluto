import abc

from google.protobuf import empty_pb2 as emp

from pluto.utils import stream
from pluto.coms.utils import conversions

from protos import controllable_pb2 as cbl

#if any of the methods fail, it will raise an error.
class ProcessFactory(object):
    def create_process(self, session_id, framework_url):
        return self._create_process(framework_url, session_id)

    @abc.abstractmethod
    def _create_process(self, framework_url, session_id):
        raise NotImplementedError


class Process(abc.ABC):
    __slots__ = ['_controllable', '_session_id']

    def __init__(self, framework_url, session_id):
        self._controllable = self._create_controllable(framework_url, session_id)
        self._session_id = session_id

    @property
    def session_id(self):
        return self._session_id

    def initialize(self,
                   start,
                   end,
                   universe,
                   strategy,
                   capital,
                   max_leverage,
                   data_frequency,
                   look_back,
                   mode):
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
            look_back=look_back,
            mode=mode
        ).SerializeToString()
        # send parameters to the controllable as a stream of bytes
        self._initialize(self._controllable, stream.chunk_bytes(params))

    def parameter_update(self, params):
        self._parameter_update(self._controllable, params)

    def clock_update(self, clock_event):
        self._clock_update(self._controllable, clock_event)

    def account_update(self, broker_state):
        self._account_update(self._controllable, broker_state)

    def stop(self):
        self._stop(self._controllable, emp.Empty())

    @abc.abstractmethod
    def _initialize(self, controllable, iterator):
        raise NotImplementedError

    @abc.abstractmethod
    def _parameter_update(self, controllable, params):
        raise NotImplementedError

    @abc.abstractmethod
    def _clock_update(self, controllable, clock_event):
        raise NotImplementedError

    @abc.abstractmethod
    def _account_update(self, controllable, iterator):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_controllable(self, framework_url, session_id):
        raise NotImplementedError

    @abc.abstractmethod
    def _stop(self, controllable, param):
        raise NotImplementedError