import abc

from pluto.utils import stream
from pluto.coms.utils import conversions

from protos import controllable_pb2 as cbl

#if any of the methods fail, it will raise an error.
class Process(object):
    __slots__ = ['_controllable', '_session_id']

    def __init__(self, session_id, framework_url):
        '''

        Parameters
        ----------
        session_id
        framework_url
        '''
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
        # send parameters to the controllable as a stream of bytes
        self._controllable.Initialize(stream.chunk_bytes(params))

    def parameter_update(self, params):
        self._controllable.UpdateParameters(params)

    def clock_update(self, clock_event):
        self._controllable.ClockUpdate(clock_event)

    def account_update(self, broker_state):
        self._controllable.UpdateAccount(broker_state)

    def stop(self):
        self._controllable.Stop()
        self._stop()

    @abc.abstractmethod
    def _update(self, event_writer):
        raise NotImplementedError

    @abc.abstractmethod
    def _stop(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _create_controllable(self, framework_url, session_id):
        raise NotImplementedError(self._create_controllable.__name__)
