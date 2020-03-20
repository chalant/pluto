import abc

from google.protobuf import empty_pb2 as emp

from pluto.utils import stream
from pluto.coms.utils import conversions
from pluto.interface.utils import paths
from pluto.interface.utils.method_access import invoke, _framework_id

from protos import controllable_pb2 as cbl

#if any of the methods fail, it will raise an error.
class ProcessFactory(object):
    def create_process(self, session_id, framework_url):
        return self._create_process(framework_url, session_id, paths.root())

    @abc.abstractmethod
    def _create_process(self, framework_url, session_id, root_dir):
        raise NotImplementedError

    def set_monitor_service(self, monitor_service):
        pass


class Process(abc.ABC):
    __slots__ = ['_controllable', '_session_id']

    def __init__(self, framework_url, session_id, root_dir):
        self._controllable = self._create_controllable(
            _framework_id,
            framework_url,
            session_id,
            root_dir)
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
            id=self._session_id,
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
        return invoke(self._controllable.Initialize, stream.chunk_bytes(params))

    def parameter_update(self, params):
        return invoke(self._controllable.ParameterUpdate, params)

    def clock_update(self, clock_event):
        return invoke(self._controllable.ClockUpdate, clock_event)

    def account_update(self, broker_state):
        return invoke(self._controllable.UpdateAccount, broker_state)

    def stop(self):
        invoke(self._controllable.Stop, emp.Empty())
        self._stop()

    def watch(self):
        return invoke(self._controllable.Watch, emp.Empty())

    def stop_watching(self):
        return invoke(self._controllable.StopWatching, emp.Empty())

    @abc.abstractmethod
    def _create_controllable(self, framework_id, framework_url, session_id, root_dir):
        raise NotImplementedError

    @abc.abstractmethod
    def _stop(self):
        raise NotImplementedError