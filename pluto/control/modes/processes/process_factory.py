import abc

from grpc import RpcError
from google.protobuf import empty_pb2 as emp

from pluto.coms.utils import conversions
from pluto.interface.utils import paths
from pluto.interface.utils.service_access import _framework_id

from protos import controllable_pb2 as cbl

class ProcessFactory(abc.ABC):
    def create_process(self, session_id, framework_url):
        return self._create_process(
            framework_url,
            session_id,
            paths.root())

    @abc.abstractmethod
    def _create_process(self, framework_url, session_id, root_dir):
        raise NotImplementedError

    def set_monitor_service(self, monitor_service):
        pass

    def set_broker_service(self, broker_service):
        pass


class ProcessWrapper(abc.ABC):
    def __init__(self, process):
        '''

        Parameters
        ----------
        process: Process
        '''
        self._process = process

    @property
    def session_id(self):
        return self._process.session_id

    def initialize(self, start, end, capital, max_leverage, mode):
        self._initialize(start, end, capital, max_leverage, mode)
        self._process.initialize(start, end, capital, max_leverage, mode)

    def parameter_update(self, params):
        self._process.parameter_update(params)

    def clock_update(self, clock_event):
        self._clock_update(clock_event)
        self._process.clock_update(clock_event)

    def account_update(self, broker_state):
        self._process.account_update(broker_state)

    def stop(self):
        self._process.stop()

    def watch(self):
        self._process.watch()

    def stop_watching(self):
        self._process.stop()

    @abc.abstractmethod
    def _initialize(self, start, end, capital, max_leverage, mode):
        raise NotImplementedError

    @abc.abstractmethod
    def _clock_update(self, clock_event):
        raise NotImplementedError

#todo: add loggin for all process failures
#todo: should there be a limit for recovery attempts?
#todo: recovery must not block
class Process(abc.ABC):
    __slots__ = ['_controllable', '_session_id']

    def __init__(self,
                 framework_url,
                 session_id,
                 root_dir,
                 execute_events=False):

        self._framework_url = framework_url
        self._root_dir = root_dir
        self._session_id = session_id
        self._execute_events = execute_events

        self._controllable = self._create_controllable(
            _framework_id,
            framework_url,
            session_id,
            root_dir)

    @property
    def session_id(self):
        return self._session_id

    def initialize(self, start, end, capital, max_leverage, mode):
        # send parameters to the controllable as a stream of bytes
        return self._controllable.Initialize(
            cbl.InitParams(
                id=self._session_id,
                start=conversions.to_proto_timestamp(start),
                end=conversions.to_proto_timestamp(end),
                capital=capital,
                max_leverage=max_leverage,
                mode=mode), ())

    def parameter_update(self, params):
        return self._controllable.ParameterUpdate(params, ())

    def clock_update(self, clock_event):
        return self._controllable.ClockUpdate(
            clock_event, ())

    def account_update(self, broker_state):
        return self._controllable.UpdateAccount(
            broker_state, ())

    def stop(self):
        # upon receiving the stop message, the controllable will
        # perform the necessary steps (liquidate positions, update its metrics
        # with data from broker, send back performance metrics). Once all that is
        # done, it will "unblock", then the process will be shutdown.
        # note: the call will be released when all the orders have been processed
        # this means that the process will still receive broker updates
        self._controllable.Stop(emp.Empty, ())
        self._stop()

    def watch(self):
        return self._controllable.Watch(emp.Empty(), ())

    def stop_watching(self):
        return self._controllable.StopWatching(emp.Empty(), ())

    def recover(self):
        self._controllable = self._create_controllable(
            _framework_id,
            self._framework_url,
            self._session_id,
            self._root_dir,
            True,
            self._execute_events)

    @abc.abstractmethod
    def _create_controllable(self,
                             framework_id,
                             framework_url,
                             session_id,
                             root_dir,
                             recover=False,
                             execute_events=False):
        raise NotImplementedError

    @abc.abstractmethod
    def _stop(self):
        raise NotImplementedError

