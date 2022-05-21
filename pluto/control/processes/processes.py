import abc
import hashlib

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


# todo: add logging for all process failures
# todo: should there be a limit for recovery attempts?
# todo: recovery must not block


class Process(object):
    __slots__ = ['_process_id', '_controllable']

    def __init__(self, process_id, controllable):
        self._process_id = process_id
        self._controllable = controllable

    @property
    def process_id(self):
        return self._process_id

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
        # self._stop()

    def watch(self, watcher):
        # subscribe the watcher...
        self._controllable.monitor.watch(watcher)

        # make a watch request...
        self._controllable.Watch(emp.Empty(), ())

    def stop_watching(self):
        self._controllable.StopWatching(emp.Empty(), ())

    def __hash__(self):
        return self._process_id


class ProcessInitializer(object):
    def __init__(self, controllable):
        # self._execute_events = execute_events

        self._controllable = controllable

    def initialize(self, setup):
        # send parameters to the controllable as a stream of bytes
        return self._controllable.Initialize(
            cbl.RunSetup(
                strategy_id=setup.strategy_id),
            ())

    def start(self, setup):

        session_id = setup.session_id

        process_id = hashlib.md5(str(self._strategy_id + session_id).encode()).hexdigest()

        self._controllable.Run(
            cbl.RunRequest(
                id=process_id,
                session_id=session_id,
                capital=setup.capital,
                leverage=setup.leverage,
                start=conversions.to_proto_timestamp(setup.start),
                end=conversions.to_proto_timestamp(setup.end),
                commission_models=cbl.CommissionModels(
                    equity=setup.equity_commission_model,
                    futures=setup.futures_commission_model),
                tax_models=cbl.TaxModels(
                    equity=setup.equity_tax_model,
                    futures=setup.futures_tax_model),
                universe_name=setup.trading_universe,
                clear_perf=setup.clear_perf),
            ())

        return Process(process_id, self._controllable)

    # def recover(self):
    #     self._controllable = self._create_controllable(
    #         _framework_id,
    #         self._framework_url,
    #         self._session_id,
    #         self._root_dir,
    #         True,
    #         self._execute_events)

    # @abc.abstractmethod
    # def _create_controllable(self,
    #                          framework_id,
    #                          framework_url,
    #                          session_id,
    #                          root_dir,
    #                          recover=False,
    #                          execute_events=False):
    #     raise NotImplementedError
    #
    # @abc.abstractmethod
    # def _stop(self):
    #     raise NotImplementedError
