import threading
from concurrent import futures
import queue
import abc

from os import path

import grpc

import click

from google.protobuf import empty_pb2 as emp

from directory import directory as drt
from directory import utils as du

from pluto.interface.utils import paths, service_access

from pluto.coms.utils import conversions

from pluto.control.events_log import events_log
from pluto.control.controllable import commands
from pluto.control.controllable import state_storage as st
from pluto.control.controllable.utils import factory
from pluto.control.controllable import performance_logging as pl

from pluto.server import server, service

from pluto.strategies import setup as stg_stp

from pluto.setup import engine_setup as m_stp
from pluto.setup import directory_setup as drs

from protos import broker_pb2
from protos import controllable_pb2
from protos import controllable_pb2_grpc as cbl_rpc
from protos import interface_pb2_grpc as itf_rpc
from protos import interface_pb2 as itf

from protos.clock_pb2 import (BAR, TRADE_END)


class FrequencyFilter(abc.ABC):
    @abc.abstractmethod
    def filter(self, evt_exc_pairs):
        raise NotImplementedError


class DayFilter(FrequencyFilter):
    def filter(self, evt_exc_pairs):
        exchanges = []
        for evt, exc in evt_exc_pairs:
            if evt == TRADE_END:
                exchanges.append(exc)
        return exchanges


class MinuteFilter(FrequencyFilter):
    def filter(self, evt_exc_pairs):
        exchanges = []
        for evt, exc in evt_exc_pairs:
            if evt == TRADE_END or evt == BAR:
                exchanges.append(exc)
        return exchanges


class _ServiceState(abc.ABC):
    __slots__ = ['_service', '_controllable']

    def __init__(self, svc, controllable):
        self._service = svc
        self._controllable = controllable

    def execute(self, command):
        self._execute(self._service, command, self._controllable)

    @abc.abstractmethod
    def _execute(self, svc, command, controllable):
        raise NotImplementedError


class _Recovering(_ServiceState):
    def _execute(self, svc, command, controllable):
        # compare
        if command.dt <= controllable.real_dt:
            # don't do anything if the signal has "expired" this might happen
            # if the service receives signals while restoring state and it means
            # that the event has already been processed
            pass
        else:
            # set state to running since we're synchronized
            svc.state = svc.ready
            # set controllable run state to ready
            controllable.run_state = controllable.ready
            # execute command
            command()


class _Ready(_ServiceState):
    def _execute(self, svc, command, controllable):
        command()


class ControllableService(cbl_rpc.ControllableServicer):
    def __init__(self,
                 directory,
                 monitor,
                 controllable_factory,
                 mode):
        """

        Parameters
        ----------
        directory: directory.directory.Directory
        monitor
        controllable_factory
        """

        self._directory = directory

        self._log_performance = False
        self._perf_writer = None
        self._stop = False

        self._frequency_filter = None

        # used for queueing commands
        self._queue = queue.Queue()
        self._thread = None

        self._controllable = cbl = None

        self._recover = False

        self._ready = _Ready(self, cbl)
        self._recovery = recovery = _Recovering(self, cbl)
        self._state = recovery

        self._strategy_path = None

        self._store_state = False
        self._state_storage = st.NoStateStorage()

        self._strategies_dir = stg_dir = directory.get_directory('strategies')
        self._states_dir = stg_dir.get_directory('states')
        self._strategy_performance_dir = stg_dir.get_directory('performance')

        self._thread_pool = futures.ThreadPoolExecutor(5)

        self._monitor = monitor

        self._cbl_fty = controllable_factory

        self._strategy_setup = None

        self._mode = mode

    @property
    def log_performance(self):
        return self._log_performance

    @log_performance.setter
    def log_performance(self, value):
        self._log_performance = value

    @property
    def recover(self):
        return self._recover

    @recover.setter
    def recover(self, value):
        self._recover = value

    @property
    def store_state(self):
        return self._store_state

    @store_state.setter
    def store_state(self, value):
        self._store_state = value

    @property
    def frequency_filter(self):
        return self._frequency_filter

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value

    def ready(self):
        return self._ready

    def recovering(self):
        return self._recovery

    # todo need to use an interceptor to check for tokens etc.
    def stop(self):
        pass

    @service_access.framework_only
    def Initialize(self, request, context):
        id_ = request.strategy_id  # the id of the controllable => will be used in performance updates

        self._strategy_setup = strategy_setup = stg_stp.StrategySetup(id_)

        mode = self._mode

        controllable = self._cbl_fty.get_controllable(factory.ControllableParams(
            mode,
            id_))

        if controllable:
            self._controllable = controllable

            with open(self._strategies_dir.get_file(id_, 'py').path, 'r') as f:
                controllable.initialize(
                    strategy_setup,
                    f.read(),
                    mode)

            data_frequency = strategy_setup.data_frequency

            if data_frequency == 'daily':
                self._frequency_filter = DayFilter()
            elif data_frequency == 'minute':
                self._frequency_filter = MinuteFilter()

            # run the thread
            self._state = self._ready

        else:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Mode {} doesn't exist".format(mode))

        return controllable_pb2.InitializeResponse(
            data_frequency=strategy_setup.data_frequency,
            shorting=strategy_setup.shorting,
            asset_classes=strategy_setup.asset_classes,
            fractional=strategy_setup.fractional
        )

    @service_access.framework_only
    def Run(self, request, context):
        id_ = request.id  # the id of the controllable => will be used in performance updates

        engine_setup = m_stp.load_engine_setup(
            self._directory,
            request.universe_name,
            request.capital,
            request.engine_setup
        )

        mode = self._mode

        # todo: we should have a directory for storing performance

        # activate state storage if we're in live mode
        state_path = self._strategies_dir.add_file(id_, 'bin').path

        if self._recover:
            self._start_recovery(id_, state_path)

        if self._store_state:

            # clear file state file...
            with open(state_path, 'wb') as f:
                f.truncate(0)

            self._state_storage = st.StateStorage(
                state_path,
                self._thread_pool)

        perf_path = self._strategy_performance_dir.add_file(id_, 'bin').path

        if self._log_performance:
            self._perf_writer = pl.PerformanceWriter(
                perf_path,
                self._thread_pool,
                id_
            )
        else:
            self._perf_writer = pl.NullPerformanceWriter(
                self._monitor,
                id_
            )

        if request.clear_perf:
            # clear file if we're in simulation mode
            with open(perf_path, 'wb') as f:
                f.truncate(0)

        controllable = self._controllable

        if controllable:
            controllable.run(
                id_,
                self._strategy_setup,
                engine_setup,
                conversions.to_datetime(request.start),
                conversions.to_datetime(request.end))

            # run the thread
            self._state = self._ready

            self._thread = thread = threading.Thread(target=self._run)
            thread.start()
        else:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("Strategy was not initialized".format(mode))

        return emp.Empty()

    @service_access.framework_only
    def Join(self, request, context):
        # blocks until thread finishes
        if self._thread:
            self._thread.join()

    def _run(self):
        q = self._queue
        while not self._stop:
            try:
                self._state.execute(q.get())
            except commands.StopExecution:
                break

    def _load_state(self, session_id, state_path):
        with open(state_path, 'rb') as f:
            params = f.read()
            state = controllable_pb2.ControllableState()
            state.ParseFromString(params)
            return state

    def _start_recovery(self, process_id, state_path):
        raise NotImplementedError('Recovery')

        # 1)create controllable,
        # 2)call restore state on it => this will restore its session_state
        # 4)load events from the events log and execute them in order
        session_id = process_id

        state = self._load_state(session_id, state_path)

        self._controllable = controllable = \
            self._cbl_fty.get_controllable(state.mode)

        with open('strategy', self._states_dir) as f:
            strategy = f.read()

        controllable.restore_state(state, strategy)
        # set run_state to recovering
        controllable.run_state = controllable.recovering
        log = events_log.get_events_log(state.mode)
        events = log.read(controllable.current_dt)
        # play all missed events since last checkpoint (controllable is in recovery mode)
        perf_writer = self._perf_writer
        frequency_filter = self._frequency_filter
        state_storage = self._state_storage

        for evt_type, evt in events:
            if evt_type == 'clock':
                commands.ClockUpdate(
                    perf_writer,
                    controllable,
                    frequency_filter,
                    evt,
                    state_storage)()
            elif evt_type == 'parameter':
                for request in evt.requests:
                    if request.session_id == session_id:
                        commands.CapitalUpdate(
                            controllable,
                            evt)()
                        break
            elif evt_type == 'broker':
                commands.AccountUpdate(
                    controllable,
                    evt)()
            elif evt_type == 'stop':
                for request in evt.requests:
                    if request.session_id == session_id:
                        commands.Stop(
                            controllable,
                            evt)()
                        break

        # set run state to transitioning
        controllable.run_state = controllable.transitioning

        # create and start a thread to execute queued events
        self._thread = thread = threading.Thread(target=self._run)
        thread.start()

        # todo: store the last update request and compare it to the current
        # request, if it is identical, ignore it. (This might happen if the
        # controller is recovering from failure)

    @service_access.framework_only
    def Stop(self, request, context):
        # todo needs to liquidate positions and wipe the state.
        # multiple steps to execute: place orders, wait for all of them
        # to be executed, update performances, then return
        self._stop = True
        return emp.Empty()

    @service_access.framework_only
    def UpdateParameters(self, request, context):
        self._queue.put(
            commands.CapitalUpdate(
                self._controllable,
                request
            )
        )
        return emp.Empty()

    @service_access.framework_only
    def UpdateAccount(self, request_iterator, context):
        self._queue.put(
            commands.AccountUpdate(
                self._controllable,
                self._load_broker_state(
                    request_iterator)
            )
        )
        return emp.Empty()

    def _load_broker_state(self, request_iterator):
        b = b''
        for chunk in request_iterator:
            b += chunk.data
        brk_state = broker_pb2.BrokerState()
        brk_state.ParseFromString(b)
        return brk_state

    @service_access.framework_only
    def ClockUpdate(self, request, context):
        '''Note: an update call might arrive while the step is executing..., so
        we must queue the update message... => the step must be a thread that pulls data
        from the queue...
        '''

        self._queue.put(
            commands.ClockUpdate(
                self._perf_writer,
                self._controllable,
                self._frequency_filter,
                request,
                self._state_storage
            )
        )
        return emp.Empty()

    # @service_access.framework_only
    def Watch(self, request, context):
        self._perf_writer.observe(pl.Observer(self._monitor))

    # @service_access.framework_only
    def StopWatching(self, request, context):
        self._perf_writer.stop_observing()


@click.group()
def cli():
    pass

# todo: when we launch the server, we can fetch parameters from some
#  file...


@cli.command()
@click.argument('framework_id')
@click.argument('framework_url')
@click.argument('mode')
@click.option('-d', '--root-dir', default=path.expanduser("~/.pluto"))
@click.option('-cu', '--controllable-url')
@click.option('-lp', '--log-performance')
@click.option('-ss', '--store-state', is_flag=True)
@click.option('-re', '--recovery', is_flag=True)
@click.option('-ee', '--execute-events', is_flag=True)
def start(
        framework_id,
        framework_url,
        mode,
        root_dir,
        controllable_url,
        log_performance,
        store_state,
        recovery,
        execute_events):

    # todo: load parameters from setup files
    #  framework id, framework url etc.
    #  note: the framework url must be generated on the fly
    #  so we pass it as argument.

    """

    Parameters
    ----------
    framework_id
    framework_url : str
        url for callbacks
    mode
    root_dir
    controllable_url: str
    log_performance
    store_state
    recovery
    execute_events
    """

    # If the controllable fails, it will be relaunched by the controller.

    # TODO: save the framework_url for future use. NOTE: the framework url must be immutable
    # (is a service in kubernetes)

    # run forever or until an exception occurs, in which case, send back a report to the controller
    # or write to a log file. If the strategy crashes internally, there might be some bug that
    # need reviewing

    # set the framework_id if ran as a process
    service_access._framework_id = framework_id
    channel = grpc.insecure_channel(framework_url)

    controllable_service = ControllableService(
        du.DirectoryInterface(drs.create_directory(root_dir)),
        itf_rpc.MonitorStub(channel),
        factory.RemoteControllableFactory(channel))

    controllable_service.store_state = store_state
    controllable_service.recover = recovery
    controllable_service.log_performance = log_performance

    try:
        svr = server.get_server(controllable_service)
        svr.serve(
            controllable_service,
            controllable_url)
    except Exception as e:
        # todo: write to log?, send report to controller?
        raise RuntimeError('Unexpected error', e)


if __name__ == '__main__':
    cli()
