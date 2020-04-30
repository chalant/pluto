import threading
import signal
from concurrent import futures
import queue
import abc
from functools import partial

import grpc
import click
from google.protobuf import empty_pb2 as emp

from pluto.interface.utils import paths, method_access
from pluto.interface import directory
from pluto.coms.utils import conversions
from pluto.control.controllable import commands
from pluto.control.controllable import simulation_controllable as sc
from pluto.control.events_log import events_log
from pluto.control.controllable.utils import io
from pluto.control.controllable.utils import factory

from protos import controllable_pb2
from protos import controllable_pb2_grpc as cbl_rpc
from protos import interface_pb2_grpc as itf_rpc
from protos import interface_pb2 as itf
from protos.clock_pb2 import (
    BAR,
    TRADE_END)


def get_controllable(mode):
    '''

    Parameters
    ----------
    mode: str

    Returns
    -------
    pluto.control.controllable.controllable.Controllable
    '''
    if mode == 'simulation':
        return sc.SimulationControllable()
    elif mode == 'live':
        # todo: create controllable with broker stub, using the framework url
        raise NotImplementedError('live controllable')
    else:
        return


class _NoneObserver(object):
    def clear(self):
        pass

    def update(self, performance, end):
        pass


class _Observer(object):
    def __init__(self, monitor_stub, file_path, session_id):
        self._stub = monitor_stub
        self._reload = True
        self._file_path = file_path
        self._session_id = session_id

    def clear(self):
        self._reload = True

    def update(self, performance, end):
        stub = self._stub
        session_id = self._session_id
        if self._reload == True:
            for packet in io.read_perf(self._file_path):
                method_access.invoke(
                    stub.PerformanceUpdate,
                    itf.Packet(
                        packet=packet,
                        session_id=session_id))
            self._reload = False
        method_access.invoke(
            stub.PerformanceUpdate,
            itf.Packet(
                packet=performance,
                session_id=session_id,
                end=end))


class _StateStorage(object):
    def __init__(self, storage_path, thread_pool):
        '''

        Parameters
        ----------
        storage_path: str
        thread_pool: concurrent.futures.ThreadPoolExecutor
        '''
        self._storage_path = storage_path
        self._thread_pool = thread_pool

    def _write(self, state):
        # todo: should we append instead of over-writing?
        with(self._storage_path, 'wb') as f:
            f.write(state)

    def store(self, dt, controllable):
        self._thread_pool.submit(partial(
            self._write,
            state=controllable.get_state(dt)))

    def load_state(self):
        with open(self._storage_path, 'rb') as f:
            return f.read()


class _NoStateStorage(object):
    def store(self, dt, controllable):
        pass


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

    def __init__(self, service, controllable):
        self._service = service
        self._controllable = controllable

    def execute(self, command):
        self._execute(self._service, command, self._controllable)

    @abc.abstractmethod
    def _execute(self, service, command, controllable):
        raise NotImplementedError


class _Recovering(_ServiceState):
    def _execute(self, service, command, controllable):
        if command.dt <= controllable.current_dt:
            # don't do anything if the signal has "expired" this might happen
            # if the service receives signals while restoring state...
            pass
        else:
            # set state to running since we're synchronized
            service.state = service.ready
            # set controllable run state to ready
            controllable.run_state = controllable.ready
            # execute command
            command()


class _Ready(_ServiceState):
    def _execute(self, service, command, controllable):
        command()


class _PerformanceWriter(object):
    # class for writing performance in some file...
    # todo: need to write to session_id and execution mode (live, paper, simulation)
    # live and paper cannot be over-written, only appended
    # todo: the writer can make a call-back to some observers
    # we also have a reader (which is a writer observer)
    # the reader reads the performance in the file and waits for updates from the writer.
    # the updates are read before getting written in the filesystem.
    def __init__(self, session_id, monitor_stub, file_path, thread_pool):
        '''

        Parameters
        ----------
        file_path: str
        thread_pool: concurrent.futures.ThreadPoolExecutor
        '''
        self._path = file_path
        self._thread_pool = thread_pool

        self._none_observer = none = _NoneObserver()
        self._observer = _Observer(monitor_stub, file_path, session_id)
        self._current_observer = none

        self._lock = threading.Lock()

    def _write(self, performance, end, path):
        packet = conversions.to_proto_performance_packet(
            performance).SerializeToString()

        self._current_observer.update(packet, end)
        io.write_perf(path, packet)

    def performance_update(self, performance, end):
        # todo: we need to do a non-blocking write using queues?
        # self._thread_pool.submit(partial(
        #     self._write,
        #     performance=performance,
        #     end=end,
        #     path=self._path))
        self._write(performance, end, self._path)

    def observe(self):
        with self._lock:
            observer = self._observer
            observer.clear()
            self._current_observer = observer

    def stop_observing(self):
        with self._lock:
            self._current_observer = self._none_observer


class ControllableService(cbl_rpc.ControllableServicer):
    def __init__(self, monitor_stub, controllable_factory):
        self._perf_writer = None
        self._stop = False

        self._frequency_filter = None

        # used for queueing commands
        self._queue = queue.Queue()
        self._thread = None

        self._controllable = cbl = None

        self._ready = _Ready(self, cbl)
        self._recovery = recovery = _Recovering(self, cbl)
        self._state = recovery

        self._strategy_path = None
        self._directory = None
        self._state_storage = _NoStateStorage()

        self._root_dir = root = paths.get_dir('controllable')
        self._states_dir = paths.get_dir('states', root)

        self._thread_pool = futures.ThreadPoolExecutor(5)
        self._monitor_stub = monitor_stub
        self._cbl_fty = controllable_factory

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

    @method_access.framework_only
    def Initialize(self, request_iterator, context):
        b = b''
        for chunk in request_iterator:
            b += chunk.data

        params = controllable_pb2.InitParams()
        params.ParseFromString(b)

        id_ = params.id  # the id of the controllable => will be used in performance updates
        universe = params.universe
        capital = params.capital
        max_leverage = params.max_leverage
        strategy = params.strategy
        data_frequency = params.data_frequency
        start_dt = conversions.to_datetime(params.start)
        end_dt = conversions.to_datetime(params.end)
        look_back = params.look_back

        if data_frequency == 'daily':
            self._frequency_filter = DayFilter()
        elif data_frequency == 'minute':
            self._frequency_filter = MinuteFilter()

        mode = params.mode

        controllable = self._cbl_fty.get_controllable(mode, id_)

        # todo: we should have a directory for performance

        self._directory = dir_ = paths.get_dir(
            id_, paths.get_dir('states', self._root_dir))
        # activate state storage if we're in live mode

        # todo: it would be cleaner to have an utils file for common paths
        perf_path = paths.get_file_path(
            mode,
            paths.get_dir(
                id_,
                paths.get_dir('strategies')))

        if mode == 'live' or mode == 'paper':
            self._state_storage = _StateStorage(
                paths.get_file_path(mode, paths.get_dir(id_, dir_)),
                self._thread_pool)

        else:
            # clear file if we're in simulation mode
            with open(perf_path, 'wb') as f:
                f.truncate(0)

        # todo: we need a monitor stub
        self._perf_writer = _PerformanceWriter(
            id_,
            self._monitor_stub,
            paths.get_file_path(perf_path),
            self._thread_pool
        )

        if controllable:
            self._controllable = controllable
            controllable.initialize(
                id_,
                start_dt,
                end_dt,
                universe,
                strategy,
                capital,
                max_leverage,
                data_frequency,
                mode,
                look_back)
            # run the thread
            self._state = self._ready

            self._thread = thread = threading.Thread(target=self._run)
            thread.start()
        else:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Mode {} doesn't exist".format(mode))
        return emp.Empty()

    def _run(self):
        q = self._queue
        while not self._stop:
            try:
                self._state.execute(q.get())
            except commands.StopExecution:
                break

    def _load_state(self, session_id):
        with open(paths.get_file_path(session_id, self._states_dir), 'rb') as f:
            params = f.read()
            state = controllable_pb2.ControllableState()
            state.ParseFromString(params)
            return state

    def restore_state(self, session_id):
        # 1)create controllable, (PROBLEM: need mode (from controllable?))
        # 2)call restore state on it => this will restore its session_state
        # 4)load events from the events log and push them in the queue
        #    PROBLEM: we need the datetime (from controllable? => current_dt)
        # 5)start thread => this will start executing events in the queue
        # 6)the controllable must ignore "expired" events : events that have already been processed
        state = self._load_state(session_id)
        self._controllable = controllable = get_controllable(state.mode)

        with open('strategy', self._directory) as f:
            strategy = f.read()

        controllable.restore_state(state, strategy)
        # set run_state to recovering
        controllable.run_state = controllable.recovering
        log = events_log.get_events_log(state.mode)
        events = log.read(session_id, controllable.current_dt)
        # play all missed events since last checkpoint (controllable is in recovery mode)
        perf_writer = self._perf_writer
        frequency_filter = self._frequency_filter
        state_storage = self._state_storage

        # todo: need to handle all the event types

        for evt_type, evt in events:
            if evt_type == 'clock':
                commands.ClockUpdate(
                    perf_writer,
                    controllable,
                    frequency_filter,
                    evt,
                    state_storage)()
            elif evt_type == 'parameter':
                commands.CapitalUpdate(controllable, evt)()
            elif evt_type == 'broker':
                pass  # todo
            else:
                pass

    @method_access.framework_only
    def Stop(self, request, context):
        # todo needs to liquidate positions and wipe the state.
        self._stop = True
        return emp.Empty()

    @method_access.framework_only
    def UpdateParameters(self, request, context):
        self._queue.put(
            commands.CapitalUpdate(
                self._controllable,
                request
            )
        )
        return emp.Empty()

    @method_access.framework_only
    def UpdateAccount(self, request_iterator, context):
        # todo
        # self._queue.put(
        #     commands.
        # )
        return emp.Empty()

    @method_access.framework_only
    def ClockUpdate(self, request, context):
        '''Note: an update call might arrive while the step is executing..., so
        we must queue the update message... => the step must be a thread that pulls data
        from the queue...
        '''
        # NOTE: use FixedBasisPointsSlippage for slippage simulation.

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

    @method_access.framework_only
    def Watch(self, request, context):
        self._perf_writer.observe()

    @method_access.framework_only
    def StopWatching(self, request, context):
        self._perf_writer.stop_observing()


class Server(object):
    def __init__(self):
        self._event = threading.Event()
        self._server = grpc.server(futures.ThreadPoolExecutor(10))

    def start(self, controllable, url=None):
        server = self._server
        if not url:
            port = server.add_insecure_port('localhost:0')
        else:
            port = server.add_insecure_port(url)
        cbl_rpc.add_ControllableServicer_to_server(controllable, server)
        print(port)
        server.start()
        self._event.wait()
        controllable.stop()
        server.stop()

    def stop(self):
        self._event.set()


_SERVER = Server()


def termination_handler(signum, frame):
    _SERVER.stop()


def interruption_handler(signum, frame):
    _SERVER.stop()


signal.signal(signal.SIGINT, interruption_handler)
signal.signal(signal.SIGTERM, termination_handler)


@click.group()
def cli():
    pass


@cli.command()
@click.argument('framework_id')
@click.argument('framework_url')
@click.argument('session_id')
@click.argument('root_dir')
@click.option('-cu', '--controllable-url')
@click.option('--recovery', is_flag=True)
def start(framework_id, framework_url, session_id, root_dir, controllable_url, recovery):
    '''

    Parameters
    ----------
    framework_url : str
        url for callbacks
    controllable_url: str
    '''

    # If the controllable fails, it will be relaunched by the controller.

    # TODO: save the framework_url for future use. NOTE: the framework url must be immutable
    # (is a service in kubernetes)

    # run forever or until an exception occurs, in which case, send back a report to the controller
    # or write to a log file. If the strategy crashes internally, there might be some bug that
    # need reviewing

    with directory.StubDirectory(root_dir):
        # set the framework_id if ran as a process
        method_access._framework_id = framework_id
        channel = grpc.insecure_channel(framework_url)
        service = ControllableService(
            itf_rpc.MonitorStub(channel),
            factory.ControllableProcessFactory(channel))
        if recovery:
            service.restore_state(session_id)
        try:
            _SERVER.start(
                service,
                controllable_url)
        except Exception as e:
            # todo: write to log?, send report to controller?
            raise RuntimeError('Unexpected error', e)


if __name__ == '__main__':
    cli()
