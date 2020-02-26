import threading
import signal
from concurrent import futures
import queue
import abc

import grpc
import click
from google.protobuf import empty_pb2 as emp

from pluto.interface.utils import paths
from pluto.coms.utils import conversions
from pluto.control.controllable import commands
from pluto.control.controllable import simulation_controllable as sc
from pluto.control.events_log import events_log

from protos import controllable_pb2
from protos import controllable_pb2_grpc as cbl_rpc
from protos.clock_pb2 import (
    BAR,
    TRADE_END)

ROOT = paths.get_dir('controllable')
DIR = paths.get_dir('states', ROOT)


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
        raise NotImplementedError('live controllable')
    else:
        return


class _StateStorage(object):
    def __init__(self, storage_path):
        self._storage_path = storage_path

    def store(self, dt, controllable):
        # todo: non-blocking!
        # todo: PROBLEM: we might have some conflicts in state, since we could have
        # multiple controllables with the same session_id running in different
        # modes...

        with open(self._storage_path, 'wb') as f:
            f.write(controllable.get_state(dt))

    def load_state(self, session_id):
        return ''


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
    def __init__(self, session_id, mode):
        pass

    def performance_update(self, performance):
        print(performance['cumulative_perf']['period_close'])
        with open(paths.get_file_path('performance', ROOT), 'a') as f:
            f.write(str(performance['cumulative_risk_metrics'])+'\n')
        # print(performance)
        # writer.PerformancePacketUpdate(
        #     crv.to_proto_performance_packet(
        #         controllable.session_end(dt)))


class ControllableService(cbl_rpc.ControllableServicer):
    def __init__(self, framework_url):
        self._perf_writer = None
        self._ctl_url = framework_url

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
    def _with_metadata(self, rpc, params):
        '''If we're not registered, an RpcError will be raised. Registration is handled
        externally.'''
        return rpc(params, metadata=(('Token', self._token)))

    def stop(self):
        pass

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

        controllable = get_controllable(mode)

        self._directory = dir_ = paths.get_dir(id_, DIR)
        # activate state storage if we're in live mode
        if mode == 'live':
            self._state_storage = _StateStorage(
                paths.get_file_path('state', dir_))

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
            self._perf_writer = _PerformanceWriter(id_, mode)
            self._thread = thread = threading.Thread(target=self._run)
            thread.start()
        else:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Mode {} doesn't exist".format(mode))
        return emp.Empty()

    def _run(self):
        queue = self._queue
        while not self._stop:
            self._state.execute(queue.get())

    def _load_state(self, session_id):
        with open(paths.get_file_path(session_id, DIR), 'rb') as f:
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
        events = events_log.read(session_id, controllable.current_dt)
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
                commands.CapitalUpdate(controllable, evt)()
            elif evt_type == 'broker':
                pass  # todo
            else:
                pass

    def Stop(self, request, context):
        # todo needs to liquidate positions and wipe the state.
        self._stop = True
        return emp.Empty()

    def UpdateParameters(self, request, context):
        self._queue.put(
            commands.CapitalUpdate(
                self._controllable,
                request
            )
        )
        return emp.Empty()

    def UpdateAccount(self, request_iterator, context):
        # todo
        # self._queue.put(
        #     commands.
        # )
        return emp.Empty()

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
@click.argument('framework_url')
@click.argument('session_id')
@click.option('-cu', '--controllable-url')
@click.option('--recovery', is_flag=True)
def start(framework_url, session_id, controllable_url, recovery):
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
    service = ControllableService(framework_url)
    # run forever or until an exception occurs, in which case, send back a report to the controller
    # or write to a log file. If the strategy crashes internally, there might be some bug that
    # need reviewing
    if recovery:
        service.restore_state(session_id)
    try:
        _SERVER.start(
            service,
            controllable_url)
    except Exception as e:
        # todo: write to log?, send report to controller?
        pass


if __name__ == '__main__':
    cli()
