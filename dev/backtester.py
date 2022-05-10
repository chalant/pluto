from dev import editor
from pluto.explorer import explorer
from pluto.interface import monitor
from pluto.controller import controllerservice, controller
from pluto.coms.utils import conversions
from pluto.server import service

from pluto.control.controllable import server

from pluto.control.modes import mode
from pluto.broker import broker
from pluto.control.controllable.utils import factory

from pluto.control.loop import simulation_loop

from pluto.universes import universes
from pluto.control.processes import in_memory
from pluto.control.processes import processes

from pluto.control import commands

from protos import development_pb2 as dev_rpc
from protos import development_pb2_grpc as development
from protos import controller_pb2_grpc as ctl_rpc
from protos import interface_pb2_grpc as interface


class BacktestingBroker(broker.Broker):
    pass


# todo: this is a process?
class BackTestingSignalHandler(object):
    def __init__(self, backtesting_setup, process):
        self._setup = backtesting_setup
        self._broker = broker.SimulationBroker(
            self._capital,
            self._mac
        )

    def initialize(self, ):
        pass

    def clock_update(self, ts, evt, signals):
        pass

    def _create_broker(self):
        return broker.SimulationBroker(
            self._capital,
            self._max_leverage)


class BackTester(object):
    def __init__(self, directory):
        self._controller = controllerservice.ControllerService(directory)

        # note end date is always the most recent data point
        self._directory = directory

        self._stg_stp = None
        self._process = None

    def initialize(self, setup):
        # todo: create broker stub ?
        # todo: start a controllable process...

        # get calendar of the universe

        self._process = process = processes.Process(setup.strategy_id)

        process.set_controllable(
            in_memory.InMemoryControllableStub(
                server.ControllableService(
                    in_memory.InMemoryMonitorStub(),
                    in_memory.InMemoryControllableFactory(),
                    self._directory
                )
            )
        )

        # loop = simulation_loop.SimulationLoop(start, end)
        mon = self._monitor
        mon.set_control_mode(mode)

        return process.initialize(setup)

    def run(self, setup):

        uni = universes.get_universe(setup.trading_universe)

        loop = simulation_loop.SimulationLoop(
            setup.start,
            setup.end,
            self._process,
            setup.frequency)

        loop.add_clock(uni.exchanges)

        loop.start()

        # self._controller.set_controller(controller.Controller(
        #     loop,
        #     setup.end))

