from concurrent import futures

from contrib.control import controllable

from contrib.control import algocontrol as ac

from contrib.finance.metrics import tracker
from contrib.coms.client import account
from protos import broker_pb2_grpc as br

from contrib.broker import simulation_broker as sb

from contrib.control.clock_pb2 import (
    BAR,
    INITIALIZE
)

import grpc

class SimulationBrokerServicer(br.BrokerServicer):
    def __init__(self, simulation_broker):
        """

        Parameters
        ----------
        simulation_broker : sb.SimulationBroker
        """

        self._sim_br = simulation_broker

    def SingleOrder(self, request, context):
        self._sim_br.order()

    def BatchOrder(self, request, context):
        pass

    def Transactions(self, request, context):
        pass

class SimulationRunMode(ac.BaseRunMode):
    def __init__(self, server, broker_client):
        """
        Parameters
        ----------
        broker_client : contrib.coms.client.account.BrokerClient
        """
        self._control_mode = ac.BaseRunMode()
        self._sim_broker = sim_br = sb.SimulationBroker()
        br.add_BrokerServicer_to_server(SimulationBrokerServicer(sim_br), server)
        self._broker_client = broker_client

    def on_bar(self, dt, algo, bar_data, metrics_tracker, data_portal, calendar, bundler, broker_state):
        self._sim_broker.on_bar(dt, bar_data)
        self._broker_client.update(broker_state)
        return self._control_mode.on_bar(dt, algo, bar_data, metrics_tracker, data_portal, calendar, bundler)

class SimulationControllable(controllable.Controllable):
    def _update(self, dt, event, calendar, broker_state):
        pass

class Simulator(controllable.Controllable):
    def __init__(self, address, strategy, benchmark_asset, restrictions, state_storage_path):
        server = grpc.server(futures.ThreadPoolExecutor(5))
        server.add_insecure_port(address)
        super(Simulator,self).__init__(server)
        broker_client = account.BrokerClient(grpc.insecure_channel(address))
        self._controller = ac.AlgorithmController(
            ac.SimulationRunMode(server, broker_client),
            strategy,
            benchmark_asset,
            restrictions,
            state_storage_path)
        self._metrics_tracker = tracker.MetricsTracker(broker_client)

    def _update(self, dt, event, calendar, broker_state):
        if event == BAR:
            self._controller.on_bar(dt)
        elif event == INITIALIZE:
            self._controller.on_initialize(dt, self._metrics_tracker, calendar)
        pass

def run_simulation(address, strategy, benchmark_asset, restrictions, state_storage_path):
    simulator = Simulator(address, strategy, benchmark_asset, restrictions, state_storage_path)
