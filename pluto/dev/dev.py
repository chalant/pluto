from concurrent import futures

import grpc

from protos import development_pb2_grpc as development
from protos import controller_pb2_grpc as ctl_rpc
from protos import interface_pb2_grpc as interface

from pluto.dev import editor
from pluto.explorer import explorer
from pluto.controller import controller
from pluto.broker import simulation_broker as sb
from pluto.broker import broker_service

class Environment(development.EnvironmentServicer):
    def __init__(self, directory):
        self._directory = directory
        self._explorer = exp = explorer.Explorer(directory)
        self._editor = editor.Editor(directory)

        self._broker = None
        self._controller = None

        pool = futures.ThreadPoolExecutor()
        #server used for the framework objects (controllables, broker, etc.)
        self._framework_server = grpc.server(pool)
        self._server = server = grpc.server(pool)

        development.add_EnvironmentServicer_to_server(self, server)
        interface.add_ExplorerServicer_to_server(exp, server)


    def Setup(self, request, context):
        directory = self._directory
        #todo: what if the loop is already running? => the controller (in client) must first call
        # setup. If no values have been provided by the client, the controller sets some default
        # values

        #todo setup a new broker with new capital etc.
        start = request.start
        end = request.end
        #todo:
        #changes in the broker will be updated on the next loop iteration.

        #setup up the broker
        # todo: metrics tracker, blotter, current_data etc.
        self._broker = broker = sb.SimulationBroker()
        broker.capital = request.capital
        broker.max_leverage = request.max_leverage

        # broker_rpc.add_BrokerServicer_to_server(broker, self._broker_server)
        #the broker service runs on a different server, so that it can't be accessed by the user
        port = self._framework_server.start('localhost:0')
        broker_srv = broker_service.BrokerService(broker, 'localhost:{}'.format(port))

        self._controller = sim_ctl = controller.SimulationController(directory, start, end, broker_srv)
        ctl_rpc.add_ControllerServicer_to_server(sim_ctl, self._server)

    def start(self, port, address='localhost'):
        self._server.start(address+':'+port)

    def stop(self, grace=0):
        self._server.stop(grace)
