from concurrent import futures

import grpc

from protos import development_pb2 as dev_rpc
from protos import development_pb2_grpc as development
from protos import controller_pb2_grpc as ctl_rpc
from protos import interface_pb2_grpc as interface

from pluto.dev import editor
from pluto.explorer import explorer
from pluto.controller import controllerservice, controller
from pluto.data.universes import universes

class Environment(development.EnvironmentServicer):
    def __init__(self, directory, framework_url):
        self._directory = directory
        self._explorer = exp = explorer.Explorer(directory)
        self._editor = edt = editor.Editor(directory)

        self._controller = None
        self._framework_url = framework_url

        # pool = futures.ThreadPoolExecutor()
        #server used for the framework objects (controllables, broker, etc.)
        # self._framework_server = grpc.server(pool)
        self._server = server = grpc.server(futures.ThreadPoolExecutor())

        development.add_EnvironmentServicer_to_server(self, server)
        development.add_EditorServicer_to_server(edt, server)
        interface.add_ExplorerServicer_to_server(exp, server)

    def Setup(self, request, context):
        directory = self._directory
        with directory.write() as w:
            #note: if no universe is provided, use the default universe.
            #a session regroups a set of "static" parameters (aren't likely to change overtime)
            session = w.add_session(
                request.strategy_id,
                universes.get_universe(request.universe))

        start = request.start
        end = request.end
        look_back = request.look_back
        data_frequency = request.data_frequency

        self._controller = sim_ctl = \
            controllerservice.ControllerService(
                directory,
                controller.SimulationController(
                    self._framework_url,
                    request.capital,
                    request.max_leverage,
                    start.ToDatetime(),
                    end.ToDatetime()))

        #enable controller service
        ctl_rpc.add_ControllerServicer_to_server(sim_ctl, self._server)

        return dev_rpc.SetupResponse(session_id = session.id)

    def start(self, port, address='localhost'):
        self._server.start(address+':'+port)

    def stop(self, grace=0):
        self._server.stop(grace)
