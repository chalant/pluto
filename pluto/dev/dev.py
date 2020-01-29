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
from pluto.coms.utils import conversions


class Environment(development.EnvironmentServicer):
    def __init__(self, directory, framework_url):
        self._directory = directory
        self._explorer = exp = explorer.Explorer(directory)
        self._editor = edt = editor.Editor(directory)

        self._controller = None
        self._framework_url = framework_url

        # pool = futures.ThreadPoolExecutor()
        # server used for the framework objects (controllables, broker, etc.)
        # self._framework_server = grpc.server(pool)
        self._server = server = grpc.server(futures.ThreadPoolExecutor())

        development.add_EnvironmentServicer_to_server(self, server)
        development.add_EditorServicer_to_server(edt, server)
        interface.add_ExplorerServicer_to_server(exp, server)

    def Setup(self, request, context):
        directory = self._directory
        with directory.write_event() as w:
            look_back = request.look_back
            data_frequency = request.data_frequency
            # note: if no universe is provided, use the default universe.
            # a session regroups a set of "static" parameters (aren't likely to change overtime)
            session = w.add_session(
                request.strategy_id,
                universes.get_universe(request.universe),
                data_frequency,
                look_back if look_back else 150)

        start = request.start
        end = request.end

        self._controller = sim_ctl = \
            controllerservice.ControllerService(
                directory,
                controller.SimulationController(
                    self._framework_url,
                    request.capital,
                    request.max_leverage,
                    conversions.to_datetime(start),
                    conversions.to_datetime(end)))

        # enable controller service
        ctl_rpc.add_ControllerServicer_to_server(sim_ctl, self._server)

        return dev_rpc.SetupResponse(session_id=session.id)

    def Delete(self, request, context):
        # todo deletes the (local) session, along with all the associated performance files
        pass

    def _test_strategy(self, strategy, path):
        script = strategy.decode('utf-8')
        # todo : see zipline/algorithm.py
        # todo : must set a namespace.
        # todo: we need to prepare the whole environment for running the strategy
        # (see zipline/algorithm.py).
        # todo : run a small test to check for errors: raise a runtime error
        # if the strategy contains errors send the interpreters output to the
        # client.
        # 1)syntax errors, 2)execution errors
        # todo: write the output stream and send it back to the client as a string
        # this stage should raise some syntax errors.
        ast = compile(script, path, 'exec')

    def start(self, port, address='localhost'):
        self._server.start(address + ':' + port)

    def stop(self, grace=0):
        self._server.stop(grace)
