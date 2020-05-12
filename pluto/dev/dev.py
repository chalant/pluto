import threading
from concurrent import futures

import grpc

from pluto.dev import editor
from pluto.explorer import explorer
from pluto.interface import monitor
from pluto.controller import controllerservice, controller
from pluto.coms.utils import conversions

from protos import development_pb2 as dev_rpc
from protos import development_pb2_grpc as development
from protos import controller_pb2_grpc as ctl_rpc
from protos import interface_pb2_grpc as interface


class DevService(development.EnvironmentServicer):
    def __init__(self,
                 server,
                 directory,
                 framework_url,
                 mode_factory,
                 loop_factory,
                 process_factory):
        self._directory = directory
        self._server = server
        self._controller = None

        self._framework_url = framework_url

        self._monitor = mon = monitor.Monitor()
        self._explorer = exp = explorer.Explorer(directory)
        self._editor = edt = editor.Editor(directory)

        self._process_factory = process_factory
        self._mode_factory = mode_factory
        self._loop_factory = loop_factory

        development.add_EditorServicer_to_server(edt, server)
        interface.add_ExplorerServicer_to_server(exp, server)
        interface.add_MonitorServicer_to_server(mon, server)

    def LoadSession(self, request, context):
        # todo: load previously set session
        pass

    def Setup(self, request, context):
        directory = self._directory
        with directory.write() as w:
            look_back = request.look_back
            data_frequency = request.data_frequency
            cancel_policy = request.cancel_policy
            # note: if no universe is provided, use the default universe.
            # a session regroups a set of "static" parameters (aren't likely to change overtime)

            # todo: the same strategy can't run on the same universe and data_frequency
            # the data frequency should be set at user-level? (framework-level for now...)

            # if this set of parameters all ready exists, return that session instead
            session = w.add_session(
                request.strategy_id,
                request.universe,
                data_frequency,
                look_back if look_back else 150,
                cancel_policy if cancel_policy else 'never_cancel')

        start = conversions.to_datetime(request.start)
        end = conversions.to_datetime(request.end)

        process_factory = self._process_factory

        # mode = simulation_mode.SimulationControlMode(
        #     self._framework_url,
        #     request.capital,
        #     request.max_leverage,
        #     process_factory)

        mode = self._mode_factory.get_mode(
            request.capital,
            request.max_leverage,
            process_factory)

        loop = self._loop_factory.get_loop(start, end)

        # loop = simulation_loop.SimulationLoop(start, end)
        loop.add_control_mode(mode)
        mon = self._monitor
        mon.set_control_mode(mode)
        # set monitor in-case we have an in-memory process factory
        process_factory.set_monitor_service(mon)

        self._controller = ctl = \
            controllerservice.ControllerService(
                directory,
                controller.Controller(
                    loop,
                    end))

        # enable controller service
        ctl_rpc.add_ControllerServicer_to_server(ctl, self._server)

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

    def stop(self):
        ctrl = self._controller
        if ctrl:
            ctrl.Stop()


class Server(object):
    def __init__(self):
        self._event = threading.Event()
        self._server = grpc.server(futures.ThreadPoolExecutor())

        self._environment = None
        self._framework_url = None

    def initialize(self, directory, framework_url):
        self._directory = directory
        self._framework_url = framework_url

        server = self._server

        self._environment = env = DevService(server, directory, framework_url)
        development.add_EnvironmentServicer_to_server(env, server)

    def serve(self):
        server = self._server
        server.add_insecure_port(self._framework_url)

        server.start()
        event = self._event
        event.clear()
        event.wait()
        env = self._environment
        if env:
            env.stop()
        server.stop(0)

    def stop(self):
        self._event.set()
