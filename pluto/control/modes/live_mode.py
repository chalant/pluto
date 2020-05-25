from pluto.control.modes import mode
from pluto.control.modes.processes import process_manager

from protos import broker_pb2_grpc

class LiveControlMode(mode.ControlMode):
    def __init__(self, server, framework_url, process_factory):
        super(LiveControlMode, self).__init__(framework_url, process_factory)
        broker_pb2_grpc.add_BrokerServicer_to_server(self._broker, server)

    def _create_process_manager(self):
        return process_manager.LiveProcessManager()

    def _accept_loop(self, loop):
        # todo: only accept LiveLoop type or subtypes
        return False

