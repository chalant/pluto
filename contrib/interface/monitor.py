from protos import interface_pb2 as itf_msg
from protos import interface_pb2_grpc as itf_rpc

class Monitor(itf_rpc.MonitorServicer):
    def __init__(self, directory):
        '''
        Parameters
        ----------
        directory : contrib.interface.directory.Directory
        '''
        self._directory = directory

    def Watch(self, request, context):
        pass

    def Stop(self, request, context):
        pass

    def Run(self, request, context):
        pass

    def StrategyList(self, request, context):
        pass

    def InspectCode(self, request, context):
        pass