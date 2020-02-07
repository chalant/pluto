from protos import interface_pb2_grpc as interface
from protos import interface_pb2 as itf_msg

class Explorer(interface.ExplorerServicer):
    def __init__(self, directory):
        self._directory = directory

    def StrategyList(self, request, context):
        with self._directory.read() as d:
            strategies = d.get_strategy_list()
            for stg in strategies:
                yield itf_msg.StrategyResponse(strategy_id=stg.id, name=stg.name)

    def SessionList(self, request, context):
        #todo
        pass