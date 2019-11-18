from protos import interface_pb2 as itf_msg
from protos import interface_pb2_grpc as itf_rpc

class Editor(itf_rpc.EditServicer):
    def __init__(self, directory):
        '''

        Parameters
        ----------
        directory
        '''
        self._directory = directory

    def New(self, request, context):
        with self._directory as d:
            stg = d.add_strategy(request.name, request.strategy_id)
            return itf_msg.NewStrategyRequest(template=stg.get_implementation())

    def StrategyList(self, request, context):
        pass

    def Deploy(self, request, context):
        '''

        Parameters
        ----------
        request
        context

        Returns
        -------

        Notes
        -----

        Freezes the strategy directory so that it can be ran in live and paper.
        No modifications will be allowed
        '''
        pass

    def BackTest(self, request, context):
        pass