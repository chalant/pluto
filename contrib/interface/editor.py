import grpc

from protos import interface_pb2 as itf_msg
from protos import interface_pb2_grpc as itf_rpc

class Editor(itf_rpc.EditorServicer):
    def __init__(self, directory):
        '''

        Parameters
        ----------
        directory : contrib.interface.directory.Directory
        '''
        self._directory = directory

    def New(self, request, context):
        with self._directory.write() as d:
            stg = d.add_strategy(request.name, request.strategy_id)
            for b in stg.get_implementation():
                yield itf_msg.Strategy(chunk=b)

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

        Locks the strategy directory so that it can be ran in live and paper mode.
        No modifications can occur once a strategy is locked.
        Modifications on copies are allowed
        '''
        with self._directory.write() as d:
            stg = d.get_strategy(request.strategy_id)
            #todo: must check if the script can actually run without errors.
            try:
                self._test_strategy(stg.get_implementation(), stg.path)
                try:
                    stg.store_implementation(request.strategy)
                    # lock the strategy
                    stg.lock()
                except RuntimeError:
                    # this should not happen since the client cannot receive a locked
                    # strategy but handle the exception just in case.
                    d.add_strategy(stg.name, stg.id)
            except RuntimeError:
                context.set_details('The strategy script contains some errors')
                context.set_code(grpc.StatusCode.ABORTED)


    def _test_strategy(self, strategy, path):
        script = strategy.decode('utf-8')
        #todo : see zipline/algorithm.py
        #todo : must set a namespace.
        #todo: we need to prepare the whole environment for running the strategy
        # (see zipline/algorithm.py).
        #todo : run a small test to check for errors: raise a runtime error
        # if the strategy contains errors and send the interpreters output to the
        # client.
        # 1)syntax errors, 2)execution errors
        # todo: write the output stream and send it back to the client as a string
        #this stage should raise some syntax errors.
        ast = compile(script, path, 'exec')

    def StrategyList(self, request, context):
        with self._directory.read() as d:
            strategies = d.get_strategy_list()
            for stg in strategies:
                yield itf_msg.StrategyResponse(strategy_id=stg.id, name=stg.name)

    def GetStrategy(self, request, context):
        '''

        Parameters
        ----------
        request
        context

        Returns
        -------
        typing.Generator
        '''
        with self._directory.read() as d:
            stg = d.get_strategy(request.strategy_id)
            for b in stg.get_implementation():
                yield itf_msg.Strategy(chunk=b)



    def BackTest(self, request, context):
        #todo
        pass