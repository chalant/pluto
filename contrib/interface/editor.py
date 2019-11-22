import grpc

import io

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
            stg = d.add_strategy(request.name)
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
                    d.add_strategy(stg.name)
            except RuntimeError:
                #todo: we need to read errors and send them back to the client
                # note: we can't make the interpreter crash, so we need to run
                # the script as a subprocess. => Use the 'basic' api to run a
                # note : since the user might run a backtest on this, we will share the
                # same methods as the backtest method
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

    def Save(self, request_iterator, context):
        with self._directory.write() as d:
            buffer = ''
            for chunk in request_iterator:
                buffer += chunk.chunk
            stg = itf_msg.Strategy()
            stg.ParseFromString(buffer)
            s = d.get_strategy(stg.id)
            self._store_strategy(d, s)

    def _store_strategy(self, directory, strategy):
        try:
            strategy.store_implementation(stg.strategy)
        except RuntimeError:
            ns = d.add_strategy(s.name)
            ns.store_implementation(stg.strategy)


    def BackTest(self, request, context):
        #todo:
        #we should flag a strategy if its backtest was successful (without errors)
        pass