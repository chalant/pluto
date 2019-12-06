import grpc

import io

from protos import interface_pb2 as itf_msg
from protos import interface_pb2_grpc as itf_rpc

from contrib.control.modes import simulation_mode
from contrib.control.loop import simulation_loop as loop
from contrib.control import commands

class Editor(itf_rpc.EditorServicer):
    def __init__(self, directory, controller):
        '''

        Parameters
        ----------
        directory: contrib.interface.directory.Directory
        controller: contrib.control.controller.controller.Controller

        '''
        self._directory = directory
        self._controller = controller
        self._loop = None

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

        #todo: deploying the strategy "freezes" it to the universe
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
                    s = d.add_strategy(stg.name)
                    s.store_implementation(request.strategy)
            except RuntimeError:
                #todo: we need to read errors and send them back to the client
                # note: we can't make the interpreter crash, so we need to run
                # the script as a subprocess. => Use the 'basic' api to run a
                # note : since the user might run a backtest on this, we will share the
                # same methods as the backtest method
                context.set_details('The strategy script contains some errors')
                context.set_code(grpc.StatusCode.ABORTED)

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
            try:
                s.store_implementation(stg.strategy)
            except RuntimeError:
                ns = d.add_strategy(s.name)
                ns.store_implementation(stg.strategy)

    def BackTest(self, request, context):
        with self._directory.read() as d:
            #steps for running a strategy:
                #if the strategy is not already running,
                    #1)create a controllable, run it as a sub-process
                    #2)assign a strategy to the controllable either through cli or
                        #through the service (initiliaze + parameters)
                    #3)create a stub
                    #4)on the stub, call Run

            #todo: how do we filter clock signals?
            # the user can call a "set_universe" function and provide a universe
            # universe name. We use a "default" universe that filters stocks based
            # on some criteria.
            # the universe can be an aggregate of multiple exchanges
            # do we need to filter clock signals? this should be handled on control
            # layer. the clocks are determined by the exchanges covered by the universe.
            # so the clocks signals are filtered on "universe" layer.
            # loop => mode(s) => universe(s) => session(s)
            # => this means that universes are not set on controllable/strategy layer
            #  there are at a level above. Signals are filtered server-side.

            #the universe is in the request. If not, set a default value.
            #we provide the universe as a parameter in the UI.
            #session = Session(param)
            #so: filter = mode.get_filter(strategy.universes)
            # mode.add_session(filter, strategy, name)

            #todo: parmeters must be set before launching in the loop

            #todo: the capital is set in the mode. and all the strategy that run in a mode
            # share the same capital. On backtesting, the mode only exists in the scope
            # of the method.

            #todo: each strategy must subscribe to a set of exchanges in order to "filter"
            # clock events. Or we could just check if the clock time is in the sessions...
            # the sessions are created by combining calendars of different exchanges.
            #we should flag a strategy if its backtest was successful (without errors)
            stg_id = request.strategy_id
            start = request.start
            end = request.end
            capital = request.capital
            max_leverage = request.max_leverage

            capital_ratio = 1 #the strategy will be allocated 100% of the capital

            stg = d.get_strategy(stg_id)
            imp = stg.get_implementation()

            #the loop generates clock events
            #

            # todo: run a loop around this
            # method blocks until completion or interruption
            # todo: client might 'misbehave' and call this method multiple times...
            # todo: this shouldn't block, since the client might need to call stop etc.
            # todo: if this method is called while it is running, it will add the sessions to the loop
            #  (in live mode), won't do anything in dev? => each type of service has a different way
            #  of handling multiple calls...
            # todo: problem we can have multiple clients... => limit to one client?

            # todo: the control mode takes a loop as argument...
            ctl = self._controller
            self._loop = lp = loop.SimulationMinuteLoop(
                start.ToDatetime(),
                end.ToDatetime(),
                capital=capital,
                max_leverage=max_leverage)
            # the mode creates a controllable sub-process on run.
            #todo: domain filters? => we don't use the "domain" concept here.

            #problem: how do we know which clocks to run ?
            # should the user provide the "domain" in the script? should this be required?
            if self._running:
                raise grpc.RpcError('A session is already running!')
            # this call blocks until the controller is either interrupted or has completed the run.
            else:
                lp.execute(commands.Setup())
                lp.start()
                self._running = True

            return  # todo

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