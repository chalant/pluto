import grpc

from protos import controller_pb2_grpc

class ControllerService(controller_pb2_grpc.ControllerServicer):
    def __init__(self, directory, controller):
        '''

        Parameters
        ----------
        directory
        loop : pluto.control.loop.loop.Loop
        '''
        self._directory = directory
        self._controller = controller

    def Run(self, request, context):
        with self._directory.read() as d:
            # steps for running a strategy:
            # if the strategy is not already running,
            # 1)create a controllable, run it as a sub-process
            # 2)assign a strategy to the controllable either through cli or
            # through the service (initialize + parameters)
            # 3)create a stub
            # 4)on the stub, call Run

            # todo: parameters must be set before launching in the loop

            # todo: the capital is set in the mode. and all the strategy that run in a mode
            # share the same capital. On backtesting, the mode only exists in the scope
            # of the method.

            # clock events. Or we could just check if the clock time is in the sessions...
            # the sessions are created by combining calendars of different exchanges.
            # we should flag a strategy if its backtest was successful (without errors)

            #contains all the sessions, (maybe including the ones that are running)
            #if the ones that are running are omitted, they will be liquidated.

            # todo: if this method is called while it is running, it will add the sessions to the loop
            #  (in live mode), won't do anything in dev? => each type of service has a different way
            #  of handling multiple calls...

            ctl = self._controller
            try:
                ctl.run(d, request.run_params)
            except RuntimeError as e:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(str(e))
            # the mode creates a controllable sub-process on run.
            # todo: domain filters? => we don't use the "domain" concept here.

            # problem: how do we know which clocks to run ?
            return

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

    def Stop(self, request, context):
        pass

    def Watch(self, request, context):
        pass




