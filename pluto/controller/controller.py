import grpc

from protos import controller_pb2_grpc

from pluto.control.loop import simulation_loop
from pluto.control.modes import simulation_mode
from pluto.broker import simulation_broker

class Controller(controller_pb2_grpc.ControllerServicer):
    def __init__(self, directory):
        self._directory = directory

    def Run(self, request, context):
        with self._directory.read() as d:
            # steps for running a strategy:
            # if the strategy is not already running,
            # 1)create a controllable, run it as a sub-process
            # 2)assign a strategy to the controllable either through cli or
            # through the service (initiliaze + parameters)
            # 3)create a stub
            # 4)on the stub, call Run

            # todo: how do we filter clock signals?
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

            # the universe is in the request. If not, set a default value.
            # we provide the universe as a parameter in the UI.
            # session = Session(param)
            # so: filter = mode.get_filter(strategy.universes)
            # mode.add_session(filter, strategy, name)

            # todo: parmeters must be set before launching in the loop

            # todo: the capital is set in the mode. and all the strategy that run in a mode
            # share the same capital. On backtesting, the mode only exists in the scope
            # of the method.

            # todo: each strategy must subscribe to a set of exchanges in order to "filter"
            # clock events. Or we could just check if the clock time is in the sessions...
            # the sessions are created by combining calendars of different exchanges.
            # we should flag a strategy if its backtest was successful (without errors)
            stg_id = request.strategy_id
            start = request.start
            end = request.end
            captial_ratio = request.capital_ratio
            max_leverage = request.max_leverage

            stg = d.get_strategy(stg_id)
            imp = stg.get_implementation()

            # the loop generates clock events
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
            # todo: domain filters? => we don't use the "domain" concept here.

            # problem: how do we know which clocks to run ?
            # should the user provide the "domain" in the script? should this be required?
            if self._running:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details('Backtest already running')
            # this call blocks until the controller is either interrupted or has completed the run.
            else:
                lp.execute(commands.Setup())
                lp.start()
                self._running = True
                # todo: the controllable should run on the same
            return  # todo

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


class SimulationController(Controller):
    def __init__(self, directory, start, end, broker):
        super(SimulationController, self).__init__(directory)
        self._control_mode = mode = simulation_mode.SimulationControlMode(broker)
        self._loop = simulation_loop.MinuteSimulationLoop(start, end, mode)

    def _run(self, request, context):
        params = request.run_params




