import abc

from . import controller

from contrib.control.clock import clock

import socket

#TODO: the controller should always send a "session-id" as metadata
# the client and the controller will exchange this session-id
#TODO: raise a grpc error if a request doesn't have a session-id...
#TODO: We should encapsulate the controller, since the behavior changes with the environment..
class SimulationControlMode(controller.ControlMode):
    #todo: the control mode should take-in a broker
    def __init__(self, sim_clock_factory, start_dt, end_dt):
        self._router = SimulationClockSignalRouter(sim_clock_factory, start_dt, end_dt)

    def name(self):
        return 'simulation'

    def signal_router(self):
        return self._router

    def _get_controllable(self):
        #creates an address for the controllable
        #in simulation, creates a local controllable
        s = socket()
        s.bind(('', 0))
        address = 'localhost:{}'.format(s.getsockname()[1])
        s.close()

        #todo: run the controllable.py script as a subprocess (using popen) and pass it the
        # generated address
        # then create a channel on the generated address, pass it to a controllable stub
        # and return the controllable stub also, we need to propagate kill signals
        # we will be using the subprocess module to propagate the kill signal etc.
        return

class SimulationClockListener(clock.ClockListener):
    def _clock_update(self, request, sessions):
        for session in sessions:
            session.clock_update(request)

class SimulationClockSignalRouter(clock.ClockSignalRouter):
    def __init__(self, sim_clock_factory, start_dt, end_dt):
        super(SimulationClockSignalRouter, self).__init__(start_dt, end_dt)
        self._sim_clock_fct = sim_clock_factory

    def _get_listener(self):
        return clock.DelimitedClockListener(SimulationClockListener())

    def _get_clock(self, exchange):
        return self._sim_clock_fct.get_clock(exchange, self._start_date, self._end_date)

class SimulationClockFactory(abc.ABC):
    def get_clock(self, exchange):
        return self._get_clock(exchange)

    @abc.abstractmethod
    def _get_clock(self, exchange):
        raise NotImplementedError