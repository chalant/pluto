import abc

from . import controller

from contrib.control.clock import clock
from contrib.utils import stream

import socket

#TODO: the controller should always send a "session-id" as metadata
# the client and the controller will exchange this session-id
#TODO: raise a grpc error if a request doesn't have a session-id...
#TODO: We should encapsulate the controller, since the behavior changes with the environment..
class SimulationControlMode(controller.ControlMode):
    #todo: the control mode should take-in a broker
    def __init__(self, sim_clock_factory):
        self._filter = clock.BaseSignalFilter()

    def name(self):
        return 'simulation'

    def signal_filter(self):
        return self._filter

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

    def _get_broker(self):
        #todo: create and return a simulation broker
        return

class SimulationClockListener(clock.ClockListener):
    def __init__(self, simulation_broker):
        #the simulation broker intance can be used with other listeners
        self._simulation_broker = simulation_broker

    def _clock_update(self, request, sessions):
        #the broker is updated before the sessions

        #todo: broker will only update the values of the elements that are traded on
        # the exchange specified by the signal.
        broker = self._simulation_broker
        state = broker.clock_update(request) #update the broker: simulates all the received
                                             #from all the sessions
        #update the accounts of the sessions with the broker state
        for session in sessions:
            session.broker_update(stream.chunk_bytes(state.SerializeToString()))

        #perform the next action
        for session in sessions:
            session.clock_update(request)

class SimulationClockSignalRouter(clock.ClockSignalRouter):
    def __init__(self, start_dt, end_dt):
        super(SimulationClockSignalRouter, self).__init__(start_dt, end_dt)

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

