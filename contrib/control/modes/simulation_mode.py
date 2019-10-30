from contrib.control.clock import domain_filter
from contrib.control.modes import mode
from contrib.control.broker import simulation_broker as sb

from socket import socket
import subprocess

#TODO: the controller should always send a "session-id" as metadata
# the client and the controller will exchange this session-id
#TODO: raise a grpc error if a request doesn't have a session-id...
#TODO: We should encapsulate the controller, since the behavior changes with the environment..
class SimulationControlMode(mode.ControlMode):
    #todo: the control mode should take-in a broker
    def __init__(self):
        super(SimulationControlMode, self).__init__()
        self._broker = sb.SimulationBroker()#todo: we need a simulation blotter

    def name(self):
        return 'simulation'

    def _create_domain_filter(self, dom_def, clocks, exchange_mappings, domain_id, broker):
        return domain_filter.SimulationDomainFilter(dom_def, clocks, exchange_mappings, broker)

    def _create_trader(self, broker_url):
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
        subprocess.Popen(['python', 'contrib/control/controllable/server.py', 'start'])
        return

    def _create_broker(self):
        return self._broker

