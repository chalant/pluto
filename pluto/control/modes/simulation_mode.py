from pluto.control.modes import mode

from socket import socket
import subprocess


class SimulationControlMode(mode.ControlMode):
    #todo: the control mode should take-in a broker
    def __init__(self, broker):
        super(SimulationControlMode, self).__init__()
        self._broker = broker

    def name(self):
        return 'simulation'

    def _create_trader(self, broker_url):
        #creates an address for the controllable
        #in simulation, creates a local controllable

        #todo: check read the subprocess stdout to get its address.
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

        #todo: return stub
        return

    def _create_broker(self):
        return self._broker

