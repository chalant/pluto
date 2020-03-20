import subprocess
from protos import controllable_pb2_grpc as rpc
import grpc


from socket import socket
from pluto.control.trader import trader

class SimulationTrader(trader.Trader):
    def __init__(self):
        self._process = None
        self._controllable = None

    def initialize(self):
        pass



    def start(self, broker_url):
        process = self._process
        if not process:
            self._process = self._create_process(broker_url)

    def stop(self):
        process = self._process
        if process:
            process.terminate()

    def _create_process(self, broker_url):
        # creates an address for the controllable
        # in simulation, creates a local controllable
        s = socket()
        s.bind(('', 0))
        address = 'localhost:{}'.format(s.getsockname()[1])
        s.close()

        #todo: while the socket is closed, some other process could grab the address
        # before we create our subprocess
        # todo: run the controllable.py script as a subprocess (using popen) and pass it the
        # generated address
        # then create a channel on the generated address, pass it to a controllable stub
        # and return the controllable stub also, we need to propagate kill signals
        # we will be using the subprocess module to propagate the kill signal etc.
        process = subprocess.Popen([
            'python',
            'contrib/control/controllable/server.py',
            'start',
            address,
            broker_url
        ])
        channel = grpc.insecure_channel(address)
        self._controllable = rpc.ControllableStub(channel)
