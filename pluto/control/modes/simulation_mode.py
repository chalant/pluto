from pluto.control.modes import mode
from pluto.control.modes.processes import local_process
from pluto.control.events_log import events_log

class SimulationControlMode(mode.ControlMode):
    def __init__(self, framework_url, capital, max_leverage):
        super(SimulationControlMode, self).__init__(framework_url)
        self._capital = capital
        self._max_leverage = max_leverage

    def _create_process(self, session_id, framework_url):
        #creates an address for the controllable
        #in simulation, creates a local controllable

        #todo: run the controllable.py script as a subprocess (using popen) and pass it the
        # generated address
        # then create a channel on the generated address, pass it to a controllable stub
        # and return the controllable stub also, we need to propagate kill signals
        # we will be using the subprocess module to propagate the kill signal etc.
        return local_process.LocalProcess(session_id, framework_url)

    def _broker_update(self, dt, evt, event_writer, broker, process):
        pass


    def _create_broker(self):
        return

    def _create_event_writer(self):
        return events_log.writer()
