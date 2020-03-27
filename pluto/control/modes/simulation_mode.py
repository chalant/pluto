from pluto.control.modes import mode
from pluto.control.events_log import events_log
from pluto.broker import broker


class SimulationControlMode(mode.ControlMode):
    def __init__(self,
                 framework_url,
                 capital,
                 max_leverage,
                 process_factory):
        self._capital = capital
        self._max_leverage = max_leverage

        super(SimulationControlMode, self).__init__(
            framework_url,
            process_factory)

    def _create_broker(self):
        return broker.SimulationBroker(self._capital, self._max_leverage)

    def _mode_type(self):
        return 'simulation'

    def _accept_loop(self, loop):
        # can accept any type of loop
        return True
