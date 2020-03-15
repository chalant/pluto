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

    def _broker_update(self, dt, evt, event_writer, broker, process):
        pass

    def _create_broker(self):
        return broker.SimulationBroker(self._capital, self._max_leverage)

    def _create_events_log(self):
        return events_log.get_events_log()

    def _mode_name(self):
        return 'simulation'
