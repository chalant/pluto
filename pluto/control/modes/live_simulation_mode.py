from pluto.control.modes import mode
from pluto.broker import broker


class LiveSimulationMode(mode.ControlMode):
    def __init__(self,
                 server,
                 capital,
                 max_leverage,
                 process_factory,
                 market_factory):
        self._capital = capital
        self._max_leverage = max_leverage
        self._market_factory = market_factory

        super(LiveSimulationMode, self).__init__(
            server,
            process_factory)

    def _create_broker(self):
        #todo: this object should be a BrokerServicer sub-class
        return broker.LiveSimulationBroker(
            self._capital,
            self._max_leverage,
            self._market_factory)

    def mode_type(self):
        return 'live'

    def _accept_loop(self, loop):
        # can take any type of loop
        return True
