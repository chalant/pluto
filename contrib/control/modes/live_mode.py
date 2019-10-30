from contrib.control.modes import mode

class LiveControlMode(mode.ControlMode):
    def __init__(self):
        self._clocks = {}

    def name(self):
        return 'live'

    def _create_trader(self, broker_address):
        pass

    def _get_clock(self, exchange):
        pass
