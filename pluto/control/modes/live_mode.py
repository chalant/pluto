from pluto.control.modes import mode

class LiveControlMode(mode.ControlMode):
    def __init__(self):
        super(LiveControlMode, self).__init__()

    def name(self):
        return 'live'

    def _create_trader(self, broker_address):
        pass

    def _get_clock(self, exchange):
        pass
