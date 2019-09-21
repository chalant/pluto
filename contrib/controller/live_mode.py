from contrib.controller import controller

class LiveControlMode(controller.ControlMode):
    def __init__(self):
        self._clocks = {}

    def name(self):
        return 'live'

    def _get_trader(self, capital, max_leverage, broker_address):
        pass

    def _get_clock(self, exchange):
        pass
