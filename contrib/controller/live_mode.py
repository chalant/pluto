from contrib.controller import controller

class LiveControlMode(controller.ControlMode):
    def __init__(self):
        self._clocks = {}

    def name(self):
        return 'live'

    def _get_controllable(self):
        pass

    def _get_clock(self, exchange):
        pass
