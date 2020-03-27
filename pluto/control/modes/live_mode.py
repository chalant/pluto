from pluto.control.modes import mode

class LiveControlMode(mode.ControlMode):
    def __init__(self, framework_url):
        super(LiveControlMode, self).__init__(framework_url, )

    def _accept_loop(self, loop):
        # todo: only accept LiveLoop type or subtypes
        return False

