from pluto.control.clock import clock

class PaperClockSignal(clock.SignalFilter):
    pass

class PaperClockListener(clock.BaseClockListener):
    def _clock_update(self, request, sessions):
        #todo
        pass
