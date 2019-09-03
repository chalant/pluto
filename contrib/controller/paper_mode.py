from contrib.control.clock import clock

class PaperClockSignalRouter(clock.ClockSignalRouter):
    def __init__(self, live_clock_factory, start_date, end_date):
        super(PaperClockSignalRouter, self).__init__(start_date, end_date)
        self._cl_fct = live_clock_factory

    def _get_clock(self, exchange):
        return self._cl_fct.get_clock(exchange)

    def _get_listener(self, broker):
        return clock.DelimitedClockListener()

class PaperClockListener(clock.BaseClockListener):
    def _clock_update(self, request, sessions):
        #todo
        pass