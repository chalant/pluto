class BackTestSetup(object):
    def __init__(self):
        self.start_dt = None
        self.end_dt = None

    def set_start_dt(self, dt):
        pass

    def set_end_dt(self, dt):
        #note: backtest should end such that we still have some out
        # of sample data...
        pass