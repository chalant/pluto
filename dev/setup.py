class BackTestingSetup(object):
    def __init__(self):
        self.capital = 100000.0

        #leverage is determined by the account
        self.leverage = 2.0
        self.data_frequency = 'day'

        #maximum history data from the most recent data point
        self.look_back = 150
        self.cancel_policy = 'never_cancel'
        self.trading_universe = 'us_equities'

    def get_universes(self):
        pass

    def get_data_frequency(self):
        pass

    def get_cancel_policies(self):
        pass