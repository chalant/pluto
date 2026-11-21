from zipline.finance import cancel_policy


class StrategySetup(object):
    def __init__(self, strategy_id):
        self.__strategy_id = strategy_id

        self.look_back = 150
        self.cancel_policy = 'never cancel'
        self.shorting = True
        self.asset_classes = 'equity'
        self.data_frequency = 'daily'
        self.fractional = False
        self.cancel_policy = cancel_policy.NeverCancel()

    @property
    def strategy_id(self):
        return self.__strategy_id
