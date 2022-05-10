class SlippageModel(object):
    pass


class CommissionModel(object):
    pass


class EngineSetup(object):
    def __init__(self):
        self.slippage_model = ''
        self.commission_model = ''

    def set_slippage_model(self, model):
        pass

    def set_commission_model(self, model):
        pass

    def set_cancel_policy(self, policy):
        pass

    def set_leverage_handler(self, handler):
        # by default, raises an error if the leverage exceeds some
        # threshold...
        pass