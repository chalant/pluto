class CommissionModels(object):
    def __init__(self, commissions_setup):
        self._models = commissions_setup

    def get_commission_model(self, asset_type, exchange):
        return self._models[asset_type][exchange]

    def __repr__(self):
        return repr(self._models)

    def __str__(self):
        return str(self._models)
