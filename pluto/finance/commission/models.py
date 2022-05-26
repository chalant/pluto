class CommissionModels(object):
    def __init__(self, commissions_setup):
        self._models = commissions_setup

    def get_commission_model(self, asset_type, exchange):
        return self._models[asset_type][exchange]

    def __repr__(self):
        return repr(self._models)

    def __str__(self):
        return str(self._models)


def get_commission_model(model_type, asset_class):
    # todo: we load parameters from yaml files...
    #  parameters can be filled by the user, and overwrites the
    #  previous ones. There are default parameters as-well.

    pass
