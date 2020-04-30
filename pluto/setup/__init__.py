from os import path

from pluto.interface.utils import paths
from pluto.finance.commission import models
from pluto.finance import slippage

from protos import setup_pb2

_PATH = paths.get_file_path('setup')
_SLIPPAGE_DIR = paths.get_dir('slippage', _PATH)

_SETUP = {}
_PARAMETERS_CACHE = {

}


def get_setup_path():
    return _PATH


class _Setup(object):
    def __init__(self, commissions, slippages):
        self._commission_models = commissions
        self._slippage_models = slippages

    def get_commission_models(self):
        return self._commission_models

    def get_slippage_models(self):
        return self._slippage_models


class _SlippageModels(object):
    def __init__(self, slippage_models):
        self._models = slippage_models

    def get_slippage_model(self, asset_type):
        return self._models[asset_type]


def _load_commissions(path):
    try:
        setup = _PARAMETERS_CACHE['commission']
    except KeyError:
        with open(path, 'rb') as f:
            setup = setup_pb2.CommissionsSetup()
            setup.ParseFromString(f.read())
    finally:
        return models.CommissionModels(setup.commissions)


def _load_slippages(universe=None):
    p = 'slippage'
    pth = path.join(p)
    try:
        setup = _PARAMETERS_CACHE[p]
    except KeyError:
        with open(path.join(pth, 'meta'), 'rb') as f:
            setup = setup_pb2.SlippageSetup()
            setup.ParseFromString(f.read())
            _PARAMETERS_CACHE[p] = setup
    finally:
        return _SlippageModels({
            stp.asset_type: slippage.get_slippage_model(
                stp,
                path.join(pth, stp.model),
                universe)
            for stp in setup.slippages
        })

def load_setup(universe=None):
    try:
        setup = _PARAMETERS_CACHE['setup']
    except KeyError:
        with open(_PATH, 'rb') as f:
            setup = setup_pb2.Setup()
            setup.ParseFromString(f.read())
            _PARAMETERS_CACHE['setup'] = setup
    finally:
        return _Setup(
            _load_commissions(setup.commissions_setup_file_name),
            _load_slippages(universe))

#for clearing the parameters cache if we need to reload everything without interrupting
#the process.
def reset():
    _PARAMETERS_CACHE.clear()
