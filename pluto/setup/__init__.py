from os import path

from pluto.interface.utils import paths
from pluto.finance.commission import models
from pluto.finance import slippage

from protos import setup_pb2

_PATH = paths.get_file_path('setup')
_SLIPPAGE_DIR = paths.get_dir('slippage', _PATH)

_SETUP = {}

def get_setup_path():
    return _PATH

class _Setup(object):
    def __init__(self, setup_dict):
        self._commission_models = setup_dict['COMMISSIONS']
        self._slippage_models = setup_dict['SLIPPAGE']

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
    with open(path, 'rb') as f:
        setup = setup_pb2.CommissionsSetup()
        setup.ParseFromString(f.read())
        return models.CommissionModels(setup.commissions)


def _load_slippage(universe):
    pth = path.join('slippage')
    with open(path.join(pth, 'meta'), 'rb') as f:
        setup = setup_pb2.SlippageSetup()
        setup.ParseFromString(f.read())
        return _SlippageModels({
            stp.asset_type: slippage.get_slippage_model(
                stp,
                path.join(pth, stp.model),
                universe)
            for stp in setup.slippages
        })


def load_setup(universe):
    if not _SETUP:
        with open(_PATH, 'rb') as f:
            setup = setup_pb2.Setup()
            setup.ParseFromString(f.read())
            _SETUP['COMMISSIONS'] = _load_commissions(
                setup.commissions_setup_file_name)
            _SETUP['SLIPPAGE'] = _load_slippage(universe)
    return _Setup(_SETUP)
