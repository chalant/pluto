from os import path

from sqlalchemy import sql

from zipline.assets import Equity, Future
from zipline.finance import commission

from pluto.interface.utils import paths
from pluto.interface.utils import db_utils
from pluto.finance.commission import models
from pluto.finance.commission import schema
from pluto.finance.slippage import setup as slp_stp

from protos import setup_pb2

_PATH = None
# _SLIPPAGE_DIR = paths.get_dir('slippage', _PATH)

_SETUP = {}
_PARAMETERS_CACHE = {

}

_ENGINE = None
_COMMISSIONS = None

def setup_directory():
    global _ENGINE
    global _PATH
    global _COMMISSIONS

    _PATH = paths.get_dir('setup')
    _COMMISSIONS = paths.get_dir('commission', _PATH)
    models = paths.get_file_path('models', _COMMISSIONS)
    _ENGINE = db_utils.create_engine(models)
    schema.metadata.create_all(_ENGINE)

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

_asset_types = {
    'equity': Equity,
    'future': Future
}

def _get_commission_model(broker, model_type, engine):
    with engine.connect() as conn:
        table = schema.get_parameter_table(model_type)
        row = conn.execute(sql.select([table]).where(
            table.c.broker == broker
        )).fetchone()
        if not row:
            raise ValueError(
                'No {} commission model was found for {}'.format(
                    model_type,
                    broker
                ))
        if model_type == 'per_share':
            return commission.PerShare(
                row['cost_per_share'],
                row['min_trade_cost']
            )
        elif model_type == 'per_dollar':
            return commission.PerDollar(
                row['cost_per_dollar']
            )
        elif model_type == 'per_contract':
            return commission.PerContract(
                row['cost'],
                row['exchange_fee']
            )
        elif model_type == 'per_trade':
            return commission.PerTrade(
                row['cost'])
        elif model_type == 'per_future_trade':
            return commission.PerFutureTrade(
                row['cost'])
        else:
            raise ValueError(
                'commission model {} is not supported'.format(
                    model_type))

def _load_commissions(path):
    '''
            protobuf file that looks like:
            [
                {
                    asset_type: equity
                    exchange: NYSE,
                    broker: IB,
                    model_type: per_share
                    },
                {
                    asset_type: equity
                    exchange: EQUITY_EXCHANGE1
                    broker: BROKER1,
                    model_type: COMMISSION_MODEL1
                    },
                {
                    asset_type: future,
                    exchange: FUTURE_EXCHANGE1
                    broker: BROKER1,
                    model: COMMISSION_MODEL1
                    },
                {
                    asset_type: future,
                    exchange: FUTURE_EXCHANGE2
                    broker: BROKER2,
                    model: COMMISSION_MODEL2
                    }
            ]
            '''

    try:
        setup = _PARAMETERS_CACHE['commission']
        return models.CommissionModels(setup)
    except KeyError:
        with open(path, 'rb') as f:
            setup = setup_pb2.CommissionsSetup()
            setup.ParseFromString(f.read())
            mds = {}
            for stp in setup.commissions:
                d = {}
                d[stp.exchange] = _get_commission_model(
                    stp.broker,
                    stp.model_type,
                    _ENGINE
                )
                mds[_asset_types[stp.asset_type]] = d
            _PARAMETERS_CACHE['commission'] = mds
            return models.CommissionModels(mds)


def _load_slippages(universe=None):
    p = 'slippage'
    pth = paths.get_dir(p, _PATH)
    try:
        setup = _PARAMETERS_CACHE[p]
        return _SlippageModels({
            _asset_types[stp.asset_type]: slp_stp.get_slippage_model(
                stp,
                path.join(pth, stp.model),
                universe)
            for stp in setup.slippages
        })
    except KeyError:
        with open(path.join(pth, 'meta'), 'rb') as f:
            setup = setup_pb2.SlippageSetup()
            setup.ParseFromString(f.read())
            _PARAMETERS_CACHE[p] = setup
            return _SlippageModels({
                _asset_types[slp.asset_type]: slp_stp.get_slippage_model(
                    slp,
                    path.join(pth, slp.model),
                    universe)
                for slp in setup.slippages
            })

def load_setup(universe=None):
    try:
        setup = _PARAMETERS_CACHE['setup']
        return _Setup(
            _load_commissions(setup.commissions_setup_file_name),
            _load_slippages(universe))
    except KeyError:
        with open(paths.get_file_path('setup_file', _PATH), 'rb') as f:
            setup = setup_pb2.Setup()
            setup.ParseFromString(f.read())
            _PARAMETERS_CACHE['setup'] = setup
            return _Setup(
                _load_commissions(paths.get_file_path(
                    setup.commissions_setup_file_name, _COMMISSIONS)),
                _load_slippages(universe))

#for clearing the parameters cache if we need to reload everything without interrupting
#the process.
def reset():
    _PARAMETERS_CACHE.clear()
