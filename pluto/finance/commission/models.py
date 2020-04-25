from sqlalchemy import sql

from zipline.finance import commission
from zipline.assets import Equity, Future

from pluto.finance.commission import schema
from pluto.interface.utils import (
    db_utils,
    paths
)
from pluto import setup

_DIR = paths.get_dir(
    'commissions',
    setup.get_setup_path()
)

_MODELS = paths.get_file_path('models', _DIR)

_ENGINE = db_utils.create_engine(_MODELS)
schema.metadata.create_all(_ENGINE)


def _get_commission_model(broker, model_type):
    with _ENGINE.connect() as conn:
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


_asset_types = {
    'equity': Equity,
    'future': Future
}


class CommissionModels(object):
    def __init__(self, commissions_setup):
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

        self._models = models = {}
        for setup in commissions_setup:
            models[_asset_types[setup.asset_type]][setup.exchange] = \
                _get_commission_model(
                    setup.broker,
                    setup.model_type)

    def get_commission_model(self, asset_type, exchange):
        return self._models[asset_type][exchange]

    def __repr__(self):
        return repr(self._models)

    def __str__(self):
        return str(self._models)
