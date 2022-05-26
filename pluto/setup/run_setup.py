import yaml

from zipline.finance import commission
from zipline.finance import slippage

from zipline.finance.commission import (
    DEFAULT_PER_CONTRACT_COST,
    FUTURE_EXCHANGE_FEES_BY_SYMBOL)

from zipline.finance.slippage import (
    DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT
)


class RunSetup(object):
    def __init__(self, session_id):
        self.start = None
        self.end = None

        self.broker = ""
        self.capital = ""
        self.leverage = ""
        self.trading_universe = ""
        self.data_frequency = ""

        self.commission_models = {
            'equity': commission.PerShare(),
            'futures': commission.PerContract(
                cost=DEFAULT_PER_CONTRACT_COST,
                exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
            )}

        self.slippage_models = {
            'equity': slippage.FixedBasisPointsSlippage(),
            'futures': slippage.VolatilityVolumeShare(
                volume_limit=DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT)
        }

        self.tax_models = {}

        self.session_id = session_id
        self.clear_perf = False


def load_run_setup(dir_):
    """

    Parameters
    ----------
    dir_: directory.directory.Directory

    Returns
    -------

    """
    # todo: load setup

    setup_dir = dir_.get_directory('setup')

    with open(setup_dir.get_file("setup", "yaml").path) as f:
        stp = yaml.load(f, Loader=yaml.FullLoader)

    with open(setup_dir.get_file("commission_models", "yaml").path) as f:
        # todo: load and create commission models
        cm = yaml.load(f, Loader=yaml.FullLoader)

    run_setup = RunSetup()

    return run_setup





