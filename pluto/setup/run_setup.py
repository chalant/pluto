import yaml

from pluto.setup import directory_setup as ds


class RunSetup(object):
    def __init__(self):
        self.broker = ""
        self.capital = ""
        self.leverage = ""
        self.trading_universe = ""
        self.data_frequency = ""
        self.equity_commission_model = ""
        self.futures_commission_model = ""
        self.equity_tax_model = ""
        self.futures_tax_model = ""


def load_run_setup():
    # todo: load setup

    with open(ds.setup_dir.get_file("setup", "yaml").path) as f:
        stp = yaml.load(f, Loader=yaml.FullLoader)

    with open(ds.setup_dir.get_file("commission_models", "yaml").path) as f:
        # todo: load and create commission models
        cm = yaml.load(f, Loader=yaml.FullLoader)

    run_setup = RunSetup()

    return run_setup





