import yaml

import directory_setup as ds


class EngineSetup(object):
    def __init__(self):
        self.commission_model = ""
        self.capital = ""
        self.tax_model = ""
        self.trading_universe = ""


def load_engine_setup(
        commission_models,
        tax_models,
        universe_name,
        capital):

    # todo: load setup
    commission_models.equity
    tax_models.equity

    with open(ds.universes_dir.get_file("universes", "yaml").path) as f:
        uni = yaml.load(f, Loader=yaml.FullLoader)

    with open(ds.setup_dir.get_file("tax_models", "yaml").path) as f:
        tm = yaml.load(f, Loader=yaml.FullLoader)

    with open(ds.setup_dir.get_file("commission_models", "yaml").path) as f:
        # todo: load and create commission models
        cm = yaml.load(f, Loader=yaml.FullLoader)

    stp = EngineSetup()
    stp.capital = capital

    # todo: set models

    return stp
