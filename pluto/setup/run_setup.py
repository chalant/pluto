import yaml


class RunSetup(object):
    def __init__(self, session_id):
        self.broker = ""
        self.capital = ""
        self.leverage = ""
        self.trading_universe = ""
        self.data_frequency = ""
        self.equity_commission_model = ""
        self.futures_commission_model = ""
        self.equity_tax_model = ""
        self.futures_tax_model = ""
        self.session_id = session_id


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





