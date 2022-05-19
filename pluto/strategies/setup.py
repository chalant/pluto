from sqlalchemy import text

_GET_STRATEGY_SETUP = text(
    """
    SELECT * FROM strategy_setup
    WHERE strategy_id=:strategy_id
    """)

_SAVE_STRATEGY_SETUP = text(
    """
    INSERT OR REPLACE INTO strategies
    VALUES 
    """)

_SAVE_STRATEGY_ASSET_CLASS = text(
    """
    INSERT OR REPLACE INTO strategy_asset_classes
    VALUES
    """
)


class StrategySetup(object):
    def __init__(self, strategy_id):
        self.__strategy_id = strategy_id

        self.look_back = 150
        self.cancel_policy = 'never cancel'
        self.shorting = True
        self.asset_classes = 'equity'
        self.data_frequency = 'daily'
        self.fractional = False

    @property
    def strategy_id(self):
        return self.__strategy_id


def get_strategy_setup(connection, strategy_id):
    stp = StrategySetup(strategy_id)

    res = connection.execute(
        strategy_id=strategy_id)

    if res:
        stp.look_back = res['look_back']
        stp.shorting = res['shorting']

    return stp


def save_strategy_setup(connection, strategy_setup):
    connection.execute(
        _SAVE_STRATEGY_SETUP,
        look_back=strategy_setup.look_back,
        shorting=strategy_setup.shorting,
        data_frequency=strategy_setup.data_frequency
    )

    for asset_class in strategy_setup.asset_classes.split(","):
        connection.execute(
            _SAVE_STRATEGY_ASSET_CLASS,
            asset_class=asset_class,
            strategy_id=strategy_setup.strategy_id
        )
