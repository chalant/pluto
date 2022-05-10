from sqlalchemy import text

_GET_STRATEGY_SETUP = text(
    """
    SELECT * FROM strategy_setup
    WHERE strategy_id=:strategy_id
    """)

_SAVE_STRATEGY_SETUP = text(
    """
    INSERT INTO strategies
    VALUES 
    """
)


class StrategySetup(object):
    def __init__(self):
        self.look_back = 150
        self.cancel_policy = 'never cancel'
        self.leverage = True
        self.shorting = True
        self.asset_classes = 'equity'
        self.data_frequency = 'daily'


def get_strategy_setup(connection, strategy_id):
    stp = StrategySetup()

    res = connection.execute(
        strategy_id=strategy_id)

    if res:
        stp.look_back = res['look_back']
        stp.leverage = res['leverage']
        stp.shorting = res['shorting']

    return stp


def save_strategy_setup(connection, strategy_setup):
    connection.execute(
        _SAVE_STRATEGY_SETUP,
        look_back=strategy_setup.look_back,
        leverage=strategy_setup.leverage,
        shorting=strategy_setup.shorting
    )
