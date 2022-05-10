import hashlib
import os

from sqlalchemy import text

from pluto.strategies import sessions

from pluto.universes import universes

_ADD_STRATEGY = text(
    """
    INSERT INTO strategies
    VALUES ()
    """
)

_ADD_STRATEGY_SESSION = text(
    """
    INSERT OR IGNORE INTO strategy_sessions
    VALUES ()
    """
)

_GET_STRATEGY = text(
    """
    SELECT * FROM strategies
    WHERE strategy_name=:strategy_name
    """
)

_GET_STRATEGIES = text(
    """
    SELECT * FROM strategies
    """
)

_GET_STRATEGY_SESSION = text(
    """
    SELECT * FROM strategy_sessions
    WHERE ssid=:ssid
    """
)

_GET_STRATEGY_SESSIONS = text(
    """
    SELECT * FROM strategy_sessions
    """
)

_SET_STRATEGY_STATUS = text(
    """
    UPDATE
    """
)


class StrategyCreator(object):
    def __init__(self, root, template_path):
        self._root = root
        self._template_path = template_path

    def create_strategy_file(self, strategy_name):
        with open(os.path.join(self._root, strategy_name) + ".py", 'w+') as f:

            # copy template into the new file...

            with open(self._template_path) as tp:
                f.writelines(tp.readline())


class Strategy(object):
    __slots__ = ['strategy_id', 'strategy_name', 'status']

    def __init__(self, strategy_name, status):
        self.strategy_id = None
        self.strategy_name = strategy_name
        self.status = status

    def __hash__(self):
        return self.strategy_id


class StrategySession(object):
    __slots__ = ['id_', 'strategy', 'session', 'status']

    def __init__(self, strategy, session, status):
        self.id_ = None
        self.strategy = strategy
        self.session = session
        self.status = status

    def __hash__(self):
        return self.id_


def _save_strategy_session(connection, strategy_session):
    """

    Parameters
    ----------
    connection
    strategy_session: StrategySession

    Returns
    -------

    """

    strategy = strategy_session.strategy
    session = strategy_session.session

    connection.execute(
        _ADD_STRATEGY,
        strategy_id=strategy.strategy_id,
        strategy_name=strategy.strategy_name
    )

    connection.execute(
        _ADD_STRATEGY_SESSION,
        ssid=strategy_session.id_,
        strategy_id=strategy.strategy_id,
        session_id=session.session_id,
        status=strategy_session.status)


def create_strategy(connection, strategy_creator, strategy_name):
    # todo: add file with the corresponding name if
    #  it already exists, raise an error.

    strategy = Strategy(strategy_name, 'development')

    if connection.execute(_GET_STRATEGY, strategy_name=strategy.strategy_name).count() == 0:

        connection.execute(
            _ADD_STRATEGY,
            strategy_id=strategy.strategy_id,
            strategy_name=strategy.strategy_name,
            status=strategy.status)

        strategy_creator.create_strategy_file(strategy_name)

    else:
        raise FileExistsError("Strategy of name {} already exists".format(strategy_name))

    strategy.strategy_id = hashlib.md5(strategy.strategy_name.encode()).hexdigest()

    return strategy


def get_strategies(connection):
    for res in connection.execute(_GET_STRATEGIES):
        yield Strategy(res['strategy_name'], res['status'])


def get_strategy(connection, strategy_name):
    res = connection.execute(
        _GET_STRATEGY,
        strategy_name=strategy_name
    )

    strategy = Strategy(strategy_name, res['status'])

    strategy.strategy_id = res['strategy_id']

    return strategy


def deploy_strategy(connection, strategy_session):
    # todo: make a copy of the strategy and put it in another folder to avoid
    #  corruption..

    # note: if the strategy fails to run in live for some reason, put it in the
    # failing folder and additional fail information...

    # todo: set the strategy status to deployed. Once the strategy has been deployed
    #  no modifications can be made.

    strategy = strategy_session.strategy

    connection.execute(
        _SET_STRATEGY_STATUS,
        status='deployed',
        strategy_id=strategy.strategy_id
    )


def create_strategy_session(connection, strategy, session):
    stg_sess = StrategySession(strategy, session, 'inactive')

    ssid = hashlib.md5(str(strategy.strategy.strategy_id + session.session_id).encode()).hexdigest()

    stg_sess.id_ = ssid

    _save_strategy_session(
        connection,
        stg_sess
    )


def get_strategy_session(connection, strategy_id, session_id):
    # loads a strategy session

    # create strategy session if it doesn't exist...

    res = connection.execute(
        _GET_STRATEGY_SESSION,
        strategy_id=strategy_id,
        session_id=session_id
    )

    if res:
        session = sessions.Session(
            universes.get_universe(res['universe_name']),
            res['data_frequency']
        )

        stg_sess = StrategySession(
            Strategy(res['strategy_name'], res['status']),
            session,
            res['status'])
    else:
        raise FileNotFoundError('No strategy session for strategy {}'.format(strategy.strategy_name))

    return stg_sess


