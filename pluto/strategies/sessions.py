import hashlib

from sqlalchemy import text

_ADD_SESSION = text(
    """
    INSERT OR IGNORE INTO sessions
    VALUES ()
    """
)

_GET_SESSION = text(
    """
    SELECT * FROM sessions
    WHERE universe_name=:universe_name 
        AND data_frequency=:data_frequency
    """
)


class Session(object):
    __slots__ = ['session_id', 'universe', 'data_frequency']

    def __init__(self, universe, data_frequency):
        self.session_id = hashlib.md5(str(universe.name + data_frequency).encode()).hexdigest()
        self.universe = universe
        self.data_frequency = data_frequency

    def __hash__(self):
        return self.session_id


def create_session(universe, data_frequency):
    return Session(universe, data_frequency)


def get_session(universe, data_frequency):
    return


def save_session(connection, session):
    universe = session.universe

    connection.execute(
        _ADD_SESSION,
        session_id=hashlib.md5(str(universe.name + session.data_frequency).encode()).hexdigest(),
        universe_name=universe.name,
        data_frequency=session.data_frequency)


