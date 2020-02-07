import sqlalchemy as sa

from sqlalchemy import orm

def create_engine(file_path):
    '''

    Parameters
    ----------
    file_path:str

    Returns
    -------
    sqlalchemy.engine.Engine
    '''
    db_uri = 'sqlite:///{}?check_same_thread=False'
    db_uri = db_uri.format(file_path)
    return sa.create_engine(db_uri)


def get_session_maker(engine):
    return orm.sessionmaker(bind=engine)