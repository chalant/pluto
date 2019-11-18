import sqlalchemy as sa

from sqlalchemy import orm

from contrib.interface.utils import paths

def create_engine(file_name, metadata, directory=None):
    db_uri = 'sqlite:///{}'
    file_name = file_name + '.sqlite'
    try:
        path = paths.get_file_path(file_name, directory)
        db_uri = db_uri.format(path)
        return sa.create_engine(db_uri)
    except FileNotFoundError:
        path = paths.create_file(file_name, directory)
        db_uri = db_uri.format(path)
        engine = sa.create_engine(db_uri)
        metadata.create_all(engine)
        return engine

def get_session_maker(engine):
    return orm.sessionmaker(engine)