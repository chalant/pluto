from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa

from contextlib import contextmanager

from contrib.interface.utils import db_utils as utils, paths

_CRD_FILE = 'credentials'

metadata = sa.MetaData()
engine = utils.create_engine(paths.get_file_path(_CRD_FILE), metadata)
Session = utils.get_session_maker(engine)

Base = declarative_base()

class Credentials(Base):
    __tablename__ = 'credentials-table'

    username = sa.Column(sa.String, primary_key=True, nullable=False)
    salt = sa.Column(sa.String, nullable=False)
    hash_ = sa.Column(sa.String, nullable=False)
    is_admin = sa.Column(sa.Boolean, nullable=False),
    is_local = sa.Column(sa.Boolean, nullable=False)

@contextmanager
def write():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollout()
        raise
    finally:
        session.close()

@contextmanager
def read():
    session = Session()
    try:
        yield session
    except:
        raise
    finally:
        session.close()
