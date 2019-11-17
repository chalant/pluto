import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

metadata = sa.MetaData()

Base = declarative_base(metadata=metadata)

class StrategyMetadata(Base):
    __tablename__ = 'directory_metadata'

    id = sa.Column(sa.String, primary_key=True, nullable=False)
    name = sa.Column(sa.String)
    directory_path = sa.Column(sa.String)
    locked = sa.Column(sa.Boolean)