import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session

from zipline.assets import asset_db_schema as sc

from abc import abstractmethod, ABC

Base = declarative_base(metadata=sc.metadata)

class RelatedSymbol(Base):
    ___tablename__ = 'related_symbols'

    sid = sa.Column(sa.Integer, sa.ForeignKey(sc.equities.c.sid))
    related_symbol = sa.Column(sa.String)

class UpdateFrequency(Base):
    __tablename__ = 'update_frequencies'

    frequency = sa.Column(sa.String, primary_key=True, nullable=False)

class DataType(Base):
    __tablename__ = 'data_types'

    data_type_name = sa.Column(sa.String, primary_key=True, nullable=False, unique=True)

class DataTypeFrequency(Base):
    __tablename__ = 'data_type-frequency'

    data_type_name = sa.Column(sa.String, sa.ForeignKey(DataType.c.data_type_name), nullable=False)
    frequency = sa.Column(sa.String, sa.ForeignKey(UpdateFrequency.c.frequency), nullable=False)

class AssetClass(Base):
    __tablename__ = 'asset_classes'

    asset_class_name = sa.Column(sa.String, primary_key=True, nullable=False, unique=True)

class AssetStatus(Base):
    __tablename__ = 'assets_status'

    sid = sa.Column(sa.Integer, primary_key=True, nullable=False)
    asset_class_name = sa.Column(sa.String, sa.ForeignKey(AssetClass.c.asset_class_name), nullable=False)
    status = sa.Column(sa.String, nullable=False)

#an asset class can have multiple data_types and vice-versa
class AssetClassDataType(Base):
    __tablename__ = 'asset_classes-data_types'

    asset_class_name = sa.Column(sa.Integer, sa.ForeignKey(AssetClass.c.asset_class_name), nullable=False)
    data_type_name = sa.Column(sa.Integer, sa.ForeignKey(DataType.c.data_type_name), nullable=False)

class Resource(Base):
    __tablename__ = 'resources'

    resource_id = sa.Column(sa.Integer, primary_key=True, unique=True, nullable=False)
    sid = sa.Column(sa.Integer, sa.ForeignKey(sc.asset_router.c.sid), nullable=False)
    data_type_name = sa.Column(sa.String, sa.ForeignKey(DataType.c.data_type_name), nullable=False)
    asset_class_name = sa.Column(sa.String, sa.ForeignKey(AssetClass.c.asset_class_name), nullable=False)
    last_update = sa.Column(sa.DateTime)
    latest_data_point = sa.Column(sa.DateTime)

class Source(Base):
    __tablename__ = 'sources'

    source_name = sa.Column(sa.String, primary_key=True, nullable=False, unique=True)

class ResourceSource(Base):
    __tablename__ = 'resources-source'

    source_name = sa.Column(sa.String, sa.ForeignKey(Source.c.source_name), nullable=False)
    resource_id = sa.Column(sa.Integer, sa.ForeignKey(Resource.c.source_id), nullable=False)


class ResourceQuery(ABC):
    def query(self):
        '''

        Returns
        -------
        sqlalchemy.orm.Query
        '''
        return self._query()

    @abstractmethod
    def _query(self):
        raise NotImplementedError


class Sources(ResourceQuery):
    def __init__(self, session):
        '''

        Parameters
        ----------
        session : session.Session
        source_name : str
        '''
        self._session = session

    def _query(self):
        return self._session.query(Source).join(Resource.resource_id)


class Difference(ResourceQuery):
    def __init__(self, left, right):
        super(Difference, self).__init__()
        self._left = left
        self._right = right

    def _query(self):
        return self._left.query().except_(self._right.query())


class Union(ResourceQuery):
    def __init__(self, left, right):
        self._left = left
        self._right = right

    def _query(self):
        return self._left.query().union(self._right.query())


class ResourceQueryBuilder(object):
    def __init__(self, session):
        '''

        Parameters
        ----------
        engine : sqlalchemy.orm.session.Session
        '''
        self._session = session

    def query_from_data_type(self, data_type):
        '''

        Parameters
        ----------
        data_type

        Returns
        -------

        '''

    def get_source(self, source_name):
        '''

        Parameters
        ----------
        source : str

        Returns
        -------
        ResourceQuery

        '''
        return Sources(self._session)

    def difference(self, left, right):
        '''

        Parameters
        ----------
        left : ResourceQuery
        right : ResourceQuery

        Returns
        -------
        ResourceQuery

        '''

    def union(self, left, right):
        '''

        Parameters
        ----------
        left : ResourceQuery
        right :ResourceQuery

        Returns
        -------
        ResourceQuery

        '''

    def get_resources(self, resource_query, row_buffer_size=1000):
        '''

        Parameters
        ----------
        resource_query : ResourceQuery

        Returns
        -------
        typing.Generator[Resource, None, None]

        '''
        return resource_query.query().yield_per(row_buffer_size).enable_eager(False)


class ResourceWriter(object):
    def __init__(self, engine):
        '''
        Parameters
        ----------
        engine : sqlalchemy.engine.Engine

        '''

        self._engine = engine

    def write(self, resources, resource_source_mappings):
        '''

        Parameters
        ----------
        resources : typing.Dict
        resource_source_mappings : typing.

        Returns
        -------

        '''
        pass
