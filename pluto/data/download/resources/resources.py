from pluto.data.download.resources import resources_db_schema as rds

class AssetsQuery(object):
    def __init__(self, session, source_name, status, exchange, frequency):
        self._query = session.query(rds.Source) \
            .filter(rds.Source.source_name == source_name) \
            .join(rds.SourceExchange.source_name) \
            .filter(rds.Source.exchange == exchange) \
            .join(rds.SourceAsset.source_name) \
            .join(rds.AssetStatus.sid) \
            .join(rds.AssetStatus.status == status) \
            .join(rds.AssetClassDataType.source_name) \
            .join(rds.DataTypeFrequency.data_type_name) \
            .filter(rds.DataTypeFrequency.frequency == frequency) \
            .join(rds.AssetDataState.sid)

    def get_query(self):
        return self._query

    def results(self, row_buffer_size=1000):
        return self._query.yield_per(row_buffer_size).enable_eager(False)

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
        resources : typing.List[typing.Dict]
        resource_source_mappings : typing.

        Returns
        -------

        '''
        pass

class Resources(object):
    def __init__(self, session):
        '''

        Parameters
        ----------
        session : sqlalchemy.orm.session.Session
        '''

        self._session = session

    def get_assets_query(self, source_name, status, exchange, frequency):
        return AssetsQuery(self._session, source_name, status, exchange, frequency)

    def get_difference_query(self, left_query, right_query):
        '''

        Parameters
        ----------
        left_query : sqlalchemy.orm.Query
        right_query : sqlalchemy.orm.Query

        Returns
        -------
        sqlalchemy.orm.Query
        '''
        return left_query.get_query().except_(right_query.get_query())

    def results(self, query, row_buffer_size=1000):
        '''

        Parameters
        ----------
        query : sqlalchemy.orm.Query

        Returns
        -------
        typing.Generator[rds.Resource, None, None]

        '''
        return query.yield_per(row_buffer_size).enable_eager(False)



