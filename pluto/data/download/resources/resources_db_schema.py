import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from zipline.assets import asset_db_schema as sc

Base = declarative_base(metadata=sc.metadata)

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

class AssetType(Base):
    __tablename__ = 'asset_types'

    asset_class_name = sa.Column(sa.String, primary_key=True, nullable=False, unique=True)


class Source(Base):
    __tablename__ = 'sources'

    source_name = sa.Column(sa.String, primary_key=True, nullable=False, unique=True)
    exchange = sa.Column(sa.String, sa.ForeignKey(sc.exchanges.c.exchange), nullable=False)


# an asset class can have multiple data_types and vice-versa
class AssetClassDataType(Base):
    __tablename__ = 'asset_types-data_types'

    source_name = sa.Column(sa.String, sa.ForeignKey(Source.c.source_name), nullable=False)
    asset_class_name = sa.Column(sa.Integer, sa.ForeignKey(AssetType.c.asset_class_name), nullable=False)
    data_type_name = sa.Column(sa.Integer, sa.ForeignKey(DataType.c.data_type_name), nullable=False)

class SourceAsset(Base):
    __tablename__ = 'source_assets'

    source_name = sa.Column(sa.String, sa.ForeignKey(Source.c.source_name), nullable=False)
    sid = sa.Column(sa.Integer, primary_key=True, nullable=False, unique=True)
    asset_class_name = sa.Column(sa.String, sa.ForeignKey(AssetType.asset_class_name), nullable=False)

class AssetDataState(Base):
    __tablename__ = 'asset_data_states'

    sid = sa.Column(sa.Integer, sa.ForeignKey(SourceAsset.c.sid), nullable=False)
    data_type_name = sa.Column(sa.String, sa.ForeignKey(DataType.c.data_type_name), nullable=False)
    last_update = sa.Column(sa.DateTime)
    latest_data_point = sa.Column(sa.DateTime)

class AssetStatus(Base):
    __tablename__ = 'assets_status'

    sid = sa.Column(sa.Integer, sa.ForeignKey(SourceAsset.c.sid), nullable=False)
    asset_class_name = sa.Column(sa.String, sa.ForeignKey(AssetType.c.asset_class_name), nullable=False)
    status = sa.Column(sa.String, nullable=False)

class SourceAssetType(Base):
    __tablename__ = 'source-asset_types'

    source_name = sa.Column(sa.String, sa.ForeignKey(Source.c.source_name), nullable=False)
    asset_class_name = sa.Column(sa.String, sa.ForeignKey(AssetType.asset_class_name), nullable=False)

class SourceExchange(Base):
    __tablename__ = 'source-exchanges'

    source_name = sa.Column(sa.String, sa.ForeignKey(Source.c.source_name), nullable=False)
    exchange = sa.Column(sa.String, sa.ForeignKey(sc.exchanges.c.exchange), nullable=False)

class DataTypeSource(Base):
    __tablename__ = 'data_type-sources'

    source_name = sa.Column(sa.String, sa.ForeignKey(Source.c.source_name), nullable=False)
    data_type_name = sa.Column(sa.ForeignKey(DataType.c.data_type_name), nullable=False)