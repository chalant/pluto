from sqlalchemy import (
    Table,
    Column,
    String,
    Integer,
    Boolean,
    MetaData
)


meta = MetaData()

universes = Table(
    'universes',
    meta,
    Column('universe_id'),
    Column('universe_name')
)

universe_exchanges = Table(
    'exchanges',
    meta,
    Column('universe_id'),
    Column('exchange_name')
)