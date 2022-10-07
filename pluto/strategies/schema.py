from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Boolean,
    ForeignKey)

meta = MetaData()

strategies = Table(
    'strategies',
    meta,
    Column('strategy_id', String, primary_key=True),
    Column('strategy_name', String, nullable=False),
    Column('status', String, nullable=False)
)

sessions = Table(
    'sessions',
    meta,
    Column("session_id", String, primary_key=True),
    Column("universe_name", String, nullable=False),
    Column("data_frequency", String, nullable=False)
)

strategy_sessions = Table(
    'strategy_sessions',
    meta,
    Column('ssid', String, primary_key=True),
    Column('strategy_id', String, ForeignKey('strategies.strategy_id'), nullable=False),
    Column('session_id', String, ForeignKey('sessions.session_id'), nullable=False),
    Column('status', String, nullable=False)
)

strategy_setup = Table(
    'strategy_setup',
    meta,
    Column('strategy_id', String, ForeignKey('strategies.strategy_id'), primary_key=True),
    Column('look_back', Integer, nullable=False),
    Column('shorting', Boolean, nullable=False)
)

strategy_assets = Table(
    'strategy_asset_classes',
    meta,
    Column('asset_class', String, nullable=False),
    Column('strategy_id', String, ForeignKey('strategies.strategy_id'))
)


