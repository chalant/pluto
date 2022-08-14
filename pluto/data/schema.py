from sqlalchemy import (
    Table,
    MetaData,
    Column,
    ForeignKey,
    String)

meta = MetaData()

data = Table(
    'data',
    Column('data_id', String, primary_key=True),
    Column('bundle_name', ForeignKey('bundles.bundle_name'), nullable=False)
)

bundles = Table(
    'bundles',
    Column('bundle_name', String, primary_key=True),
    Column('calendar_name', String, unique=True),
    Column('first_trading_day', String),
    Column('last_available_dt', String))

# universes = Table(
#     'universes',
#     Column('universe_name'),
#     Column('composite_calendar_name', ForeignKey('composite_calendar.name'))
# )
#
#
# calendars = Table(
#     'calendars',
#     Column('calendar_name', String, primary_key=True),
#     Column('exchange', ForeignKey('exchanges.exchange'))
# )
#
# composite_calendars = Table(
#     'composite_calendars',
#     Column('name', String, primary_key=True),
#     Column('calendar_name', ForeignKey('calendars.calendar_name'))
# )




