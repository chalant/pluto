from sqlalchemy import (
    Table,
    MetaData,
    Column,
    ForeignKey,
    String)

meta = MetaData()

bundles = Table(
    'bundles',
    Column('bundle_name'),
    Column('calendar_name', ForeignKey),
    Column('start_session'),
    Column('end_session'),
    Column('minutes_per_day')
)

calendars = Table(
    'calendars',
    Column('calendar_name')
)


bundles_calendars = Table(
    'bundles_calendars',
    Column('calendar_name', ForeignKey())
)



