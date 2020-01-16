import sqlalchemy as sa

metadata = sa.MetaData()

exchanges = sa.Table(
    'exchanges',
    metadata,
    sa.Column(
        'exchange',
        sa.String,
        primary_key=True,
        nullable=False,
        unique=True)
)

universes = sa.Table(
    'universes',
    sa.Column(
        'universe',
        sa.String,
        primary_key=True,
        nullable=False,
        unique=True),
    sa.Column(
        'directory',
        sa.String,
        nullable=False,
        unique=True)
)

calendars = sa.Table(
    'calendars',
    metadata,
    sa.Column(
        'calendar_name',
        sa.String,
        primary_key=True,
        nullable=False,
        unique=True),
    sa.Column(
        'file_path',
        sa.String,
        nullable=False,
        unique=True)
)

universe_exchanges = sa.Table(
    'universe_exchanges',
    metadata,
    sa.ForeignKey(exchanges.c.exchange),
    sa.ForeignKey(universes.c.universe)
)

calendar_exchanges = sa.Table(
    'calendar_exchanges',
    metadata,
    sa.ForeignKey(exchanges.c.exchange),
    sa.ForeignKey(calendars.c.calendar_name)
)