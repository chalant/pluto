import sqlalchemy as sa

metadata = sa.MetaData()

events = sa.Table(
    'events',
    metadata,
    sa.Column(
        'file_path',
        sa.String),
    sa.Column(
        'session',
        sa.DateTime,
        primary_key=True,
        nullable=False)
)

datetimes = sa.Table(
    'datetimes',
    metadata,
    sa.Column(
        'datetime',
        sa.DateTime,
        nullable=False
    ),
    sa.Column(
        'session',
        sa.ForeignKey(events.c.session),
        nullable=False
    )
)

