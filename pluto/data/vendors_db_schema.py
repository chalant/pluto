import sqlalchemy as sa

metadata = sa.MetaData()


data_frequencies = sa.Table(
    'data_frequencies',
    metadata,
    sa.Column(
        'data_frequency',
        sa.Text,
        unique=True,
        nullable=True,
        primary_key=True)
)

asset_types = sa.Table(
    'asset_types',
    sa.Column(
        'asset_type',
        sa.Text,
        unique=True,
        nullable=False,
        primary_key=True)
)

exchanges = sa.Table(
    'exchanges',
    metadata,
    sa.Column(
        'exchange',
        sa.Text,
        unique=True,
        nullable=False,
        primary_key=True,
    ),
    sa.Column('canonical_name', sa.Text, nullable=False),
    sa.Column('country_code', sa.Text, nullable=False),
    sa.Column('currency', sa.Text, nullable=False)
)

vendors = sa.Table(
    'vendors',
    sa.Column(
            'asset_type',
            sa.Text,
            unique=True,
            nullable=False,
            primary_key=True),

)