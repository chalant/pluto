from sqlalchemy import (
    Table,
    Column,
    String,
    Float,
    ForeignKey,
)


def create_tables(engine, meta):
    Table(
        'exchanges',
        meta,
        Column('exchange', String, primary_key=True),
        Column('country_code', String, nullable=False)
    )

    Table(
        'assets',
        meta,
        Column('asset_class', String, primary_key=True)
    )

    Table(
        'data_frequency',
        meta,
        Column('frequency', String, primary_key=True)
    )

    Table(
        'data_providers',
        meta,
        Column('provider_name', String, primary_key=True)
    )

    Table(
        'data_providers_assets',
        meta,
        Column('asset_class', String, ForeignKey('assets.asset_class')),
        Column('provider_name', String, ForeignKey('data_providers.provider_name'))
    )

    Table(
        'data_providers_exchanges',
        meta,
        Column('provider_name'),
        Column('exchange')
    )

    Table(
        'universes',
        meta,
        Column('universe_id'),
        Column('universe_name')
    )

    Table(
        'universes_assets',
        meta,
        Column('universe_id'),
        Column('asset_class')
    )

    Table(
        'universes_exchanges',
        meta,
        Column(''),
        Column('')
    )


    Table(
        'brokers',
        meta,
        Column('broker_name', String, primary_key=True),
        Column('leverage', Float(32), nullable=False),
    )

    Table(
        'brokers_exchanges',
        meta,
        Column('broker_name', String, ForeignKey('brokers.broker_name')),
        Column('exchange', String, ForeignKey('exchanges.exchange'))
    )

    Table(
        'brokers_assets',
        meta,
        Column('broker_name', String, ForeignKey('brokers.broker_name')),
        Column('asset_class', String, ForeignKey('assets.asset_class'))
    )

    Table(
        'accounts',
        meta,
        Column('account_id', primary_key=True),
        Column('account_type', String, nullable=False),
        Column('tax_model', String, ForeignKey('tax_models.tax_model'))
    )

    Table(
        'broker_accounts',
        meta,
        Column('broker_account_id', primary_key=True),
        Column('account_mode', String, nullable=False),
        Column('capital', Float(32), nullable=False),
        Column('account_id', String, ForeignKey('accounts.account_id')),
        Column('broker_name', String, ForeignKey('brokers.broker_name'))
    )

    meta.create_all(engine)
