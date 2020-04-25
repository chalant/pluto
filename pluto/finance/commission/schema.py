import sqlalchemy as sa

metadata = sa.MetaData()

commission_metas = sa.Table(
    'commission_metas',
    metadata,
    sa.Column(
        'broker',
        sa.String,
        nullable=False),
    sa.Column(
        'model',
        sa.String,
        nullable=False
    ),
    sa.Column(
        'exchange',
        sa.String,
        nullable=False
    ),
    sa.Column(
        'country_code',
        sa.String,
        nullable=False
    ),
    sa.Column(
        'asset_type',
        sa.String,
        nullable=False)
)

_parameter_tables = {
    'per_share':sa.Table(
        'per_share',
        metadata,
        sa.Column(
        'broker',
        sa.String,
        nullable=False
        ),
        sa.Column(
        'cost_per_share',
        sa.Float,
        nullable=False
        ),
        sa.Column(
        'min_trade_cost',
        sa.Float,
        nullable=False
    )),
    'per_dollar':sa.Table(
        'per_dollar',
        metadata,
        sa.Column(
            'broker',
            sa.String,
            nullable=False
        ),
        sa.Column(
            'cost_per_dollar',
            sa.Float,
            nullable=False
        )
    ),
    'per_contract': sa.Table(
        'per_contract',
        metadata,
        sa.Column(
            'broker',
            sa.String,
            nullable=False
        ),
        sa.Column(
            'cost',
            sa.Float,
            nullable=False
        ),
        sa.Column(
            'exchange_fee',
            sa.Float,
            nullable=False
        )),
    'per_trade': sa.Table(
        'per_trade',
        metadata,
        sa.Column(
            'broker',
            sa.String,
            nullable=False
        ),
        sa.Column(
            'cost',
            sa.Float,
            nullable=False
        )
    ),
    'per_future_trade': sa.Table(
        'per_future_trade',
        metadata,
        sa.Column(
            'broker',
            sa.String,
            nullable=False),
        sa.Column(
            'cost',
            sa.Float,
            nullable=False
        )
    )
}

def get_parameter_table(model_type):
    return _parameter_tables[model_type]