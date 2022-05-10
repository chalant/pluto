import sqlalchemy as sa

account_setup = sa.Table(
    sa.column("capital"),
    sa.Column("price")
)

engine_setup = sa.Table(
    sa.Column("commission_model"),
    sa.Column("slippage_model"),

)

backtest_setup = sa.Table(
    sa.Column("start_dt"),
    sa.Column("end_dt")
)

