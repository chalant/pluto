from contrib.coms.protos import assets_pb2 as pr_asset

from zipline.assets import Asset, ExchangeInfo
from zipline.finance.order import Order
from zipline.finance.transaction import Transaction
from zipline.finance.position import Position

from contrib.coms.protos import protocol_pb2 as pr
from contrib.coms.protos import finance_pb2 as fin

from google.protobuf import timestamp_pb2 as pr_ts


def to_proto_datetime(dt):
    # todo: what if it is a pandas.Timestamp?
    if dt is None:
        return
    ts = pr_ts.Timestamp()
    ts.FromDatetime(dt)
    return ts

def to_datetime(proto_ts):
    return proto_ts.ToDatetime()


def to_proto_asset(zp_asset):
    return pr_asset.Asset(
        sid=zp_asset.sid,
        symbol=zp_asset.symbol,
        asset_name=zp_asset.asset_name,
        start_date=to_proto_datetime(zp_asset.start_date),
        end_date=to_proto_datetime(zp_asset.end_date),
        first_traded=to_proto_datetime(zp_asset.start_date),
        auto_close_date=to_proto_datetime(zp_asset.auto_close_date),
        exchange=zp_asset.exchange,
        exchange_full=zp_asset.exchange_full,
        country_code=zp_asset.country_code,
        tick_size=zp_asset.tick_size,
        multiplier=zp_asset.multiplier
    )


def to_zp_asset(pr_asset):
    return Asset(
        pr_asset.sid,
        ExchangeInfo(
            pr_asset.exchange_full,
            pr_asset.exchange,
            pr_asset.country_code
        ),
        pr_asset.symbol,
        pr_asset.asset_name,
        pr_asset.start_date,
        pr_asset.end_date,
        pr_asset.first_traded,
        pr_asset.auto_close_date,
        pr_asset.tick_size,
        pr_asset.multiplier
    )

def to_zp_order(proto_order):
    return Order(
        proto_order.dt,
        to_zp_asset(proto_order.asset),
        proto_order.amount,
        proto_order.stop,
        proto_order.limit,
        proto_order.filled,
        proto_order.commission
    )

def to_zp_transaction(proto_transaction):
    return Transaction(
        to_zp_asset(proto_transaction.asset),
        proto_transaction.amount,
        to_datetime(proto_transaction.dt),
        proto_transaction.price,
        proto_transaction.order_id,
        proto_transaction.commission
    )

def to_zp_position(proto_position):
    return Position(
        to_zp_asset(proto_position.asset),
        proto_position.amount,
        proto_position.cost_basis,
        proto_position.last_sale_price,
        to_datetime(proto_position.last_sale_date)
    )

def to_proto_order(zp_order):
    return pr.Order(
        dt = to_proto_datetime(zp_order.dt),
        asset = to_proto_asset(zp_order.asset),
        amount = zp_order.amount,
        stop = zp_order.stop,
        limit = zp_order.limit,
        filled = zp_order.filled,
        commission = zp_order.commission
    )

def to_proto_transaction(zp_transaction):
    return fin.Transaction(
        asset = to_proto_asset(zp_transaction.asset),
        amount = zp_transaction.amount,
        dt = zp_transaction.dt,
        price = zp_transaction.price,
        order_id = zp_transaction.order_id,
        commission = zp_transaction.commission
    )
