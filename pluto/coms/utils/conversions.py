from pandas import Timestamp

from google.protobuf import timestamp_pb2 as pr_ts

from zipline.assets import Asset, ExchangeInfo
from zipline.finance.order import Order
from zipline.finance.transaction import Transaction
from zipline.finance.position import Position
from zipline import protocol
from zipline.finance import execution

from protos import assets_pb2 as pr_asset
from protos import controller_pb2 as ctl
from protos import finance_pb2 as fin
from protos import metrics_pb2 as metrics
from protos import protocol_pb2 as pr
from protos import broker_pb2

def datetime_from_bytes(bytes_):
    ts = pr_ts.Timestamp()
    to_datetime(ts.ParseFromString(bytes_))

def to_proto_timestamp(dt):
    """

    Parameters
    ----------
    dt: pandas.Timestamp

    Returns
    -------
    google.protobuf.timestamp_pb2.Timestamp
    """
    ts = pr_ts.Timestamp()
    ts.FromDatetime(dt)
    return ts


def to_datetime(proto_ts):
    return Timestamp(proto_ts.ToDatetime(), tz='UTC')


def to_pandas_timestamp(protos_ts, tz=None):
    return Timestamp(to_datetime(protos_ts)).tz_localize(tz)

def to_proto_commission(zp_commission):
    return broker_pb2.Commission(
        asset=to_proto_asset(zp_commission['asset']),
        order=to_proto_order(zp_commission['order']),
        cost=zp_commission['cost']
    )

def from_proto_commission(proto_commission):
    return {
        'asset':to_zp_asset(proto_commission.asset),
        'order':to_proto_order(proto_commission.order),
        'cost':proto_commission.cost
    }

def to_proto_asset(zp_asset):
    return pr_asset.Asset(
        sid=zp_asset.sid,
        symbol=zp_asset.symbol,
        asset_name=zp_asset.asset_name,
        start_date=to_proto_timestamp(zp_asset.start_date),
        end_date=to_proto_timestamp(zp_asset.end_date),
        first_traded=to_proto_timestamp(zp_asset.start_date),
        auto_close_date=to_proto_timestamp(zp_asset.auto_close_date),
        exchange=zp_asset.exchange,
        exchange_full=zp_asset.exchange_full,
        country_code=zp_asset.country_code,
        tick_size=zp_asset.tick_size,
        multiplier=zp_asset.price_multiplier
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

# def to_zp_execution_style(proto_order_params):
#     style = proto_order_params.style
#     asset = proto_order_params.asset
#     if style == 'stop_limit':
#         return execution.StopLimitOrder(
#             proto_order_params.limit_price,
#             proto_order_params.stop_price,
#             to_zp_asset(asset),
#             exchange=proto_order_params.exchange)
#     elif style == 'market':
#         return execution.MarketOrder(
#             exchange=proto_order_params.exchange)
#     elif style == 'limit':
#         return execution.LimitOrder(
#             proto_order_params.limit_price,
#             to_zp_asset(asset),
#             exchange=proto_order_params.exchange)
#     elif style == 'stop':
#         return execution.StopOrder(
#             proto_order_params.stop_price,
#             asset=to_zp_asset(asset)
#         )
#     else:
#         raise ValueError('Unexpected order style {}'.format(style))

# def to_proto_order_params(asset, amount, style, order_id=None):
#     ast = to_proto_asset(asset)
#     if type(style) == execution.LimitOrder:
#         return broker_pb2.OrderParams(
#             asset=ast,
#             style='limit',
#             amount=amount,
#             limit_price=style.limit_price,
#             exchange=style.exchange)
#     elif type(style) == execution.MarketOrder:
#         return broker_pb2.OrderParams(
#             asset=ast,
#             style='market',
#             amount=amount,
#             exchange=style.exchange)
#     elif type(style) == execution.StopOrder:
#         return broker_pb2.OrderParams(
#             asset=ast,
#             style='stop',
#             amount=amount,
#             stop_price=style.stop_price,
#             exchange=style.exchange)
#     elif type(style) == execution.StopLimitOrder:
#         return broker_pb2.OrderParams(
#             asset=ast,
#             style='stop_limit',
#             amount=amount,
#             stop_price=style.stop_price,
#             limit_price=style.limit_price)
#     else:
#         raise ValueError('Unexpected order style {}'.format(type(style)))

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
        proto_transaction.order_id
    )


def to_zp_position(proto_position):
    return Position(
        to_zp_asset(proto_position.asset),
        proto_position.amount,
        proto_position.cost_basis,
        proto_position.last_sale_price,
        to_datetime(proto_position.last_sale_date)
    ).to_dict()


def to_zp_portfolio(proto_portfolio):
    portfolio = protocol.MutableView(protocol.Portfolio(to_datetime(proto_portfolio.start_date)))
    portfolio.cash = proto_portfolio.cash
    portfolio.cash_flow = proto_portfolio.cash_flow
    portfolio.starting_cash = proto_portfolio.starting_cash
    portfolio.pnl = proto_portfolio.pnl
    portfolio.portfolio_value = proto_portfolio.portfolio_value
    portfolio.returns = proto_portfolio.returns
    portfolio.positions = {position.key: to_zp_position(position) for position in proto_portfolio.position}
    portfolio.positions_value = proto_portfolio.positions_value
    portfolio.positions_exposure = proto_portfolio.positions_exposure


def to_zp_account(proto_account):
    pass


def to_proto_account(zp_account):
    pass


def to_proto_position(zp_position):
    """

    Parameters
    ----------
    zp_position : zipline.finance.position.Position

    Returns
    -------


    """
    return pr.Position(
        asset=to_proto_asset(zp_position['sid']),
        amount=zp_position['amount'],
        cost_basis=zp_position['cost_basis'],
        last_sale_price=zp_position['last_sale_price']
    )


def to_proto_portfolio(zp_portfolio):
    """

    Parameters
    ----------
    zp_portfolio : protocol.Portfolio

    Returns
    -------

    """
    return pr.Portfolio(
        cash_flow=zp_portfolio.cash_flow,
        starting_cash=zp_portfolio.starting_cash,
        portfolio_value=zp_portfolio.portfolio_value,
        pnl=zp_portfolio.pnl,
        returns=zp_portfolio.returns,
        cash=zp_portfolio.cash,
        positions=[
            pr.AssetPositionPair(key=asset, position=position)
            for asset, position in zp_portfolio.positions.items()
        ],
        start_date=to_proto_timestamp(zp_portfolio.start_date),
        positions_value=zp_portfolio.positions_value,
        positions_exposure=zp_portfolio.positions_exposure
    )


def to_proto_account(zp_account):
    """

    Parameters
    ----------
    zp_account : protocol.Account

    Returns
    -------

    """

    return pr.Account(

    )


def to_proto_order(zp_order):
    return pr.Order(
        dt=to_proto_timestamp(zp_order['dt']),
        asset=to_proto_asset(zp_order['sid']),
        amount=zp_order['amount'],
        stop=zp_order['stop'],
        limit=zp_order['limit'],
        filled=zp_order['filled'],
        commission=zp_order['commission']
    )


def to_proto_transaction(zp_transaction):
    return fin.Transaction(
        asset=to_proto_asset(zp_transaction['sid']),
        amount=zp_transaction['amount'],
        dt=to_proto_timestamp(zp_transaction['dt']),
        price=zp_transaction['price'],
        order_id=zp_transaction['order_id']
    )

def from_proto_cum_metrics(cum_metrics):
    return {
        'period_open': to_datetime(cum_metrics.period_open),
        'period_close': to_datetime(cum_metrics.period_close),
        'returns': cum_metrics.returns,
        'pnl': cum_metrics.pnl,
        # 'cash_flow': cum_metrics.cash_flow,
        'capital_used': cum_metrics.capital_used,
        'starting_exposure': cum_metrics.starting_exposure,
        'starting_value': cum_metrics.starting_value,
        'ending_value': cum_metrics.ending_value,
        'ending_exposure': cum_metrics.ending_exposure,
        'starting_cash': cum_metrics.starting_cash,
        'ending_cash': cum_metrics.ending_cash,
        'portfolio_value': cum_metrics.portfolio_value,
        'longs_count': cum_metrics.longs_count,
        'shorts_count': cum_metrics.shorts_count,
        'long_value': cum_metrics.long_value,
        'short_value': cum_metrics.short_value,
        'long_exposure': cum_metrics.long_exposure,
        'short_exposure': cum_metrics.short_exposure,
        'gross_leverage': cum_metrics.gross_leverage,
        'net_leverage': cum_metrics.net_leverage
    }

def from_proto_cum_risk_metrics(cum_risk_metrics):
    return {
        'algo_volatility': cum_risk_metrics.algo_volatility,
        'benchmark_period_return': cum_risk_metrics.benchmark_period_return,
        'benchmark_volatility': cum_risk_metrics.benchmark_volatility,
        'algorithm_period_return': cum_risk_metrics.algorithm_period_return,
        'alpha': cum_risk_metrics.alpha,
        'beta': cum_risk_metrics.beta,
        'sharpe': cum_risk_metrics.sharpe,
        'sortino': cum_risk_metrics.sortino,
        'max_drawdown': cum_risk_metrics.max_drawdown,
        'max_leverage': cum_risk_metrics.max_leverage,
        'trading_days': cum_risk_metrics.trading_days,
        'period_label': cum_risk_metrics.period_label,
        'excess_return': cum_risk_metrics.excess_return,
        'treasury_period_return': cum_risk_metrics.treasury_period_return
    }

def from_proto_period_metrics(period_metrics):
    return {
        'orders': [to_zp_order(order) for order in period_metrics.orders],
        'transactions': [to_zp_transaction(trx) for trx in period_metrics.transactions],
        'positions': [to_zp_position(pos) for pos in period_metrics.positions],
        'period_open': to_datetime(period_metrics.period_open),
        'period_close': to_datetime(period_metrics.period_close),
        'capital_used': period_metrics.capital_used,
        'starting_exposure': period_metrics.starting_exposure,
        'ending_exposure': period_metrics.ending_exposure,
        'starting_value': period_metrics.starting_value,
        'starting_cash': period_metrics.starting_cash,
        'returns': period_metrics.returns,
        'pnl': period_metrics.pnl
    }

def from_proto_performance_packet(proto_perf_packet):
    return {
        'cumulative_perf': from_proto_cum_metrics(proto_perf_packet.cumulative_perf),
        proto_perf_packet.packet_type: from_proto_period_metrics(proto_perf_packet.period_perf),
        'cumulative_risk_metrics': from_proto_cum_risk_metrics(proto_perf_packet.cumulative_risk_metrics)
    }

def to_proto_cum_metrics(cum_perf):
    return metrics.CumulativeMetrics(
        period_open=to_proto_timestamp(cum_perf['period_open']),
        period_close=to_proto_timestamp(cum_perf['period_close']),
        returns=cum_perf['returns'],
        pnl=cum_perf['pnl'],
        capital_used = cum_perf['capital_used'],
        # cash_flow=cum_perf['cash_flow'],
        starting_exposure=cum_perf['starting_exposure'],
        ending_exposure=cum_perf['ending_exposure'],
        starting_value=cum_perf['starting_value'],
        ending_value=cum_perf['ending_value'],
        starting_cash=cum_perf['starting_cash'],
        ending_cash=cum_perf['ending_cash'],
        portfolio_value = cum_perf['portfolio_value'],
        longs_count=cum_perf['longs_count'],
        shorts_count=cum_perf['shorts_count'],
        long_value=cum_perf['long_value'],
        short_value=cum_perf['short_value'],
        long_exposure=cum_perf['long_exposure'],
        short_exposure=cum_perf['short_exposure'],
        gross_leverage=cum_perf['gross_leverage'],
        net_leverage=cum_perf['net_leverage']
    )

def to_proto_period_perf(period_perf):
    return metrics.PeriodMetrics(
        orders=[to_proto_order(order) for order in period_perf['orders']],
        transactions=[to_proto_transaction(trc) for trc in period_perf['transactions']],
        positions=[to_proto_position(pos) for pos in period_perf['positions']],
        period_open=to_proto_timestamp(period_perf['period_open']),
        period_close=to_proto_timestamp(period_perf['period_close']),
        capital_used=period_perf['capital_used'],
        starting_exposure=period_perf['starting_exposure'],
        ending_exposure=period_perf['ending_exposure'],
        starting_value=period_perf['starting_value'],
        starting_cash=period_perf['starting_cash'],
        returns=period_perf['returns'],
        pnl=period_perf['pnl']
    )

def to_proto_cum_risk_metrics(cum_risk_metrics):
    return metrics.CumulativeRiskMetrics(
        algo_volatility=cum_risk_metrics['algo_volatility'],
        benchmark_period_return=cum_risk_metrics['benchmark_period_return'],
        benchmark_volatility=cum_risk_metrics['benchmark_volatility'],
        algorithm_period_return=cum_risk_metrics['algorithm_period_return'],
        alpha=cum_risk_metrics['alpha'],
        beta=cum_risk_metrics['beta'],
        sharpe=cum_risk_metrics['sharpe'],
        sortino=cum_risk_metrics['sortino'],
        max_drawdown=cum_risk_metrics['max_drawdown'],
        max_leverage=cum_risk_metrics['max_leverage'],
        trading_days=cum_risk_metrics['trading_days'],
        period_label=cum_risk_metrics['period_label'],
        excess_return=cum_risk_metrics['excess_return'],
        treasury_period_return=cum_risk_metrics['treasury_period_return']
    )

def to_proto_performance_packet(perf_packet):
    period_perf = perf_packet.get('daily_perf', None)
    key = 'daily_perf'
    if not period_perf:
        period_perf = perf_packet.get('minute_perf')
        key = 'minute_perf'
    return ctl.PerformancePacket(
        cumulative_perf=to_proto_cum_metrics(perf_packet['cumulative_perf']),
        period_perf=to_proto_period_perf(period_perf),
        cumulative_risk_metrics=to_proto_cum_risk_metrics(perf_packet['cumulative_risk_metrics']),
        packet_type=key
    )
