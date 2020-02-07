from pandas import Timestamp

from protos import assets_pb2 as pr_asset, controllable_pb2 as cbl, finance_pb2 as fin, metrics_pb2 as metrics, \
    protocol_pb2 as pr

from zipline.assets import Asset, ExchangeInfo
from zipline.finance.order import Order
from zipline.finance.transaction import Transaction
from zipline.finance.position import Position
from zipline import protocol

from google.protobuf import timestamp_pb2 as pr_ts

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
        asset=to_proto_asset(zp_position.asset),
        amount=zp_position.amount,
        cost_basis=zp_position.cost_basis,
        last_sale_price=zp_position.last_sale_price,
        last_sale_date=to_proto_timestamp(zp_position.last_sale_date)
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
        positions=[pr.AssetPositionPair(key=asset, position=position)
                   for asset, position in zp_portfolio.positions.items()],
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
        dt=to_proto_timestamp(zp_order.dt),
        asset=to_proto_asset(zp_order.asset),
        amount=zp_order.amount,
        stop=zp_order.stop,
        limit=zp_order.limit,
        filled=zp_order.filled,
        commission=zp_order.commission
    )


def to_proto_transaction(zp_transaction):
    return fin.Transaction(
        asset=to_proto_asset(zp_transaction.asset),
        amount=zp_transaction.amount,
        dt=zp_transaction.dt,
        price=zp_transaction.price,
        order_id=zp_transaction.order_id,
        commission=zp_transaction.commission
    )


def from_proto_performance_packet(proto_perf_packet):
    def from_proto_cum_metrics(cum_metrics):
        return {
            'period_open': to_datetime(cum_metrics.period_open),
            'period_close': to_datetime(cum_metrics.period_close),
            'returns': cum_metrics.returns,
            'pnl': cum_metrics.pnl,
            'cash_flow': cum_metrics.cash_flow,
            'starting_exposure': cum_metrics.starting_exposure,
            'ending_exposure': cum_metrics.ending_exposure,
            'starting_cash': cum_metrics.starting_cash,
            'ending_cash': cum_metrics.ending_cash,
            'longs_count': cum_metrics.long_count,
            'shorts_count': cum_metrics.short_count,
            'long_value': cum_metrics.long_value,
            'short_value': cum_metrics.short_value,
            'long_exposure': cum_metrics.long_exposure,
            'short_exposure': cum_metrics.short_exposure,
            'gross_leverage': cum_metrics.gross_leverage,
            'net_leverage': cum_metrics.net_leverage
        }

    def from_proto_cum_risk_metrics(cum_risk_metrics):
        return {
            'algorithm_volatility': cum_risk_metrics.algorithm_volatility,
            'benchmark_period_return': cum_risk_metrics.benchmark_period_return,
            'benchmark_volatility': cum_risk_metrics.benchmark_volatility,
            'algorithm_period_return': cum_risk_metrics.algorithm_period_return,
            'alpha': cum_risk_metrics.alpha,
            'beta': cum_risk_metrics.beta,
            'sharpe': cum_risk_metrics.sharpe,
            'sortino': cum_risk_metrics.sortino,
            'max_drawdown': cum_risk_metrics.max_drawdown,
            'max_leverage': cum_risk_metrics.max_leverage
        }

    def from_proto_period_performance(period_performance):
        cum_metrics = period_performance.cumulative_metrics
        pcm = period_performance.period_common_metrics
        return {
            'period_open': to_datetime(cum_metrics.period_open),
            'period_close': to_datetime(cum_metrics.period_close),
            'returns': cum_metrics.returns,
            'pnl': cum_metrics.pnl,
            'cash_flow': cum_metrics.cash_flow,
            'starting_exposure': cum_metrics.starting_exposure,
            'ending_exposure': cum_metrics.ending_exposure,
            'starting_cash': cum_metrics.starting_cash,
            'ending_cash': cum_metrics.ending_cash,
            'longs_count': cum_metrics.long_count,
            'shorts_count': cum_metrics.short_count,
            'long_value': cum_metrics.long_value,
            'short_value': cum_metrics.short_value,
            'long_exposure': cum_metrics.long_exposure,
            'short_exposure': cum_metrics.short_exposure,
            'gross_leverage': cum_metrics.gross_leverage,
            'net_leverage': cum_metrics.net_leverage,
            'orders': [to_zp_order(order) for order in pcm.orders],
            'transactions': [to_zp_transaction(trx) for trx in pcm.transactions],
            'positions': [to_zp_position(pos) for pos in pcm.positions]
        }

    return {
        'cumulative_perf': from_proto_cum_metrics(proto_perf_packet.cumulative_perf),
        'daily_perf': from_proto_period_performance(proto_perf_packet.daily_perf),
        'minutely_perf': from_proto_period_performance(proto_perf_packet.minutely_perf),
        'cumulative_risk_metrics': from_proto_cum_risk_metrics(proto_perf_packet.cumulative_risk_metrics)
    }


def to_proto_performance_packet(perf_packet):
    def to_proto_cum_metrics(cum_perf):
        return metrics.CumulativeMetrics(
            period_open=to_proto_timestamp(cum_perf['period_open']),
            period_close=to_proto_timestamp(cum_perf['period_close']),
            returns=cum_perf['returns'],
            pnl=cum_perf['pnl'],
            cash_flow=cum_perf['cash_flow'],
            starting_exposure=cum_perf['starting_exposure'],
            ending_exposure=cum_perf['ending_exposure'],
            starting_value=cum_perf['starting_value'],
            ending_value=cum_perf['ending_value'],
            starting_cash=cum_perf['starting_cash'],
            ending_cash=cum_perf['ending_cash'],
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
        return cbl.PeriodPerformance(
            cumulative_metrics=to_proto_cum_metrics(period_perf['cumulative_metrics']),
            period_common_metrics=metrics.PeriodCommonMetrics(
                orders=period_perf['orders'],
                transactions=period_perf['transactions'],
                positions=period_perf['positions']
            )
        )

    def to_proto_risk_metrics(cum_risk_metrics):
        return metrics.CumulativeRiskMetrics(
            algorithm_volatility=cum_risk_metrics['algorithm_volatility'],
            benchmark_period_return=cum_risk_metrics['benchmark_period_return'],
            benchmark_volatility=cum_risk_metrics['benchmark_volatility'],
            algorithm_period_return=cum_risk_metrics['algorithm_period_return'],
            alpha=cum_risk_metrics['alpha'],
            beta=cum_risk_metrics['beta'],
            sharpe=cum_risk_metrics['sharpe'],
            sortino=cum_risk_metrics['sortino'],
            max_drawdown=cum_risk_metrics['max_drawdown'],
            max_leverage=cum_risk_metrics['max_leverage']
        )

    return cbl.PerformancePacket(
        cumulative_perf=to_proto_cum_metrics(perf_packet['cumulative_perf']),
        daily_perf=to_proto_period_perf(perf_packet['daily_perf']),
        minutely_perf=to_proto_period_perf(perf_packet['minute_perf']),
        cumulative_risk_metrics=to_proto_risk_metrics(perf_packet['cumulative_risk_metrics'])
    )
