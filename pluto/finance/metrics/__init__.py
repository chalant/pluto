from .metric import (
    AlphaBeta,
    BenchmarkReturnsAndVolatility,
    CashFlow,
    DailyLedgerField,
    MaxLeverage,
    NumTradingDays,
    Orders,
    PeriodLabel,
    PNL,
    Returns,
    ReturnsStatistic,
    SimpleLedgerField,
    StartOfPeriodLedgerField,
    Transactions,
    _ConstantCumulativeRiskMetric
)

import empyrical

def pluto_metrics():
    return {
        Returns(),
        ReturnsStatistic(empyrical.annual_volatility, 'algo_volatility'),
        BenchmarkReturnsAndVolatility(),
        PNL(),
        CashFlow(),
        Orders(),
        Transactions(),

        SimpleLedgerField('positions'),

        StartOfPeriodLedgerField(
            'portfolio.positions_exposure',
            'starting_exposure',
        ),
        DailyLedgerField(
            'portfolio.positions_exposure',
            'ending_exposure',
        ),

        StartOfPeriodLedgerField(
            'portfolio.positions_value',
            'starting_value'
        ),
        DailyLedgerField('portfolio.positions_value', 'ending_value'),

        StartOfPeriodLedgerField('portfolio.cash', 'starting_cash'),
        DailyLedgerField('portfolio.cash', 'ending_cash'),

        DailyLedgerField('portfolio.portfolio_value'),

        DailyLedgerField('position_tracker.stats.longs_count'),
        DailyLedgerField('position_tracker.stats.shorts_count'),
        DailyLedgerField('position_tracker.stats.long_value'),
        DailyLedgerField('position_tracker.stats.short_value'),
        DailyLedgerField('position_tracker.stats.long_exposure'),
        DailyLedgerField('position_tracker.stats.short_exposure'),

        DailyLedgerField('account.gross_leverage'),
        DailyLedgerField('account.net_leverage'),

        AlphaBeta(),
        ReturnsStatistic(empyrical.sharpe_ratio, 'sharpe'),
        ReturnsStatistic(empyrical.sortino_ratio, 'sortino'),

        ReturnsStatistic(empyrical.max_drawdown),
        MaxLeverage(),

        # Please kill these!
        _ConstantCumulativeRiskMetric('excess_return', 0.0),
        _ConstantCumulativeRiskMetric('treasury_period_return', 0.0),
        NumTradingDays(),
        PeriodLabel(),
    }