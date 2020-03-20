import abc

import operator as op

import empyrical as ep
import numpy as np


class Metric(abc.ABC):
    def initialization(self, **kwargs):
        """

        Keyword Arguments
        -----------------
        first_open_session: Timestamp
        """
        pass

    def end_of_bar(self, **kwargs):
        """
        Keyword Arguments
        -----------------
        packet: dict
        ledger: contrib.Account
        dt: pandas.Timestamp
        emission_rate: str
        trading_calendar: TradingCalendar
        data_portal: DataPortal
        benchmark_source: contrib.BenchmarkSource
        """
        pass

    def end_of_session(self, **kwargs):
        """

        Keyword Arguments
        -----------------
        packet : dict
        ledger : Account
        dt : Timestamp
        trading_calendar : TradingCalendar
        data_portal : DataPortal
        benchmark_source : BenchmarkSource
        """
        pass

    def start_of_session(self, **kwargs):
        """

        Keyword Arguments
        -----------------
        ledger : Account
        session : Timestamp
        data_portal : DataPortal
        """
        pass


class SimpleLedgerField(Metric):
    def __init__(self, ledger_field, packet_field=None):
        self._get_ledger_field = op.attrgetter(ledger_field)
        if packet_field is None:
            self._packet_field = ledger_field.rsplit('.', 1)[-1]
        else:
            self._packet_field = packet_field

    def end_of_bar(self, **kwargs):
        kwargs.pop('packet')['minute_perf'][self._packet_field] = self._get_ledger_field(kwargs.pop('ledger'))

    def end_of_session(self, **kwargs):
        kwargs.pop('packet')['daily_perf'][self._packet_field] = self._get_ledger_field(kwargs.pop('ledger'))


class DailyLedgerField(Metric):
    def __init__(self, ledger_field, packet_field=None):
        self._get_ledger_field = op.attrgetter(ledger_field)
        if packet_field is None:
            self._packet_field = ledger_field.rsplit('.', 1)[-1]
        else:
            self._packet_field = packet_field

    def end_of_bar(self, **kwargs):
        field = self._packet_field
        packet = kwargs.pop('packet')
        packet['cumulative_perf'][field] = packet['minute_perf'][field] = \
            (self._get_ledger_field(kwargs.pop('ledger')))

    def end_of_session(self, **kwargs):
        field = self._packet_field
        packet = kwargs['packet']
        packet['cumulative_perf'][field] = packet['daily_perf'][field] = \
            (self._get_ledger_field(kwargs.pop('ledger')))


class StartOfPeriodLedgerField(Metric):
    def __init__(self, ledger_field, packet_field=None):
        self._get_ledger_field = op.attrgetter(ledger_field)
        if packet_field is None:
            self._packet_field = ledger_field.rsplit('.', 1)[-1]
        else:
            self._packet_field = packet_field

    def initialization(self, **kwargs):
        self._start_of_simulation = self._get_ledger_field(kwargs.pop('ledger'))

    def start_of_session(self, **kwargs):
        self._previous_day = self._get_ledger_field(kwargs.pop('ledger'))

    def _end_of_period(self, sub_field, packet):
        packet_field = self._packet_field
        packet['cumulative_perf'][packet_field] = self._start_of_simulation
        packet[sub_field][packet_field] = self._previous_day

    def end_of_bar(self, **kwargs):
        self._end_of_period('minute_perf', kwargs.pop('packet'))

    def end_of_session(self, **kwargs):
        self._end_of_period('daily_perf', kwargs.pop('packet'))


class Returns(Metric):
    def _end_of_period(self, field, packet, ledger):
        portfolio_returns = ledger.portfolio.returns
        packet[field]['returns'] = ledger.todays_returns(portfolio_returns)
        packet['cumulative_perf']['returns'] = portfolio_returns
        packet['cumulative_risk_metrics']['algorithm_period_return'] = portfolio_returns

    def end_of_bar(self, **kwargs):
        return self._end_of_period('minute_perf', kwargs.pop('packet'), kwargs.pop('ledger'))

    def end_of_session(self, **kwargs):
        self._end_of_period('daily_perf', kwargs.pop('packet'), kwargs.pop('ledger'))


class BenchmarkReturnsAndVolatility(Metric):
    def initialization(self, **kwargs):
        self._first_session = kwargs.pop('first_session')

    def start_of_session(self, **kwargs):
        self._current_session = kwargs.pop('dt')

    def end_of_bar(self, **kwargs):
        if kwargs.pop('emission_rate') == 'minute':
            benchmark_source = kwargs.pop('benchmark_source')
            packet = kwargs.pop('packet')

            r = benchmark_source.minute_cumulative_returns


            if np.isnan(r):
                r = None
            packet['cumulative_risk_metrics']['benchmark_period_return'] = r

            v = benchmark_source.minute_annual_volatility
            if np.isnan(v):
                v = None
            packet['cumulative_risk_metrics']['benchmark_volatility'] = v

    def end_of_session(self, **kwargs):
        packet = kwargs.pop('packet')

        benchmark_source = kwargs.pop('benchmark_source')
        r = benchmark_source.cumulative_returns
        if np.isnan(r):
            r = None
        packet['cumulative_risk_metrics']['benchmark_period_return'] = r

        v = benchmark_source.annual_volatility

        if np.isnan(v):
            v = None
        packet['cumulative_risk_metrics']['benchmark_volatility'] = v


class PNL(Metric):
    def initialization(self, **kwargs):
        self._previous_pnl = 0.0

    def start_of_session(self, **kwargs):
        self._previous_pnl = kwargs.pop('ledger').portfolio.pnl

    def _end_of_period(self, field, packet, ledger):
        pnl = ledger.portfolio.pnl
        packet[field]['pnl'] = pnl - self._previous_pnl
        packet['cumulative_perf']['pnl'] = ledger.portfolio.pnl

    def end_of_bar(self, **kwargs):
        self._end_of_period('minute_perf', kwargs.pop('packet'), kwargs.pop('ledger'))

    def end_of_session(self, **kwargs):
        self._end_of_period('daily_perf', kwargs.pop('packet'), kwargs.pop('ledger'))


class CashFlow(Metric):
    def initialization(self, **kwargs):
        self._previous_cash_flow = 0.0

    def end_of_bar(self, **kwargs):
        cash_flow = kwargs.pop('ledger').portfolio.cash_flow
        packet = kwargs.pop('packet')
        packet['minute_perf']['capital_used'] = (cash_flow - self._previous_cash_flow)
        packet['cumulative_perf']['capital_used'] = cash_flow

    def end_of_session(self, **kwargs):
        cash_flow = kwargs.pop('ledger').portfolio.cash_flow
        packet = kwargs.pop('packet')
        packet['daily_perf']['capital_used'] = (
                cash_flow - self._previous_cash_flow
        )
        packet['cumulative_perf']['capital_used'] = cash_flow
        self._previous_cash_flow = cash_flow


class Orders(Metric):
    def end_of_bar(self, **kwargs):
        kwargs.pop('packet')['minute_perf']['orders'] = kwargs.pop('ledger').orders(kwargs.pop('dt'))

    def end_of_session(self, **kwargs):
        kwargs.pop('packet')['daily_perf']['orders'] = kwargs.pop('ledger').orders()


class Transactions(Metric):
    def end_of_bar(self, **kwargs):
        kwargs.pop('packet')['minute_perf']['transactions'] = kwargs.pop('ledger').transactions(kwargs.pop('dt'))

    def end_of_session(self, **kwargs):
        kwargs.pop('packet')['daily_perf']['transactions'] = kwargs.pop('ledger').transactions()


class Positions(Metric):
    def end_of_bar(self, **kwargs):
        kwargs.pop('packet')['minute_perf']['positions'] = kwargs.pop('ledger').positions(kwargs.pop('dt'))

    def end_of_session(self, **kwargs):
        kwargs.pop('packet')['daily_perf']['positions'] = kwargs.pop('ledger').positions()


class ReturnsStatistic(Metric):
    """A metric that reports an end of simulation scalar or time series
        computed from the algorithm returns.

        Parameters
        ----------
        function : callable
            The function to call on the daily returns.
        field_name : str, optional
            The name of the field. If not provided, it will be
            ``function.__name__``.
        """

    def __init__(self, function, field_name=None):
        if field_name is None:
            field_name = function.__name__

        self._function = function
        self._field_name = field_name

    def end_of_bar(self, **kwargs):
        res = self._function(kwargs.pop('ledger').daily_returns)
        if not np.isfinite(res):
            res = None
        kwargs.pop('packet')['cumulative_risk_metrics'][self._field_name] = res

    def end_of_session(self, **kwargs):
        self.end_of_bar(**kwargs)


class AlphaBeta(Metric):
    def initialization(self, **kwargs):
        pass

    def end_of_bar(self, **kwargs):
        risk = kwargs.pop('packet')['cumulative_risk_metrics']

        ledger_returns = kwargs.pop('ledger').daily_returns
        benchmark_returns = kwargs.pop('benchmark_source').daily_returns()

        alpha, beta = ep.alpha_beta_aligned(
            ledger_returns,
            benchmark_returns)

        if np.isnan(alpha):
            alpha = None
        if np.isnan(beta):
            beta = None

        risk['alpha'] = alpha
        risk['beta'] = beta

    def end_of_session(self, **kwargs):
        return self.end_of_bar(**kwargs)


class MaxLeverage(Metric):
    def initialization(self, **kwargs):
        self._max_leverage = 0.0

    def end_of_bar(self, **kwargs):
        self._max_leverage = max(self._max_leverage, kwargs.pop('ledger').account.leverage)
        kwargs.pop('packet')['cumulative_risk_metrics']['max_leverage'] = self._max_leverage

    def end_of_session(self, **kwargs):
        self.end_of_bar(**kwargs)


class NumTradingDays(Metric):
    def initialization(self, **kwargs):
        self._num_trading_days = 0

    def start_of_session(self, **kwargs):
        self._num_trading_days += 1

    def end_of_bar(self, **kwargs):
        kwargs.pop('packet')['cumulative_risk_metrics']['trading_days'] = (self._num_trading_days)

    def end_of_session(self, **kwargs):
        self.end_of_bar(**kwargs)


class _ConstantCumulativeRiskMetric(Metric):
    """A metric which does not change, ever.

    Notes
    -----
    This exists to maintain the existing structure of the perf packets. We
    should kill this as soon as possible.
    """

    def __init__(self, field, value):
        self._field = field
        self._value = value

    def end_of_bar(self, **kwargs):
        kwargs.pop('packet')['cumulative_risk_metrics'][self._field] = self._value

    def end_of_session(self, **kwargs):
        kwargs.pop('packet')['cumulative_risk_metrics'][self._field] = self._value


class PeriodLabel(Metric):
    """Backwards compat, please kill me.
        """

    def start_of_session(self, **kwargs):
        self._label = kwargs.pop('session').strftime('%Y-%m')

    def end_of_bar(self, **kwargs):
        kwargs.pop('packet')['cumulative_risk_metrics']['period_label'] = self._label

    def end_of_session(self, **kwargs):
        self.end_of_bar(**kwargs)
