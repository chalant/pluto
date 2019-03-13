import datetime
from functools import partial

from dateutil.relativedelta import relativedelta

from contrib.finance import metrics as mtr


class MetricsTracker(object):
    def __init__(self, account, metrics=None):
        self._account = account

        self._first_session = None
        self._first_market_open = None
        self._capital_base = None

        if metrics is None:
            self._metrics = mtr.contrib_metrics()

    @property
    def portfolio(self):
        return self._account.portfolio

    @property
    def account(self):
        return self._account.account

    @property
    def positions(self):
        return self._account.positions

    def update(self, dt, data_portal, trading_calendar, target_capital=None,
               portfolio_value_adjustment=0.0, handle_non_market_minutes=False):
        self._account.update(dt, data_portal, trading_calendar, target_capital, portfolio_value_adjustment,
                             handle_non_market_minutes)


    def handle_minute_close(self, dt, data_portal):
        """

        Handles the close of the given minute in minute emission.

        Parameters
        ----------
        dt : Timestamp
            The minute that is ending

        Returns
        -------
        A minute perf packet.

        """
        self.sync_last_sale_prices(dt, data_portal)
        first_session = self._first_session

        packet = {
            'period_start': first_session,
            'period_end': dt,
            'capital_base': self._capital_base,
            'minute_perf': {
                'period_open': self._market_open,
                'period_close': dt,
            },
            'cumulative_perf': {
                'period_open': first_session,
                'period_close': dt,
            },
            'progress': None,
            'cumulative_risk_metrics': {},
        }
        ledger = self._account

        # updates returns at the end of the bar.
        ledger.end_of_bar(dt)

        for metric in self._metrics:
            metric.end_of_bar(packet=packet, ledger=ledger, dt=dt, data_portal=data_portal)
        return packet

    def handle_market_open(self, session_label, data_portal, trading_calendar):
        """

        Handles the start of each session.

        Parameters
        ----------
        session_label : Timestamp
            The label of the session that is about to begin.
        data_portal : DataPortal
            The current data portal.
        trading_calendar : TradingCalendar

        """
        ledger = self._account
        ledger.start_of_session(session_label)

        ledger.handle_market_open(session_label, data_portal)

        self._current_session = session_label

        self._market_open, self._market_close = self._execution_open_and_close(
            trading_calendar,
            session_label,
        )

        for metric in self._metrics:
            metric.start_of_session(ledger=ledger, data_portal=data_portal)

    def handle_market_close(self, dt, data_portal):
        packet = {
            'period_start': self._first_session,
            'period_end': dt,
            'capital_base': self._capital_base,
            'daily_perf': {
                'period_open': self._market_open,
                'period_close': dt,
            },
            'cumulative_perf': {
                'period_open': self._first_session,
                'period_close': dt,
            },
            'progress': self._progress(self),
            'cumulative_risk_metrics': {},
        }

        ledger = self._account
        ledger.end_of_session(dt)

        for metric in self._metrics:
            metric.end_of_session(packet=packet, ledger=ledger, data_portal=data_portal)

    @staticmethod
    def _execution_open_and_close(calendar, session):
        open_, close = calendar.open_and_close_for_session(session)
        execution_open = calendar.execution_time_from_open(open_)
        execution_close = calendar.execution_time_from_close(close)

        return execution_open, execution_close


    def handle_initialization(self, first_session, first_open_session, capital_base):
        """

        Handles initialization of the metrics tracker.

        Parameters
        ----------
        first_session : Timestamp
        first_open_session : Timestamp
        capital_base : float

        """
        self._first_open_session = first_open_session
        self._first_session = first_session
        self._capital_base = capital_base

        for metric in self._metrics:
            metric.initialization(first_open_session=first_open_session)

    def handle_stop(self, dt):
        """

        Parameters
        ----------
        dt : Timestamp
            Time at which the stop signal was generated
        data_portal : DataPortal

        """


    def handle_liquidation(self, dt):
        """

        Parameters
        ----------
        dt : Timestamp
            Time at which the liquidation signal was generated
        data_portal : DataPortal

        """
