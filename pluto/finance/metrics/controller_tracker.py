import pandas as pd

from pluto.finance import metrics as mtr
from pluto.finance.metrics import tracker


class CalendarTracker(object):
    def __init__(self, benchmark_source):
        self._returns = []
        self._benchmark_returns = []
        self._benchmark_source = benchmark_source

    def compute_returns(self, dt, ledger):
        self._returns.append(ledger.todays_returns)
        self._benchmark_source.append(self._benchmark_source.get_returns(dt))


# this class stores data
class MetricsTracker(object):
    def __init__(self,
                 start_dt,
                 capital,
                 emission_rate):
        """

        Parameters
        ----------
        start_dt: pandas.Timestamp
        capital: float
        emission_rate: str
        """

        self._first_open_session = None

        self._capital_base = capital

        self._metrics = mtr.pluto_metrics()

        self._first_session = start_dt
        self._market_open = start_dt

        # for metric in self._metrics:
        #     metric.initialization(
        #         first_session=start_dt,
        #         ledger=self._ledger)

        self._calendar_trackers = {}
        self._emission_rate = emission_rate

    def handle_minute_close(self, dt, data_portal, ledger, calendar_name):
        """

        Handles the close of the given minute in minute emission.

        Parameters
        ----------
        dt : Timestamp
            The minute that is ending
        data_portal: zipline.data.data_portal.DataPortal
        ledger: pluto.finance.ledger.Ledger

        Returns
        -------
        A minute perf packet.

        """
        # self.sync_last_sale_prices(dt, data_portal) <= this is already done at the update method...

        packet = {
            'period_end': dt,
            'capital_base': self._capital_base,
            'minute_perf': {
                'period_open': self._first_open_session,
                'period_close': dt,
            },
            'cumulative_perf': {
                'period_close': dt,
            },
            'progress': None,
            'cumulative_risk_metrics': {},
        }

        # updates returns at the end of the minute.
        # ledger_.end_of_bar(self._data_frequency)
        # benchmark_source.on_minute_end(dt, data_portal, trading_calendar, sessions)

        for metric in self._metrics:
            metric.end_of_bar(
                packet=packet,
                ledger=ledger,
                dt=dt,
                data_portal=data_portal,
                emission_rate=self._emission_rate)

        return packet

    def handle_market_open(self, session_label, data_portal, calendar_name):
        """

        Handles the start of each session.

        Parameters
        ----------
        session_label : pandas.Timestamp
            The label of the session that is about to begin.
        data_portal : zipline.data.data_portal.DataPortal
            The current data portal.

        """
        ledger_ = self._ledger
        ledger_.start_of_session(session_label)

        self._current_session = session_label

        if not self._first_open_session:
            self._first_open_session = session_label

        # the daily packet's period_open: the session's execution open, as
        # zipline's MetricsTracker.handle_market_open
        self._market_open, self._market_close = tracker.MetricsTracker._execution_open_and_close(
            data_portal.trading_calendar,
            session_label,
        )

        for metric in self._metrics:
            metric.start_of_session(
                dt=session_label,
                session=session_label,
                ledger=ledger_,
                data_portal=data_portal)

    def handle_market_close(self, dt, data_portal, calendar_name):
        ledger_ = self._ledger
        first_session = ledger_.first_session

        packet = {
            'period_start': first_session,
            'period_end': dt,
            'capital_base': self._capital_base,
            'daily_perf': {
                'period_open': self._market_open,
                'period_close': dt,
            },
            'cumulative_perf': {
                'period_open': first_session,
                'period_close': dt,
            },
            # 'progress': self._progress(self),
            'cumulative_risk_metrics': {},
        }

        ledger_ = self._ledger

        for metric in self._metrics:
            metric.end_of_session(
                dt=dt,
                packet=packet,
                ledger=ledger_,
                data_portal=data_portal)

        return packet
