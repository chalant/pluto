import pandas as pd

from pluto.finance import metrics as mtr
from pluto.coms.utils import conversions
from pluto.finance import returns

from protos import tracker_state_pb2 as trs


class MetricsTracker(object):
    def __init__(self,
                 ledger_,
                 capital,
                 data_frequency,
                 emission_rate,
                 start_dt):
        """

        Parameters
        ----------
        ledger_: pluto.finance.ledger.Ledger
        capital: float
        data_frequency: str
        emission_rate: str
        start_dt: pandas.Timestamp
        """
        # self._benchmark_source = benchmark_source
        self._ledger = ledger_

        # self._returns_tracker = returns.ReturnsTracker(
        #     self._ledger,
        #     look_back)

        self._first_open_session = None

        self._capital_base = capital
        self._data_frequency = data_frequency
        self._emission_rate = emission_rate

        self._metrics = mtr.pluto_metrics()

        self._first_session = start_dt
        self._last_sync = None

        for metric in self._metrics:
            metric.initialization(
                first_session=start_dt,
                ledger=self._ledger)

    @property
    def ledger(self):
        return self._ledger

    @property
    def portfolio(self):
        return self._ledger.portfolio

    @property
    def account(self):
        return self._ledger.account

    @property
    def positions(self):
        return self._ledger._position_tracker.positions

    def process_transaction(self, transaction):
        self._ledger.process_transaction(transaction)

    def handle_splits(self, splits):
        self._ledger.process_splits(splits)

    def process_order(self, order):
        self._ledger.process_order(order)

    def process_commission(self, commission):
        self._ledger.process_commission(commission)

    def process_close_position(self, asset, dt, data_portal):
        self._ledger.close_position(
            asset, dt,
            data_portal,
            self._data_frequency)

    def capital_change(self, amount):
        self._ledger.capital_change(amount)

    def sync_last_sale_prices(self, dt, data_portal, handle_non_market_minutes=False):
        if dt != self._last_sync:

            self._ledger.sync_last_sale_prices(
                dt, data_portal,
                self._data_frequency,
                handle_non_market_minutes)

            self._last_sync = dt

    def update_account(self, main_account):
        self._ledger.update_account(main_account)

    def get_state(self, dt):
        return trs.TrackerState(
            first_open_session=conversions.to_proto_timestamp(
                self._first_open_session),
            ledger_state=self._ledger.get_state(dt),
            last_checkpoint=conversions.to_proto_timestamp(dt)
        ).SerializeToString()

    def restore_state(self, state):
        ledger_ = self._ledger

        tr_state = trs.TrackerState()
        tr_state.ParseFromString(state)

        self._first_open_session = pd.Timestamp(
            conversions.to_datetime(tr_state.first_open_session),
            tz='UTC')

        self._last_checkpoint = ledger_.restore_state(tr_state.account_state)

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
        # self.sync_last_sale_prices(dt, data_portal) <= this is already done at the update method...
        ledger_ = self._ledger

        first_session = ledger_.first_session

        packet = {
            'period_start': first_session,
            'period_end': dt,
            'capital_base': self._capital_base,
            'minute_perf': {
                'period_open': self._first_open_session,
                'period_close': dt,
            },
            'cumulative_perf': {
                'period_open': first_session,
                'period_close': dt,
            },
            'progress': None,
            'cumulative_risk_metrics': {},
        }

        # updates returns at the end of the minute.
        # benchmark_source = self._benchmark_source
        # ledger_.end_of_bar(self._data_frequency)
        # returns_tracker = self._returns_tracker
        # benchmark_source.on_minute_end(dt, data_portal, trading_calendar, sessions)
        # returns_tracker.end_of_bar(self._data_frequency)

        for metric in self._metrics:
            metric.end_of_bar(
                packet=packet,
                ledger=ledger_,
                # benchmark_source=benchmark_source,
                dt=dt,
                data_portal=data_portal,
                emission_rate=self._emission_rate
            )
        return packet

    def handle_market_open(self, session_label, data_portal):
        """

        Handles the start of each session.

        Parameters
        ----------
        session_label : pandas.Timestamp
            The label of the session that is about to begin.
        data_portal : DataPortal
            The current data portal.
        trading_calendar : trading_calendars.TradingCalendar

        """
        ledger_ = self._ledger
        ledger_.start_of_session(session_label)

        # self._returns_tracker.start_of_session(session_label)
        # self._benchmark_source.on_session_start(session_label)

        self._current_session = session_label

        if not self._first_open_session:
            self._first_open_session = session_label

        self._market_open = session_label

        # self._market_open, self._market_close = self._execution_open_and_close(
        #     trading_calendar,
        #     session_label,
        # )

        for metric in self._metrics:
            metric.start_of_session(
                dt=session_label,
                session=session_label,
                ledger=ledger_,
                data_portal=data_portal)

    def handle_market_close(self, dt, data_portal):
        first_session = self._ledger.first_session

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

        # returns_tracker = self._returns_tracker
        # benchmark_source = self._benchmark_source
        # returns_tracker.end_of_session()

        # benchmark_source.on_session_end(
        #     data_portal,
        #     trading_calendar,
        #     sessions)

        for metric in self._metrics:
            metric.end_of_session(
                dt=dt,
                packet=packet,
                ledger=ledger_,
                # benchmark_source=benchmark_source,
                data_portal=data_portal
                # returns_tracker=returns_tracker
            )

        return packet

    @staticmethod
    def _execution_open_and_close(calendar, session):
        open_, close = calendar.open_and_close_for_session(session)
        execution_open = calendar.execution_time_from_open(open_)
        execution_close = calendar.execution_time_from_close(close)

        return execution_open, execution_close

    def handle_stop(self, dt):
        """

        Parameters
        ----------
        dt : pandas.Timestamp
            Time at which the stop signal was generated
        data_portal : DataPortal

        """
        raise NotImplementedError('handle_stop')

    def handle_liquidation(self, dt):
        """

        Parameters
        ----------
        dt : pandas.Timestamp
            Time at which the liquidation signal was generated

        """
        raise NotImplementedError('handle_liquidation')
