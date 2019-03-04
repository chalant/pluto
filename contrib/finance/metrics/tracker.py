from contrib.finance import metrics as mtr

class MetricsTracker(object):
    def __init__(self, account, first_session, first_market_open, metrics=None):
        #todo: we need a moving window of "sessions" and we need to record the previous
        # values
        #todo: with the default metrics, there is no need for start_of_simulation
        # and end_of_simulation.
        self._account = account

        self._first_session = first_session
        self._first_market_open = first_market_open

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

    def update(self, dt):
        self._account.update(dt)

    def handle_initialization(self, dt, trading_calendar):
        for metric in self._metrics:
            metric.initialization(first_open_session=self._first_market_open)


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
        #todo: whats this?
        ledger.end_of_bar(self._session_count)

        for metric in self._metrics:
            metric.end_of_bar(packet=packet,ledger=ledger,dt=dt,data_portal=data_portal)
        return packet

    def handle_market_open(self, session_label, data_portal, trading_calendar):
        """Handles the start of each session.

                Parameters
                ----------
                session_label : Timestamp
                    The label of the session that is about to begin.
                data_portal : DataPortal
                    The current data portal.
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
            metric.start_of_session(
                ledger=ledger,
                data_portal=data_portal
            )

    def handle_market_close(self, dt, data_portal):
        pass


    @staticmethod
    def _execution_open_and_close(calendar, session):
        open_, close = calendar.open_and_close_for_session(session)
        execution_open = calendar.execution_time_from_open(open_)
        execution_close = calendar.execution_time_from_close(close)

        return execution_open, execution_close

    def on_initialize(self, first_open_session, trading_calendar, sessions, emission_rate, benchmark_source):
        for metric in self._metrics:
            metric.initialization(first_open_session=first_open_session)

    def handle_stop(self):
        pass

