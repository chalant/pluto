from pluto.coms.utils import conversions

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
            Not used: the ledger holds the capital (kept in the signature).
        data_frequency: str
        emission_rate: str
            Not used (kept in the signature).
        start_dt: pandas.Timestamp
            Not used: the ledger holds the start (kept in the signature).
        """
        # self._benchmark_source = benchmark_source
        self._ledger = ledger_
        # self._returns_tracker = returns.ReturnsTracker(
        #     self._ledger,
        #     look_back)

        self._first_open_session = None
        self._data_frequency = data_frequency
        self._last_sync = None

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

        self._first_open_session = conversions.to_datetime(
            tr_state.first_open_session)

        self._last_checkpoint = ledger_.restore_state(tr_state.ledger_state)

    def handle_market_open(self, session_label, data_portal):
        """

        Handles the start of each session: the ledger starts the session.

        Parameters
        ----------
        session_label : pandas.Timestamp
            The label of the session that is about to begin.
        data_portal : DataPortal
            Not used (the tracker computes no metrics: they are computed from the stream of the broker service).

        """
        self._ledger.start_of_session(session_label)

        self._current_session = session_label

        if not self._first_open_session:
            self._first_open_session = session_label

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
