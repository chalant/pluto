import logbook

import pandas as pd

from zipline import algorithm
from zipline.utils import events
from zipline.utils import cache
from zipline.utils.pandas_utils import clear_dataframe_indexer_caches

log = logbook.Logger("TradingAlgorithm")

class TradingAlgorithm(algorithm.TradingAlgorithm):
    def __init__(self,
                 sim_params,
                 data_portal,
                 blotter,
                 metrics_tracker,
                 get_pipeline_loader,
                 initialize,
                 handle_data,
                 before_trading_start,
                 analyze=None):

        self._metrics_tracker = metrics_tracker
        # List of trading controls to be used to validate orders.
        self.trading_controls = []

        # List of account controls to be checked on each bar.
        self.account_controls = []

        self._recorded_vars = {}

        self._platform = 'pluto'
        self.logger = None

        self.data_portal = data_portal

        self.asset_finder = data_portal.asset_finder
        self.sim_params = sim_params
        self.trading_calendar = sim_params.trading_calendar
        self._last_sync_time = pd.NaT

        self.init_engine(get_pipeline_loader)
        self._pipelines = {}

        # Create an already-expired cache so that we compute the first time
        # data is requested.
        self._pipeline_cache = cache.ExpiringCache(
            cleanup=clear_dataframe_indexer_caches
        )

        self.blotter = blotter

        # The symbol lookup date specifies the date to use when resolving
        # symbols to sids, and can be set using set_symbol_lookup_date()
        self._symbol_lookup_date = None

        self.initialized = False
        self._initialize = initialize
        self._before_trading_start = before_trading_start
        self._handle_data = handle_data
        self._analyze = analyze

        self._in_before_trading_start = False

        self.event_manager = events.EventManager()

        self.event_manager.add_event(
            events.Event(
                events.Always(),
                # We pass handle_data.__func__ to get the unbound method.
                # We will explicitly pass the algorithm to bind it again.
                self.handle_data.__func__,
            ),
            prepend=True,
        )

    @property
    def account(self):
        return self._metrics_tracker.account

    @property
    def portfolio(self):
        return self._metrics_tracker.portfolio

    def initialize(self, *args, **kwargs):
        self._initialize(self, *args, **kwargs)

    def analyze(self, perf):
        if not self._analyze:
            return

        return self._analyze(perf)

    def run(self, data_portal=None):
        pass

    def get_generator(self):
        return

    def calculate_capital_changes(self, dt, emission_rate, is_interday, portfolio_value_adjustment=0.0):
        return

class LiveTradingAlgorithm(TradingAlgorithm):
    def run_pipeline(self, pipeline, end_session, chunksize):
        """
        Compute `pipeline`, providing values for at least `end_date`.

        Produces a DataFrame containing data for days between `start_date` and
        `end_date`, where `end_date` is defined by:

            `end_date = min(start_date + chunksize trading days,
                            simulation_end)`

        Returns
        -------
        (data, valid_until) : tuple (pd.DataFrame, pd.Timestamp)

        See Also
        --------
        PipelineEngine.run_pipeline
        """
        sessions = self.trading_calendar.all_sessions

        # Load data until the day before end session...
        end_date_loc = sessions.get_loc(end_session)

        #todo: chunk size = end_dt - start_dt
        start_loc = end_date_loc - chunksize

        start_session = sessions[start_loc]

        return self.engine.run_pipeline(pipeline, start_session, end_session), end_session
