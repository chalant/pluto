import logbook

import pandas as pd

from zipline import algorithm
from zipline.utils import events
from zipline.utils import cache
from zipline.utils.pandas_utils import clear_dataframe_indexer_caches

from zipline.pipeline.engine import (
    ExplodingPipelineEngine,
    SimplePipelineEngine)

from zipline.utils.api_support import (
    api_method)

from zipline.utils.preprocess import preprocess
from zipline.utils.input_validation import (
    ensure_upper_case
)

log = logbook.Logger("TradingAlgorithm")

class TradingAlgorithm(algorithm.TradingAlgorithm):
    def __init__(self,
                 controllable,
                 sim_params,
                 blotter,
                 metrics_tracker,
                 get_pipeline_loader,
                 initialize,
                 handle_data,
                 before_trading_start,
                 analyze=None):
        self._controllable = controllable

        self._metrics_tracker = metrics_tracker
        # List of trading controls to be used to validate orders.
        self.trading_controls = []

        # List of account controls to be checked on each bar.
        self.account_controls = []

        self._recorded_vars = {}

        self._platform = 'pluto'
        self.logger = None

        self.sim_params = sim_params
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

    def init_engine(self, get_loader):
        """
        Construct and store a PipelineEngine from loader.

        If get_loader is None, constructs an ExplodingPipelineEngine
        """

        controllable = self._controllable

        if get_loader is not None:
            self.engine = SimplePipelineEngine(
                get_loader,
                controllable.asset_finder,
                self.default_pipeline_domain(controllable.trading_calendar),
            )
        else:
            self.engine = ExplodingPipelineEngine()

    def get_history_window(self, bar_count, frequency, assets, field, ffill):
        controllable = self._controllable

        if not self._in_before_trading_start:
            return controllable.data_portal.get_history_window(
                assets,
                self.datetime,
                bar_count,
                frequency,
                field,
                controllable.data_frequency,
                ffill,
            )
        else:
            # If we are in before_trading_start, we need to get the window
            # as of the previous market minute
            adjusted_dt = \
                controllable.trading_calendar.previous_minute(
                    self.datetime
                )

            window = controllable.data_portal.get_history_window(
                assets,
                adjusted_dt,
                bar_count,
                frequency,
                field,
                controllable.data_frequency,
                ffill,
            )

            # Get the adjustments between the last market minute and the
            # current before_trading_start dt and apply to the window
            adjs = controllable.data_portal.get_adjustments(
                assets,
                field,
                adjusted_dt,
                controllable.datetime
            )
            window = window * adjs

            return window

    def calculate_capital_changes(self,
                                  dt,
                                  emission_rate,
                                  is_interday,
                                  portfolio_value_adjustment=0.0):
        return

    @api_method
    @preprocess(root_symbol_str=ensure_upper_case)
    def continuous_future(self,
                          root_symbol_str,
                          offset=0,
                          roll='volume',
                          adjustment='mul'):
        """Create a specifier for a continuous contract.

        Parameters
        ----------
        root_symbol_str : str
            The root symbol for the future chain.

        offset : int, optional
            The distance from the primary contract. Default is 0.

        roll_style : str, optional
            How rolls are determined. Default is 'volume'.

        adjustment : str, optional
            Method for adjusting lookback prices between rolls. Options are
            'mul', 'add', and None. Default is 'mul'.

        Returns
        -------
        continuous_future : zipline.assets.ContinuousFuture
            The continuous future specifier.
        """
        return self._controllable.asset_finder.create_continuous_future(
            root_symbol_str,
            offset,
            roll,
            adjustment,
        )

    def symbol(self, symbol_str, country_code=None):
        """Lookup an Equity by its ticker symbol.

        Parameters
        ----------
        symbol_str : str
            The ticker symbol for the equity to lookup.
        country_code : str or None, optional
            A country to limit symbol searches to.

        Returns
        -------
        equity : zipline.assets.Equity
            The equity that held the ticker symbol on the current
            symbol lookup date.

        Raises
        ------
        SymbolNotFound
            Raised when the symbols was not held on the current lookup date.

        See Also
        --------
        :func:`zipline.api.set_symbol_lookup_date`
        """
        # If the user has not set the symbol lookup date,
        # use the end_session as the date for symbol->sid resolution.
        _lookup_date = self._symbol_lookup_date \
            if self._symbol_lookup_date is not None \
            else self.sim_params.end_session

        return self._controllable.asset_finder.lookup_symbol(
            symbol_str,
            as_of_date=_lookup_date,
            country_code=country_code,
        )

    @api_method
    def sid(self, sid):
        """Lookup an Asset by its unique asset identifier.

        Parameters
        ----------
        sid : int
            The unique integer that identifies an asset.

        Returns
        -------
        asset : zipline.assets.Asset
            The asset with the given ``sid``.

        Raises
        ------
        SidsNotFound
            When a requested ``sid`` does not map to any asset.
        """
        return self._controllable.asset_finder.retrieve_asset(sid)

    @api_method
    @preprocess(symbol=ensure_upper_case)
    def future_symbol(self, symbol):
        """Lookup a futures contract with a given symbol.

        Parameters
        ----------
        symbol : str
            The symbol of the desired contract.

        Returns
        -------
        future : zipline.assets.Future
            The future that trades with the name ``symbol``.

        Raises
        ------
        SymbolNotFound
            Raised when no contract named 'symbol' is found.
        """
        return self._controllable.asset_finder.lookup_future_symbol(symbol)

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
        sessions = self._controllable.trading_calendar.all_sessions

        # Load data until the day before end session...
        end_date_loc = sessions.get_loc(end_session)

        #todo: chunk size = end_dt - start_dt
        start_loc = end_date_loc - chunksize

        start_session = sessions[start_loc]

        return self.engine.run_pipeline(pipeline, start_session, end_session), end_session
