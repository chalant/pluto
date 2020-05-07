import logbook
import warnings

import numpy as np
import pandas as pd

from zipline import algorithm
from zipline.utils import events
from zipline.utils import cache
from zipline.utils.pandas_utils import (
    clear_dataframe_indexer_caches,
    normalize_date
)

from zipline.utils.math_utils import (
    tolerant_equals,
)

from zipline.errors import (
    CannotOrderDelistedAsset
)

from zipline.pipeline.engine import (
    ExplodingPipelineEngine,
    SimplePipelineEngine
)

from zipline.utils.api_support import (
    api_method,
)

from zipline.utils.preprocess import preprocess
from zipline.utils.input_validation import (
    ensure_upper_case
)

log = logbook.Logger("TradingAlgorithm")


# TODO: we need to store the algorithm state as-well=> store its dictionary variable etc.
class TradingAlgorithm(algorithm.TradingAlgorithm):
    def __init__(self,
                 controllable,
                 sim_params,
                 blotter,
                 metrics_tracker,
                 pipeline_loader,
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

        self.init_engine(pipeline_loader)
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
        self._controllable.sync_last_sale_prices()
        return self._metrics_tracker.account

    @property
    def portfolio(self):
        self._controllable.sync_last_sale_prices()
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

    def init_engine(self, pipeline_loader):
        """
        Construct and store a PipelineEngine from loader.

        If get_loader is None, constructs an ExplodingPipelineEngine
        """

        controllable = self._controllable

        loader = pipeline_loader

        if loader is not None:
            self.engine = SimplePipelineEngine(
                loader,
                controllable,
                controllable.domain
            )
        else:
            self.engine = ExplodingPipelineEngine()

    @api_method
    def schedule_function(self,
                          func,
                          date_rule=None,
                          time_rule=None,
                          half_days=True,
                          calendar=None):

        # When the user calls schedule_function(func, <time_rule>), assume that
        # the user meant to specify a time rule but no date rule, instead of
        # a date rule and no time rule as the signature suggests
        if isinstance(date_rule, (events.AfterOpen, events.BeforeClose)) and not time_rule:
            warnings.warn('Got a time rule for the second positional argument '
                          'date_rule. You should use keyword argument '
                          'time_rule= when calling schedule_function without '
                          'specifying a date_rule', stacklevel=3)

        date_rule = date_rule or events.date_rules.every_day()
        time_rule = ((time_rule or events.time_rules.every_minute())
                     if self.sim_params.data_frequency == 'minute' else
                     # If we are in daily mode the time_rule is ignored.
                     events.time_rules.every_minute())

        cal = self._controllable.trading_calendar
        self.add_event(
            events.make_eventrule(date_rule, time_rule, cal, half_days),
            func)

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

    def _calculate_order_value_amount(self, asset, value):
        """
                Calculates how many shares/contracts to order based on the type of
                asset being ordered.
                """
        # Make sure the asset exists, and that there is a last price for it.
        # FIXME: we should use BarData's can_trade logic here, but I haven't
        # yet found a good way to do that.

        dt = self.datetime
        normalized_date = normalize_date(dt)

        if normalized_date < asset.start_date:
            raise CannotOrderDelistedAsset(
                msg="Cannot order {0}, as it started trading on"
                    " {1}.".format(asset.symbol, asset.start_date)
            )
        elif normalized_date > asset.end_date:
            raise CannotOrderDelistedAsset(
                msg="Cannot order {0}, as it stopped trading on"
                    " {1}.".format(asset.symbol, asset.end_date)
            )
        else:
            last_price = \
                self._controllable.current_data.current(asset, "price")

            if np.isnan(last_price):
                raise CannotOrderDelistedAsset(
                    msg="Cannot order {0} on {1} as there is no last "
                        "price for the security.".format(
                        asset.symbol,
                        dt
                    )
                )

        if tolerant_equals(last_price, 0):
            zero_message = "Price of 0 for {psid}; can't infer value".format(
                psid=asset
            )
            if self.logger:
                self.logger.debug(zero_message)
            # Don't place any order
            return 0

        value_multiplier = asset.price_multiplier

        return value / (last_price * value_multiplier)

    def run_pipeline(self, pipeline, start_session, chunksize):
        sessions = self._controllable.trading_calendar.all_sessions

        # Load data starting from the previous trading day...
        start_date_loc = sessions.get_loc(start_session)

        # ...continuing until either the day before the simulation end, or
        # until chunksize days of data have been loaded.
        sim_end_session = self._controllable.run_params.end_session

        end_loc = min(
            start_date_loc + chunksize,
            sessions.get_loc(sim_end_session)
        )

        end_session = sessions[end_loc]

        return (
            self.engine.run_pipeline(
                pipeline,
                start_session,
                end_session
            ),
            end_session
        )

    @api_method
    def set_asset_restrictions(self, restrictions, on_error='fail'):
        pass

    @api_method
    def set_slippage(self, us_equities=None, us_futures=None):
        # disable setting slippage
        pass

    @api_method
    def set_commission(self, us_equities=None, us_futures=None):
        # disable setting commission
        pass

    @api_method
    def set_cancel_policy(self, cancel_policy):
        # disable set_cancel_policy
        pass


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

        # todo: chunk size = end_dt - start_dt
        start_loc = end_date_loc - chunksize

        start_session = sessions[start_loc]

        return self.engine.run_pipeline(pipeline, start_session, end_session), end_session
