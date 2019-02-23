from zipline.gens import tradesimulation

from zipline.gens.sim_engine import (
    BAR,
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START_BAR
)

'''This is the "main loop" => controls the algorithm, and handles shut-down signals etc.'''
class AlgorithmController(object):
    def __init__(self, algo, sim_params, data_portal, account, emission_rate, clock, benchmark_source, restrictions, universe_func):

        self._account = account
        self._emission_rate = emission_rate

    def transform(self):
        algo = self.algo
        emission_rate = self._emission_rate
        account = self._account
        dp = self.data_portal

        def every_bar(dt_to_use, current_data=self.current_data,
                      handle_data=algo.event_manager.handle_data):
            for capital_change in calculate_minute_capital_changes(dt_to_use):
                yield capital_change

            self.simulation_dt = dt_to_use
            algo.on_dt_changed(dt_to_use)
            #update the account class...
            account.update(dt_to_use)
            handle_data(algo, current_data, dt_to_use)

        def once_a_day(midnight_dt, current_data=self.current_data, data_portal=dp):
            for capital_change in algo.calculate_capital_changes(
                    midnight_dt,emission_rate=emission_rate,is_interday=True):
                yield capital_change

            self.simulation_dt = midnight_dt
            algo.on_dt_changed(midnight_dt)

            account.handle_market_open(midnight_dt, data_portal)

        def on_exit():
            # Remove references to algo, data portal, et al to break cycles
            # and ensure deterministic cleanup of these objects when the
            # simulation finishes.
            self.algo = None
            self.benchmark_source = self.current_data = self.data_portal = None

        with ExitStack() as stack:
            stack.callback(on_exit)
            stack.enter_context(self.processor)
            stack.enter_context(ZiplineAPI(self.algo))

            if algo.data_frequency == 'minute':
                def execute_order_cancellation_policy():
                    algo.blotter.execute_cancel_policy(SESSION_END)

                def calculate_minute_capital_changes(dt):
                    # process any capital changes that came between the last
                    # and current minutes
                    return algo.calculate_capital_changes(
                        dt, emission_rate=emission_rate, is_interday=False)
            else:
                def execute_order_cancellation_policy():
                    pass

                def calculate_minute_capital_changes(dt):
                    return []

            for dt, action in self.clock:
                if action == BAR:
                    for capital_change_packet in every_bar(dt):
                        yield capital_change_packet
                elif action == SESSION_START:
                    for capital_change_packet in once_a_day(dt):
                        yield capital_change_packet
                elif action == SESSION_END:
                    # Code for handling end of days...
                    # End of the session.
                    positions = account.positions
                    position_assets = algo.asset_finder.retrieve_all(positions)
                    self._cleanup_expired_assets(dt, position_assets)

                    execute_order_cancellation_policy()
                    algo.validate_account_controls()

                    yield self._get_daily_message(dt, algo, account)
                elif action == BEFORE_TRADING_START_BAR:
                    self.simulation_dt = dt
                    algo.on_dt_changed(dt)
                    algo.before_trading_start(self.current_data)
                elif action == MINUTE_END:
                    minute_msg = self._get_minute_message(
                        dt,
                        algo,
                        account,
                    )

                    yield minute_msg

            risk_message = metrics_tracker.handle_simulation_end(
                self.data_portal,
            )
            yield risk_message

