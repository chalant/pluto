from contrib.coms.protos import account_service_pb2_grpc as acs

from zipline.finance._finance_ext import (
    PositionStats,
    calculate_position_tracker_stats,
)

class RemoteLedger(object):
    '''we need this so that we can make sure that the trades were actually executed server
    side before updating any values...'''
    def __init__(self,account):
        '''account: an account from the server...'''
        self._account = account
        #todo: this must loaded from some file and saved at each end of session. (is concatenated)
        # problem: it gets bigger at each load... we might aggregate results every x period...
        self.daily_returns_array = []
        self._stats = PositionStats.new()
        self._in_sync = False
        self._positions = None
        self._account = None
        self._portfolio = None
        self._orders = None
        self._transactions = None

    @property
    def account(self):
        return self._account.account

    @property
    def stats(self):
        """The current status of the positions.

                Returns
                -------
                stats : PositionStats
                    The current stats position stats.

                Notes
                -----
                This is cached, repeated access will not recompute the stats until
                the stats may have changed.
                """
        calculate_position_tracker_stats(self.positions, self._stats)
        return self._stats


    def orders(self, dt=None):
        if not self._in_sync or not self._orders:
            self._orders = self._account.orders(dt)
        return self._orders

    @property
    def positions(self):
        if not self._positions or not self._in_sync:
            self._positions = self._account.positions
        return self._positions

    def _get_portfolio(self):
        if not self._in_sync or not self._portfolio:
            self._portfolio = self._account.portfolio
        return self._portfolio

    @property
    def todays_returns(self):
        return ((self._get_returns(self._get_portfolio()) + 1 ) / (self._previous_total_returns + 1) - 1)

    def _get_returns(self, portfolio):
        return portfolio.returns

    def update(self):
        #marks this object as not in sync so we need to synchronize with the server
        self._in_sync = False

    def start_of_session(self, session_label):
        '''called every day...'''
        #todo: try to load the last saved return if failed, fetch the returns of the portfolio,
         #if failed, the value is 0
        #mark as not in sync so that we fetch the latest values from the server
        self._in_sync = False
        self._previous_total_returns = self._get_returns(self._get_portfolio())

    def end_of_bar(self, session_ix):
        self.daily_returns_array[session_ix] = self.todays_returns

    def end_of_session(self, session_ix):
        #todo: store every tracked metrics (returns etc.) on disk
        self.daily_returns_array[session_ix] = self.todays_returns

    #this is to accomodate the algorithm object
    def capital_change(self, change_amount):
        #this is handled by the broker
        pass

    def transactions(self, dt=None):
        '''retrieves transaction from some specified datetime, if dt is none, retrieves
        all the transactions ever made...'''
        if not self._transactions or not self._in_sync:
            self._transactions = self._account.transactions(dt)
        return self._transactions

    #this is to accommodate the algorithm object
    def sync_last_sale_prices(self,
                              dt,
                              data_portal,
                              handle_non_market_minutes=False):
        #handled by the broker
        pass