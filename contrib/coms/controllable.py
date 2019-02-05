from abc import ABC, abstractmethod

from uuid import uuid4

from collections import OrderedDict

from grpc import RpcError

import numpy as np

from contrib.coms.protos import controllable_service_pb2 as cbl
from contrib.coms.protos import controllable_service_pb2_grpc as cbl_grpc
from contrib.coms.protos import controller_service_pb2_grpc as ctr
from contrib.coms.protos import broker_pb2 as broker_msg
from contrib.coms.protos import broker_pb2_grpc as broker_rpc
from contrib.coms.utils import certification as crt
from contrib.coms.utils import server_utils as srv
from contrib.utils import files

from zipline.finance.blotter import SimulationBlotter
from zipline.finance.order import Order
from zipline.finance import position
from zipline import protocol as prt
from zipline.assets import Future, Asset
from zipline.finance._finance_ext import (
    calculate_position_tracker_stats,
    PositionStats
)
from zipline.finance.execution import (
    MarketOrder,
    LimitOrder,
    StopOrder,
    StopLimitOrder
)

class Broker(object):
    '''encapsulates a broker stub and converts zipline objects into proto messages and vice-versa'''

    def __init__(self, channel, token):
        self._stub = broker_rpc.BrokerStub(channel)
        self._token = token

    def _add_metadata(self, token, rpc, params):
        try:
            rpc.with_call(params, metadata=(('Token', token)))
        except RpcError:
            # todo: try to register
            pass

    def _to_pr_asset(self, zp_asset):
        raise NotImplementedError

    def _to_zp_order(self, order_msg):
        # todo: finish this.
        return Order(
            order_msg.dt,
            self._to_zp_asset(order_msg.asset),
            order_msg.amount,
            order_msg.stop,
            order_msg.limit,
            order_msg.filled,
            order_msg.commission)

    def _to_zp_asset(self, asset):
        return Asset(
            asset.sid,
            asset.symbol,
            asset.asset_name,
            asset.start_date,
            asset.end_date,
            asset.first_traded,
            asset.auto_close_date,
            asset.exchange,
            asset.exchange_full)

    def order(self, asset, amount, style, order_id=None):
        self._add_metadata(
            self._token,
            self._stub.SingleOrder,
            self._create_order_param(asset, style, amount)
        )
        # todo: send an order message to the broker
        order = self._to_zp_order(
            self._stub.SingleOrder(
                self._create_order_param(asset, style, amount)))
        return order.id

    @property
    def account(self):
        raise NotImplementedError

    @property
    def orders(self):
        raise NotImplementedError

    @property
    def transactions(self):
        raise NotImplementedError

    @property
    def positions(self):
        raise NotImplementedError

    @property
    def liquidated(self):
        raise NotImplementedError

    def _create_order_param(self, asset, style, amount):
        t = type(style)
        asset_ = self._to_pr_asset(asset)
        if t is MarketOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.MARKET_ORDER,
                amount=amount
            )
        elif t is LimitOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.LIMIT_ORDER,
                amount=amount,
                limit_price=t.get_limit_price(True)
            )
        elif t is StopOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.STOP_ORDER,
                amount=amount,
                stop_price=t.get_stop_price(True)
            )
        elif t is StopLimitOrder:
            return broker_msg.OrderParams(
                asset=asset_,
                style=broker_msg.OrderParams.STOP_LIMIT_ORDER,
                amount=amount,
                limit_price=t.get_limit_price(True),
                stop_price=t.get_stop_price(True)
            )
        else:
            raise NotImplementedError


# TODO: we need some immutable of the account, portfolio, positions etc. for the strategy developer
class Account(object):
    '''should sit on top of the broker in order to "isolate" each virtual account from
    the main account...
    keeps track of its own portfolio etc.
    this account must be updated at some frequency ex: each minute makes some calls to the
    broker so that it gets updated... at each call, each field gets updated before delivering the
    data...'''

    def __init__(self, token, start_dt, capital, broker_channel, data_portal):
        self._broker = Broker(broker_channel, token)

        # keep track of the orders of this account
        self._orders_by_id = OrderedDict()
        self._orders_by_modified = {}

        self._new_orders = []

        # to keep track of executed orders
        self._processed_transactions = {}

        self._closed_orders = {}

        self._positions = {}

        self._im_port = prt.Portfolio(start_dt, capital)
        self._portfolio = prt.MutableView(self._im_port)

        self._stats = PositionStats.new()

        self._im_account = prt.Account()
        self._account = prt.MutableView(self._im_account)

        self._data_portal = data_portal

        self._unpaid_stock_dividends = {}
        self._unpaid_dividends = {}

    def order(self, asset, amount, style, order_id=None):
        order = self._broker.order(asset, amount, style)
        dt = order.dt
        id_ = order.id
        orders_by_id = self._orders_by_id
        orders_by_modified = self._orders_by_modified
        orders = orders_by_modified.get(dt, OrderedDict([(id_, order)]))

        orders[id_] = orders_by_id[id_] = order
        orders.move_to_end(id_, last=True)
        orders_by_id.move_to_end(id_, last=True)

        return id_

    def orders(self, dt=None):
        """Retrieve the dict-form of all of the orders in a given bar or for
                the whole simulation.

                Parameters
                ----------
                dt : pd.Timestamp or None, optional
                    The particular datetime to look up order for. If not passed, or
                    None is explicitly passed, all of the orders will be returned.

                Returns
                -------
                orders : list[dict]
                    The order information.
                """
        if dt is None:
            # orders by id is already flattened
            return [o.to_dict() for o in self._orders_by_id.values()]

        return [o.to_dict() for o in self._get_dict_values(self._orders_by_modified.get(dt, {}))]

    def _get_dict_values(self, dict_):
        return dict_.values()

    @property
    def portfolio(self):
        # todo: should return a copy of the portfolio, so that we can't mutate the values
        return self._im_port

    @property
    def positions(self):
        # we will set the last_trade_price to the amount we paid for the transaction.
        # in the future we will use data directly fetched from realtime streaming.
        # so, when we call the get process_transactions method everything gets updated
        # account, positions, portfolio etc.
        return [pos.to_dict() for pos in self._positions.values() if pos.amount != 0]

    def transactions(self, dt):
        """Retrieve the dict-form of all of the transactions in a given bar or
                for the whole simulation.

                Parameters
                ----------
                dt : pd.Timestamp or None, optional
                    The particular datetime to look up transactions for. If not passed,
                    or None is explicitly passed, all of the transactions will be
                    returned.

                Returns
                -------
                transactions : list[dict]
                    The transaction information.
                """
        if dt is None:
            # flatten the by-day transactions
            return [
                txn
                for by_day in self._processed_transactions
                for txn in by_day
            ]

        return self._processed_transactions.get(dt, [])

    def update(self, dt):
        '''Updates this account'''
        #todo: maybe store the last update datetime?
        # this function is called externally at some emission rate
        self._update_account()

    def _update_account(self):
        # todo: finish this! handle liquidated stocks!
        #todo: we need to track (and store) closed orders so that we can identify liquidated assets.
        # we assume that
        '''filters transactions that concern this account.'''
        transactions = list(self._broker.transactions.values())
        '''Updates the orders positions and transactions of this account'''

        orders = self._broker.orders.values()
        portfolio = self._portfolio
        positions = self._positions

        for transaction in transactions:
            order = orders.get(transaction.order_id, None)
            # only consider the orders placed from this account
            if order is not None:
                commission = transaction.commission
                order_id = order.id
                asset = order.asset

                if order_id in self._orders_by_id:
                    # remove any closed order
                    if not order.open:
                        del self._orders_by_id[order_id]
                        self._closed_orders[order_id] = order
                    # update our orders with the new values
                    else:
                        # update the state of the order.
                        self._orders_by_id[order_id] = order
                        # update the positions
                        self._update_position(
                            positions,
                            asset,
                            transaction
                        )
                # check if the order was closed (probably means that it has been liquidated)
                # hope that liquidated positions retain the order_id.
                #todo: handle liquidated stocks
                elif order_id in self._closed_orders:
                    # this should mean that the position was liquidated...
                    if isinstance(asset, Future):
                        # cost = transaction.commission / asset.price_multiplier ? or it is handled
                        # by the broker?
                        # todo: handle future contracts (how do they work?)
                        pass
                    else:
                        # update the cash flow with the price, number of shares and commission
                        # todo: what about taxes and fees?
                        self._cash_flow(
                            portfolio,
                            -((transaction.price * transaction.amount) - transaction.commission)
                        )

                self._append_to_list(
                    self._processed_transactions.get(transaction.dt, []),
                    transaction.to_dict()
                )
                self._process_commission(positions, asset, commission)
            # update everything after processing commissions etc.
            self._update(portfolio, positions)

    def _append_to_list(self, list_, element):
        list_.append(element)

    def _update(self, portfolio, positions):
        stats = self._compute_stats(positions)
        self._update_portfolio(portfolio, stats)
        self._update_zp_account(self._broker.account, portfolio, stats)

    def _process_commission(self, positions, asset, cost):
        if asset in positions:
            positions[asset].adjust_commission_cost_basis(asset, cost)

    # this function is called once a day (called externally by the trade_control or metrics tracker?)
    def handle_market_open(self, midnight_dt):
        data_portal = self._data_portal
        positions = self._positions
        splits = data_portal.get_splits(tuple(positions.keys()), midnight_dt)
        portfolio = self._portfolio
        self._process_splits(portfolio, positions, splits)
        self._process_dividends(
            portfolio,
            positions,
            midnight_dt,
            data_portal.asset_finder,
            data_portal.adjustment_reader
        )
        # update everything
        self._update(portfolio, positions)

    def _process_splits(self, portfolio, positions, splits):
        if splits:
            total_leftover_cash = 0

            for asset, ratio in splits:
                if asset in positions:
                    # Make the position object handle the split. It returns the
                    # leftover cash from a fractional share, if there is any.
                    pos = positions[asset]
                    leftover_cash = pos.handle_split(asset, ratio)
                    total_leftover_cash += leftover_cash
            if total_leftover_cash > 0:
                self._cash_flow(portfolio, total_leftover_cash)

    def _process_dividends(self, portfolio, positions, next_session, asset_finder, adjustment_reader):
        # Earn dividends whose ex_date is the next trading day. We need to
        # check if we own any of these stocks so we know to pay them out when
        # the pay date comes.
        unpaid_dividends = self._unpaid_dividends
        unpaid_stock_dividends = self._unpaid_stock_dividends

        held_assets = set(positions)
        if held_assets:
            cash_dividends = adjustment_reader.get_dividends_with_ex_date(
                held_assets,
                next_session,
                asset_finder
            )
            stock_dividends = (
                adjustment_reader.get_stock_dividends_with_ex_date(
                    held_assets,
                    next_session,
                    asset_finder
                )
            )

            # Earning a dividend just marks that we need to get paid out on
            # the dividend's pay-date. This does not affect our cash yet.
            self._earn_dividends(
                positions,
                cash_dividends,
                stock_dividends,
                unpaid_dividends,
                unpaid_stock_dividends
            )

        # Pay out the dividends whose pay-date is the next session. This does
        # affect out cash.
        self._cash_flow(
            portfolio,
            self._pay_dividends(
                positions,
                next_session,
                unpaid_stock_dividends,
                unpaid_stock_dividends
            )
        )

    def _earn_dividends(self, positions, cash_dividends, stock_dividends, unpaid_dividends, unpaid_stock_dividends):
        """Given a list of dividends whose ex_dates are all the next trading
                day, calculate and store the cash and/or stock payments to be paid on
                each dividend's pay date.

                Parameters
                ----------
                cash_dividends : iterable of (asset, amount, pay_date) namedtuples

                stock_dividends: iterable of (asset, payment_asset, ratio, pay_date)
                    namedtuples.
                """
        for cash_dividend in cash_dividends:
            # self._dirty_stats = True  # only mark dirty if we pay a dividend

            # Store the earned dividends so that they can be paid on the
            # dividends' pay_dates.
            div_owed = positions[cash_dividend.asset].earn_dividend(
                cash_dividend,
            )
            try:
                unpaid_dividends[cash_dividend.pay_date].append(div_owed)
            except KeyError:
                unpaid_dividends[cash_dividend.pay_date] = [div_owed]

        for stock_dividend in stock_dividends:

            div_owed = positions[
                stock_dividend.asset
            ].earn_stock_dividend(stock_dividend)
            try:
                unpaid_stock_dividends[stock_dividend.pay_date].append(
                    div_owed,
                )
            except KeyError:
                unpaid_stock_dividends[stock_dividend.pay_date] = [
                    div_owed,
                ]

    def _pay_dividends(self, positions, next_trading_day, unpaid_dividends, unpaid_stock_dividends):
        """
                Returns a cash payment based on the dividends that should be paid out
                according to the accumulated bookkeeping of earned, unpaid, and stock
                dividends.
                """
        net_cash_payment = 0.0

        try:
            payments = unpaid_dividends[next_trading_day]
            # Mark these dividends as paid by dropping them from our unpaid
            del unpaid_dividends[next_trading_day]
        except KeyError:
            payments = []

        # representing the fact that we're required to reimburse the owner of
        # the stock for any dividends paid while borrowing.
        for payment in payments:
            net_cash_payment += payment['amount']

        # Add stock for any stock dividends paid.  Again, the values here may
        # be negative in the case of short positions.
        try:
            stock_payments = unpaid_stock_dividends[next_trading_day]
        except KeyError:
            stock_payments = []

        for stock_payment in stock_payments:
            payment_asset = stock_payment['payment_asset']
            share_count = stock_payment['share_count']
            # note we create a Position for stock dividend if we don't
            # already own the asset
            if payment_asset in positions:
                pos = positions[payment_asset]
            else:
                pos = positions[payment_asset] = position.Position(
                    payment_asset,
                )

            pos.amount += share_count

        return net_cash_payment

    def _update_portfolio(self, portfolio, stats):
        start_value = portfolio.portfolio_value

        # todo: the're some additional values to compute( net_value - futures maintenance margins?)
        portfolio.positions_value = position_value = stats.net_value
        portfolio.position_exposure = stats.net_exposure
        portfolio.portfolio_value = end_value = portfolio.cash + position_value

        pnl = end_value - start_value
        if start_value != 0:
            returns = pnl / start_value
        else:
            returns = 0.0

        portfolio.pnl += pnl
        portfolio.returns = ((1 + portfolio.returns) * (1 + returns) - 1)

    # todo: no need to update the account. We will be using data from the overall account.
    # just display the main account. (not the sub-account (since there is no way to determine
    # margin requirements accuretly.
    # todo: we still need to update leverage data etc.
    # note: it is used in metrics to track the maximum leverage.
    # note: the account is meant to be used by the strategy developper to track some values, and put
    # some logic based on those values. It is not used in the metrics tracker or else where...
    def _update_zp_account(self, main_account, portfolio, stats):
        # todo: this only applies to cash account or reg T margin accounts. Risk-Based margin accounts
        # are more complex to handle.
        # todo: need to handle
        # todo: what about sma ? (is used for computing liquidations etc.)
        # todo: what about buying power?
        sub_account = self._account
        portfolio_value = portfolio.portfolio_value
        cash = portfolio.cash

        sub_account.settled_cash = cash
        # sub_account.accrued_interest = ?
        sub_account.equity_with_loan = portfolio_value
        sub_account.total_positions_value = portfolio_value - cash
        sub_account.total_positions_exposure = portfolio.positions_exposure

        # positions_value = portfolio.positions_value
        #
        # main_positions_value = main_account.total_positions_value

        # #fixme: we assume that we are subjected to rule based margins... with risk based margins,
        # # each sub-portfolio has its own margin requirements (based on its risk) which adds up
        # # to the margin requirements of the main portfolio. TIMS or SPAN
        # sub_account.regt_margin = (main_account.regt_margin * positions_value) / main_positions_value
        # init_marg_pr = main_account.initial_margin_requirement / main_positions_value
        # maint_marg_pr = main_account.maintenance_margin_requirement / main_positions_value
        #
        # sub_account.initial_margin_requirement = init_mr = init_marg_pr * positions_value
        # sub_account.maintenance_margin_requirement = maint_marg_pr * positions_value
        #
        # prev_pv = 0.0 #todo: we need the previous portfolio_value.
        # #fixme: checks if it is a margin account of not? or we need to specify it externally?
        # if init_mr == 0:
        #     sub_account.buying_power = min(portfolio_value,prev_pv) - init_mr
        # else:
        #     sub_account.buying_power = (min(portfolio_value,prev_pv) - init_mr) * 4

        sub_account.available_funds = cash
        sub_account.excess_liquidity = cash
        sub_account.cushion = cash / portfolio_value if portfolio_value else np.nan

        sub_account.day_trades_remaining = main_account.day_trades_remaining

        # this is used in the metrics module.
        sub_account.net_liquidation = portfolio_value
        sub_account.gross_leverage, sub_account.net_leverage = self._calculate_period_stats(
            portfolio_value,
            stats
        )
        sub_account.leverage = sub_account.gross_leverage

    def _calculate_period_stats(self, portfolio_value, stats):
        if portfolio_value == 0:
            gross_leverage = net_leverage = np.inf
        else:
            gross_leverage = stats.gross_exposure / portfolio_value
            net_leverage = stats.net_exposure / portfolio_value

        return gross_leverage, net_leverage

    def _update_position(self, positions, asset, transaction):
        if asset not in positions:
            pos = position.Position(asset)
            positions[asset] = pos
        else:
            pos = positions[asset]
        pos.update(transaction)

        if pos.amount == 0:
            del positions[asset]

    def _cash_flow(self, portfolio, amount):
        portfolio.cash_flow += amount
        portfolio.cash += amount

    def _compute_stats(self, positions):
        calculate_position_tracker_stats(positions, self._stats)
        return self._stats


class RemoteBlotter(object):
    def __init__(self, account):
        self._account = account
        # the broker is a remote service.

    def order(self, asset, amount, style, order_id=None):
        return self._account.order(asset, amount, style, order_id)

    def batch_order(self, order_arg_lists):
        pass

    def get_transactions(self, bar_data):
        pass


class Strategy(ABC):
    '''abstract class to be implemented by the user, and will be used in the controllable
    object'''

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    def _from_proto_ts_datetime(self, proto_dt):
        return proto_dt.ToDatetime()

    @abstractmethod
    def _initialize(self, context):
        "this must be implemented"
        raise NotImplementedError

    def _handle_data(self, context, data):
        pass

    def _before_trading_starts(self):
        "these functions can be implemented by the developer, the before trading starts"
        pass

    def run(self, request, context, blotter):
        '''creates a trading_algorithm class and runs it.
        depending on the parameters, this method either runs in "back-testing mode" or live mode
        if it is live mode, it registers to the server...'''
        capital_base = request.capital_base
        start = request.start_session
        end = request.end_session
        cbl.PerformancePacket()


class ControllableCertificateFactory(crt.CertificateFactory):
    def __init__(self, url):
        super(ControllableCertificateFactory, self).__init__()
        self._url = url

    def _create_certificate(self, root_path, cert_name, key):
        # create the subject of the certificate request
        subject = crt.CertificateSubject()
        subject.common_name = 'controllable'
        subject.alternative_names = [self._url]
        # TODO: how do pod ip's,services etc work?
        # additional addresses: pod ip, pod dns, master IP...
        builder = crt.CertificateSigningRequestBuilder()
        builder.name = 'controllable'
        builder.usages = [
            'digital signature',
            'key encipherment',
            'data encipherment',
            'server auth'
        ]
        builder.groups = ['system: authenticated']
        return builder.get_certificate_signing_request(subject, key)


class _ControllableServicer(cbl_grpc.ControllableServicer):
    def __init__(self, strategy, account_url, token, ca=None):
        self._str = strategy
        self._url = account_url
        self._token = token
        self._ca = ca

    def _create_blotter(self, url, live):
        if live:
            return RemoteBlotter(srv.create_channel(url, self._ca), self._token, )
        else:
            return SimulationBlotter()

    def Run(self, request, context):
        # TODO: before running the algorithm, we must ingest the most recent data from some source.
        # the account stub must be encapsulated in a blotter
        blotter = self._create_blotter(self._url, request.live)
        self._str.run(request, context, blotter)
        '''runs the strategy'''
        raise NotImplementedError


class ControllableServer(srv.Server):
    '''encapsulates a strategy. key and certificate are generated externally'''

    def __init__(self, name, strategy, controller_url, controllable_url, key=None, certificate=None, ca=None):
        if not isinstance(strategy, Strategy):
            raise TypeError('Expected {} got {}'.format(Strategy, type(strategy)))
        self._url = controllable_url
        self._stub = ctr.ControllerStub(srv.create_channel(controller_url, ca))
        self._str = strategy
        super(ControllableServer, self).__init__(
            controllable_url,
            key,
            certificate
        )
        self._config_file = files.JsonFile('{}/config'.format(name))
        self._config = None
        self._ca = ca

    def _register(self):
        try:
            url, token = self._load_config('broker_url', 'token')
        except KeyError:
            # generate a random name. is this necessary?
            name = str(uuid4())
            response = self._stub.Register(name=name, url=self._url)
            # the url is to access the broker
            url = response.url
            token = response.token
            # store name token and url
            self._config_file.store({'broker_url': url, 'token': token, 'name': name})
        return url, token

    def _load_config(self, *names):
        conf = next(self._config_file.load())
        attrs = []
        loaded = {}
        for name in names:
            attr = loaded.setdefault(name, None)
            if attr is None:
                try:
                    n = conf[name]
                    loaded[name] = n
                except FileNotFoundError:
                    raise KeyError
                attrs.append(n)
        return attrs

    def _add_servicer_to_server(self, server):
        url, token = self._register()
        self._cbl = _ControllableServicer(self._str, url, token, self._ca)
        cbl_grpc.add_ControllableServicer_to_server(self._cbl, server)
