import uuid

import grpc

from google.protobuf import timestamp_pb2 as prt

from contrib.coms.protos import params_pb2
from contrib.coms.protos import controller_service_pb2 as ctl
from contrib.coms.protos import controller_service_pb2_grpc as ctl_rpc
from contrib.coms.protos import controllable_pb2 as cbl
from contrib.coms.protos import controllable_pb2_grpc as cbl_rpc
from contrib.coms.protos import account_service_pb2_grpc as ta_rpc
from contrib.coms.protos import data_bundle_pb2 as dtb
from contrib.coms.utils import server_utils as srv
from contrib.coms.utils import certification as crt
from contrib.utils import files

from zipline.finance import position

from collections import defaultdict

class Ledger(object):
    def __init__(self, capital_base, data_frequency):
        pass

class Account(object):
    '''should sit on top of the broker in order to "isolate" each virtual account from
    the main account...
    keeps track of its own portfolio etc.
    this account must be updated at some frequency ex: each minute makes some calls to the
    broker so that it gets updated... at each call, each field gets updated before delivering the
    data...'''
    #todo: should keep track of time so that we know when to update the values of the fields
    # ex: update each minute... or we could update at each call? or when stuff has changed
    # ex: when an order was made, at the next call for portfolio we will update the portfolio
    # values
    #todo: we need a way to update the account at some frequency
    # (process the transactions etc.)
    def __init__(self, id, token, capital, broker):
        self._id = id
        self._token = token
        self._broker = broker

        #keep track of the orders of this account
        self._orders = {}
        self._open_orders = defaultdict(dict)

        self._new_orders = []

        self._must_sync = False

        #to keep track of executed orders
        self._processed_transactions = []

        self._closed_orders = []

        self._positions = {}

    def order(self, asset, amount, style, order_id=None):
        order = self._broker.order(asset, amount, style)
        self._new_orders.append(order)
        self._open_orders[asset][order_id] = order
        #tag as must sync, since we placed an order, next time each property is called, we
        # must update the values of the account.
        self._must_sync = True
        return order.id

    @property
    def orders(self):
        return self._orders

    def _check_sync(self):
        if self._must_sync:
            self._process_transactions()

    def _update_portfolio(self):
        pass

    @property
    def portfolio(self):
        return self._ledger.portfolio

    @property
    def positions(self):
        self._check_sync()
        raise NotImplementedError

    @property
    def transactions(self):
        self._check_sync()
        return self._processed_transactions

    def _process_transactions(self):
        #these should be the executed transactions of this accounts orders
        transactions = list(self._broker.transactions.values())
        #todo: update the account as-well.
        #todo: update the portfolio
        #we call the protected orders attribute to avoid re-updating the orders
        #todo: check if we have to add commission to the order or if it is already done
        # by the broker.
        positions = self._broker._tws.positions

        #todo: PROBLEM: not sure how to process last_trade_price (from transaction or ticker
        # price? => apparently, we need to use the ticker data. PROBLEM: we need to be
        # subscribed to an asset (which is limited using ib...) => There are "streaming" sources...
        # since they are "push" types, they call some functions in order to update our data...
        # right now we have two sources: ib and tiingo for real-time data...
        # we need to add a data streaming feature, because we need it for updating the position
        # the last sale prices are synced by using data from the data portal. But, by processing
        # transactions, we might also need to fetch the last sale prices. Put the streaming sources
        # in a custom DataPortal

        self._processed_transactions = transactions

        for order in self._broker.orders.values():
            order_id = order.id
            asset = order.asset
            symbol = asset.symbol
            if order_id in self._orders:
                #remove any closed order
                if not order.open:
                    del self._orders[order_id]
                    del self._positions[asset]
                #update our orders with the new values
                else:
                    self._orders[order_id] = order
                    ib_position = positions[symbol]
                    self._positions[asset] = self._create_position(asset, ib_position)

    def _create_position(self, asset, ib_position):
        pos = position.Position(asset)
        pos.amount = int(ib_position.position)
        pos.cost_basis = float(ib_position.average_cost)

        #todo: setup a ticker streaming so that we can fetch data for updates (last_trade_price,
        # last_trade_dt)
        #pos.last_sale_price = source.last_sale_price
        #pos.last_sale_date = source.last_sale_date
        return pos

    @property
    def account(self):
        raise NotImplementedError

    @property
    def token(self):
        return self._token

    @property
    def id(self):
        return self._id


class ClientAccountServicer(ta_rpc.ClientAccountServicer):
    '''encapsulates available services per-client'''
    #todo: must check the metadata...

    def __init__(self, bundle_factory):
        # the bundle factory is aggregated, for cashing purposes.
        self._bundle_factory = bundle_factory
        self._tokens = set()
        self._accounts = {}

    def _check_metadata(self, context):
        metadata = dict(context.invocation_metadata())
        token = metadata['Token']
        if not token in self._accounts:
            context.abort(grpc.StatusCode.PERMISSION_DENIED,'The provided token is incorrect')
            return None
        else:
            return token

    def set_blotter(self,blotter):
        #TODO: set the blotter that is responsible of handling trade requests etc.
        #is dynamically set, depending on the modes we want to trade-in (simulation,paper-trade,live-trade)
        self._blotter = blotter

    def add_token(self, token):
        self._tokens.add(token)

    def add_account(self, account):
        self._accounts[account.token] = account

    def AccountState(self, request, context):
        self._check_metadata(context)

    def PortfolioState(self, request, context):
        return self._accounts[self._check_metadata(context)].portfolio

    def Orders(self, request, context):
        pass

    def BatchOrder(self, request_iterator, context):
        pass

    def CancelAllOrdersForAsset(self, request, context):
        pass

    def PositionsState(self, request, context):
        pass

    def Transactions(self, request, context):
        pass

    def SingleOrder(self, request, context):
        #TODO: need to parse the request into zipline objects (ex: Asset object)
        self._accounts[self._check_metadata(context)].order()

    def GetDataBundle(self, request, context):
        '''creates a bundle based on the specified 'domains' and sends the bundle as a stream
        of bytes.'''
        # note: returns data by chunks of 1KB by default.
        for chunk in self._bundle_factory.get_bundle(request.country_code):
            yield dtb.Bundle(data=chunk)


class AccountServer(srv.Server):
    def __init__(self, bundle_factory, account_url, key=None, certificate=None):
        #todo: must check the metadata
        super(AccountServer, self).__init__(account_url, key, certificate)
        self._acc = ClientAccountServicer(bundle_factory)

    def add_token(self, token):
        self._acc.add_token(token)

    @property
    def blotter(self):
        return self._acc._blotter

    def _add_servicer_to_server(self, server):
        ta_rpc.add_ClientAccountServicer_to_server(self._acc,server)

    @blotter.setter
    def blotter(self,value):
        self._acc.set_blotter(value)



class Controllable(object):
    '''encapsulates utilities for remotely controlling a strategy.'''

    def __init__(self, name, controllable_channel):
        self._name = name
        self._capital = 0.0
        # reference to the controllable so that we can remotely control it
        self._ctr = cbl_rpc.ControllableStub(controllable_channel)
        # each strategy controller has a server that listens to some generated port...
        # one client account per server.

    @property
    def capital(self):
        return self._capital

    @capital.setter
    def capital(self, value):
        self._capital = value

    def _datetime_to_timestamp_pb(self, datetime):
        ts = prt.Timestamp()
        ts.FromDatetime(datetime)
        return ts

    def run(self, start, end, data_frequency='daily', metrics_set='default', live=False):
        '''this function is an iterable (generator) '''
        start_pr = self._datetime_to_timestamp_pb(start)
        end_pr = self._datetime_to_timestamp_pb(end)
        if data_frequency == 'daily':
            df = params_pb2.RunParams.DAY
        elif data_frequency == 'minutely':
            df = params_pb2.RunParams.MINUTE
        else:
            raise ValueError('No data frequency of type {} is supported'.format(data_frequency))
        for perf in self._ctr.Run(
                params_pb2.RunParams(
                    capital_base=self._capital,
                    data_frequency=df,
                    start_session=start_pr,
                    end_session=end_pr,
                    metrics_set=metrics_set,
                    live=live)):
            yield perf


class ControllerServicer(ctl_rpc.ControllerServicer, srv.IServer):
    '''Encapsulates the controller service. This class manages a portfolio of strategies (performs tests routines,
    assigns capital etc.'''

    # TODO: upon termination, we need to save the generated urls, and relaunch the services
    # that server on those urls.
    def __init__(self, bundle_factory, account_url, key=None, certificate=None, ca = None):
        # list of controllables (strategies)
        self._controllables = []
        self._bundle_factory = bundle_factory
        self._key = key
        self._cert = certificate
        self._config = files.JsonFile('controller/config')
        self._account_url = account_url
        self._client_account = AccountServer(bundle_factory, account_url, key, certificate)
        self._ca = ca

    # TODO: finish this function (registration of the controllable)
    def Register(self, request, context):
        '''the controllable calls this function to be registered'''
        # clients to the controller first register here to be controlled.
        # add the controllable to list...
        # create the strategy url based on name, also resolve name conflicts
        # TODO: store the url permanently so that the client can be id-ed beyond run lifetime.
        # The url uniquely identifies the strategy. => All metrics can be stored in the
        # corresponding url folder? database?
        client_url = request.url
        client_name = request.name
        token = self._create_token(client_name, client_url)
        controllable = Controllable(
            client_name,
            self._create_channel(client_url)
        )
        # keep track of controllables so that we can control them etc.
        self._controllables.append(controllable)
        # send the generated access url to the client (through a secure channel). The client communicate with the
        # account through this channel.
        # the client must store this url permanently, so that it can be identified
        #add token to the client so that it can recognise clients
        self._client_account.add_token(token)
        return ctl.RegisterReply(url=self._account_url,token = token)

    def _load_config(self, name):
        try:
            return next(self._config.load())[name]
        except FileNotFoundError:
            raise KeyError

    def _create_token(self, client_name, client_url):
        '''creates a url and maps it to the client url (which is the key)'''
        try:
            conf = self._load_config(client_name)
            return conf['token']
        except KeyError:
            token = str(uuid.uuid4())
            self._config.store({client_name: {'url' : client_url, 'token': token}})
        return token

    def _create_channel(self, url):
        return srv.create_channel(url,self._ca)


class ControllerCertificateFactory(crt.CertificateFactory):
    def __init__(self, url):
        super(ControllerCertificateFactory, self).__init__()
        self._url = url

    def _create_certificate(self, root_path, cert_name, key):
        '''creates a certificate request or returns a certificate if one exists...'''
        # create the subject of the certificate request
        subject = crt.CertificateSubject()
        subject.common_name = 'controller'
        subject.alternative_names = [self._url]
        # TODO: how do pod ip's,services etc work?
        # additional addresses: pod ip, pod dns, master IP...
        builder = crt.CertificateSigningRequestBuilder()
        builder.name = 'controller'
        builder.usages = ['digital signature',
                          'key encipherment',
                          'data encipherment',
                          'server auth']
        builder.groups = ['system: authenticated']
        return builder.get_certificate_signing_request(subject, key)


class ControllerServer(srv.Server):
    def __init__(self, bundle_factory, controller_url, blotter_url, key=None, certificate=None):
        '''the bundle_factory is an abstraction for creating data bundles.'''
        super(ControllerServer, self).__init__(controller_url, key, certificate)
        self._bdf = bundle_factory
        self._key = key
        self._cert = certificate
        self._blt = blotter_url

    def _add_servicer_to_server(self, server):
        ctl_rpc.add_ControllerServicer_to_server(ControllerServicer(
            self._bdf,
            self._blt,
            self._key,
            self._cert),
            server)
