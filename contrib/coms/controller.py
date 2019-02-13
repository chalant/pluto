import uuid

import grpc

from zipline.finance.execution import MarketOrder, StopLimitOrder, StopOrder, LimitOrder

from contrib.coms.protos import controller_service_pb2 as ctl
from contrib.coms.protos import controller_service_pb2_grpc as ctl_rpc
from contrib.coms.protos import controllable_service_pb2 as cbl
from contrib.coms.protos import controllable_service_pb2_grpc as cbl_rpc
from contrib.coms.protos import broker_pb2_grpc as broker_rpc
from contrib.coms.protos import broker_pb2 as br_msg
from contrib.coms.protos import data_bundle_pb2 as dtb

from contrib.coms.utils import server_utils as srv
from contrib.coms.utils import certification as crt
from contrib.coms.utils import conversions as cv
from contrib.utils import files


class BrokerServicer(broker_rpc.BrokerServicer):
    '''encapsulates available services per-client'''

    # todo: must check the metadata...

    def __init__(self, broker, bundle_factory):
        # the bundle factory is aggregated, for caching purposes.
        self._bundle_factory = bundle_factory
        self._tokens = set()
        self._accounts = {}

        self._broker = broker

    def _check_metadata(self, context):
        metadata = dict(context.invocation_metadata())
        token = metadata['Token']
        if not token in self._tokens:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, 'The provided token is incorrect')

    def add_token(self, token):
        self._tokens.add(token)

    def add_account(self, account):
        self._accounts[account.token] = account

    def AccountState(self, request, context):
        # todo: these methods aren't necessary
        raise NotImplementedError

    def PortfolioState(self, request, context):
        raise NotImplementedError

    def Orders(self, request, context):
        self._check_metadata(context)
        for order in self._get_dict_values(self._broker.orders()):
            yield cv.to_proto_order(order)

    def _get_dict_values(self, dict_):
        return dict_.values()

    def BatchOrder(self, request_iterator, context):
        raise NotImplementedError

    def CancelAllOrdersForAsset(self, request, context):
        raise NotImplementedError

    def PositionsState(self, request, context):
        raise NotImplementedError

    def Transactions(self, request, context):
        self._check_metadata(context)
        for trx in self._get_dict_values(self._broker.transactions()):
            yield cv.to_proto_transaction(trx)

    def SingleOrder(self, request, context):
        self._check_metadata(context)
        req_style = request.style
        style = None
        if req_style == br_msg.OrderParams.MARKET_ORDER:
            style = MarketOrder()
        elif req_style == br_msg.OrderParams.LIMIT_ORDER:
            style = LimitOrder(request.limit_price)
        elif req_style == br_msg.OrderParams.STOP_ORDER:
            style = StopOrder(request.stop_price)
        elif req_style == br_msg.OrderParams.STOP_LIMIT_ORDER:
            style = StopLimitOrder(request.limit_price, request.stop_price)
        if style:
            return cv.to_proto_order(self._broker.order(cv.to_zp_asset(request.asset), request.amount, style))
        else:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'unsupported order style argument')

    def GetDataBundle(self, request, context):
        '''creates a bundle based on the specified 'domains' and sends the bundle as a stream
        of bytes.'''
        # note: returns data by chunks of 1KB by default.
        self._check_metadata(context)
        for chunk in self._bundle_factory.get_bundle(request.country_code):
            yield dtb.Bundle(data=chunk)


class BrokerServer(srv.Server):
    def __init__(self, bundle_factory, broker_url, broker, key=None, certificate=None):
        super(BrokerServer, self).__init__(broker_url, key, certificate)
        self._acc = BrokerServicer(bundle_factory, broker)

    def add_token(self, token):
        self._acc.add_token(token)

    def _add_servicer_to_server(self, server):
        broker_rpc.add_BrokerServicer_to_server(self._acc, server)


class ControllableStub(object):
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

    def run(self, start, end, max_leverage, data_frequency='daily', metrics_set='default', live=False):
        '''this function is an iterable (generator) '''

        if data_frequency == 'daily':
            df = cbl.RunParams.DAY
        elif data_frequency == 'minutely':
            df = cbl.RunParams.MINUTE
        else:
            raise ValueError('No data frequency of type {} is supported'.format(data_frequency))
        for perf in self._ctr.Run(
                cbl.RunParams(
                    capital_base=self._capital,
                    data_frequency=df,
                    start_session=cv.to_datetime(start),
                    end_session=cv.to_datetime(end),
                    metrics_set=metrics_set,
                    live=live,
                    maximum_leverage=max_leverage
                )):
            yield cv.from_proto_performance_packet(perf)


class ControllerServicer(ctl_rpc.ControllerServicer, srv.IServer):
    '''Encapsulates the controller service. This class manages a portfolio of strategies (performs tests routines,
    assigns capital etc.'''

    # TODO: upon termination, we need to save the generated urls, and relaunch the services
    # that server on those urls.
    def __init__(self, bundle_factory, broker_url, key=None, certificate=None, ca=None):
        # list of controllables (strategies)
        self._controllables = {}
        self._bundle_factory = bundle_factory
        self._key = key
        self._cert = certificate
        self._ca = ca
        self._config = files.JsonFile('controller/config')
        self._account_url = broker_url

        self._client_account = BrokerServer(bundle_factory, broker_url, key, certificate)

    # TODO: finish this function (registration of the controllable)
    def Register(self, request, context):
        '''the controllable calls this function to be registered'''
        # TODO: store the url permanently so that the client can be id-ed beyond run lifetime.

        #the controllable sends its url
        controllable_url = request.url
        client_name = request.name

        #a token is generated
        token = self._create_token(client_name, controllable_url)
        controllable = ControllableStub(
            client_name,
            self._create_channel(controllable_url)
        )

        #todo: when and how should we run the controllables? => should we schedule periodic back-testings?

        # keep track of controllables so that we can control them etc.
        self._controllables[token] = controllable
        # send the generated access url to the client (through a secure channel). The client communicate with the
        # account through this channel.
        # the client must store this url permanently, so that it can be identified
        # add token to the client so that it can recognise clients
        return ctl.RegisterReply(url=self._account_url, token=token)

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
            #create a token and store clients data...
            token = str(uuid.uuid4())
            self._config.store({client_name: {'url': client_url, 'token': token}})
        return token

    def _create_channel(self, url):
        return srv.create_channel(url, self._ca)

    def start(self):
        self._client_account.start()

    def stop(self, grace=None):
        self._client_account.stop(grace)


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
    def __init__(self, bundle_factory, controller_url, broker_url, key=None, certificate=None):
        '''the bundle_factory is an abstraction for creating data bundles.'''
        super(ControllerServer, self).__init__(controller_url, key, certificate)
        self._bdf = bundle_factory
        self._key = key
        self._cert = certificate
        self._blt = broker_url

    def _add_servicer_to_server(self, server):
        ctl_rpc.add_ControllerServicer_to_server(ControllerServicer(
            self._bdf,
            self._blt,
            self._key,
            self._cert),
            server)
