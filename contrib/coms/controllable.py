from abc import ABC, abstractmethod

from uuid import uuid4

from grpc import RpcError

from contrib.coms.protos import controllable_pb2 as cbl
from contrib.coms.protos import controllable_pb2_grpc as cbl_grpc
from contrib.coms.protos import controller_service_pb2_grpc as ctr
from contrib.coms.protos import account_service_pb2 as tr_msg
from contrib.coms.protos import account_service_pb2_grpc as cl_rpc
from contrib.coms.utils import certification as crt
from contrib.coms.utils import server_utils as srv
from contrib.utils import files

from zipline.finance.blotter import Blotter
from zipline.finance.blotter import SimulationBlotter
from zipline.finance.execution import (MarketOrder,
                                       LimitOrder,
                                       StopOrder,
                                       StopLimitOrder)


class RemoteBlotter(Blotter):
    def __init__(self, channel, id, token, cancel_policy=None):
        super(RemoteBlotter, self).__init__(cancel_policy)
        self._stub = cl_rpc.ClientAccountStub(channel)
        self._token = token
        self._id = id

    def _add_metadata(self, id, token, rpc, params):
        try:
            rpc.with_call(params, metadata=(('Token', token), ('ID', id)))
        except RpcError:
            #todo: try to register
            pass

    def _create_order_param(self, style, amount):
        t = type(style)
        if t is MarketOrder:
            return tr_msg.OrderParams(style=tr_msg.OrderParams.MARKET_ORDER,
                                      amount=amount)
        elif t is LimitOrder:
            return tr_msg.OrderParams(style=tr_msg.OrderParams.LIMIT_ORDER,
                                      amount=amount,
                                      limit_price=t.get_limit_price(True))
        elif t is StopOrder:
            return tr_msg.OrderParams(style=tr_msg.OrderParams.STOP_ORDER,
                                      amount=amount,
                                      stop_price=t.get_stop_price(True))
        elif t is StopLimitOrder:
            return tr_msg.OrderParams(style=tr_msg.OrderParams.STOP_LIMIT_ORDER,
                                      amount=amount,
                                      limit_price=t.get_limit_price(True),
                                      stop_price=t.get_stop_price(True))
        else:
            raise NotImplementedError

    def order(self, asset, amount, style, order_id=None):
        self._add_metadata(
            self._id,
            self._token,
            self._stub.SingleOrder,
            self._create_order_param(style, amount)
        )

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

    def set_account(self, account):
        '''Bind an account to this strategy. This is called by the controllable class'''
        pass

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

    def run(self, request, context):
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
            return RemoteBlotter(srv.create_channel(url, self._ca), self._token)
        else:
            return SimulationBlotter()

    def Run(self, request, context):
        # TODO: before running the algorithm, we must ingest the most recent data from some source.
        # the account stub must be encapsulated in a blotter
        self._str.set_account(self._create_blotter(self._url, request.live))
        self._str.run(request, context)
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
            url, token = self._load_config('account_url', 'token')
        except KeyError:
            name = str(uuid4())
            response = self._stub.Register(name=name, url=self._url)
            url = response.url
            token = response.token
            # store name token and url
            self._config_file.store({'account_url': url, 'token': token, 'name': name})
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
