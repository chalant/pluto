from abc import ABC, abstractmethod

from uuid import uuid4

from protos import controllable_service_pb2 as cbl, controller_service_pb2_grpc as ctr
from protos import controllable_service_pb2_grpc as cbl_grpc
from pluto.coms.utils import certification as crt
from pluto.coms.utils import server_utils as srv
from pluto.utils import files

from zipline.finance.blotter import SimulationBlotter


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


class ControllableMainServer(srv.MainServerFactory):
    '''encapsulates a strategy. key and certificate are generated externally'''

    def __init__(self, name, strategy, controller_url, controllable_url, key=None, certificate=None, ca=None):
        if not isinstance(strategy, Strategy):
            raise TypeError('Expected {} got {}'.format(Strategy, type(strategy)))
        self._url = controllable_url
        self._stub = ctr.ControllerStub(srv.create_channel(controller_url, ca))
        self._str = strategy
        super(ControllableMainServer, self).__init__(
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
        c = self._cbl = _ControllableServicer(self._str, url, token, self._ca)
        cbl_grpc.add_ControllableServicer_to_server(c, server)

def register_controllable(server, controller_address, contollable_address,
                          certificate_authority=None):
    pass