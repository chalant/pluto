from contrib.coms.protos import controllable_service_pb2_grpc as cbl_grpc
from contrib.coms.utils import server_utils as srv

from zipline.finance.blotter import SimulationBlotter

class ControllableServicer(cbl_grpc.ControllableServicer):
    def __init__(self, strategy, cert_auth=None):
        self._str = strategy
        self._ca = cert_auth

    def _create_blotter(self, url, live):
        if live:
            return RemoteBlotter(srv.create_channel(url, self._ca), self._token, )
        else:
            return SimulationBlotter()

    def Run(self, request, context):
        # TODO: before running the algorithm, we must ingest the most recent data from some source.
        # the account stub must be encapsulated in a blotter
        #the url for accessing the broker.
        url = request.broker_url
        blotter = self._create_blotter(url, request.live)
        self._str.run(request, context, blotter)
        '''runs the strategy'''
        raise NotImplementedError

    def Stop(self, request, context):
        raise NotImplementedError