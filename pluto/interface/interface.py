import secrets

import grpc

from pluto.explorer import explorer
from pluto.controller import controllerservice
from pluto.interface.utils import grpc_interceptors as interceptors
from pluto.interface import manager, credentials
from pluto.interface.utils import security
from pluto.server import service

from protos import interface_pb2 as itf_msg
from protos import interface_pb2_grpc as interface
from protos import controller_pb2_grpc as ctl


class Gateway(interface.GatewayServicer, service.Service):
    def __init__(self, credentials, directory):
        '''

        Parameters
        ----------
        credentials: contrib.control.interface.credentials.Credentials
                    the initial credentials
        directory: contrib.interface.directory.Directory
        '''

        self._credentials = credentials
        self._directory = drt = directory

        self._monitor = manager.Manager(drt)
        self._explorer = explorer.Explorer(drt)
        self._controller = controllerservice.ControllerService(drt)

        self._logged_in = False

        self._authenticity = interceptors.get_interceptors('authenticity')
        self._availability = interceptors.get_interceptors('availability')

    def Login(self, request, context):
        # todo: logout after some inactivity
        # create a thread that logs out the client after a timeout is reached
        # we need to reset the timeout each time the authenticated client performs an action
        # for now, the service only accepts one client at a time.
        # for multiple clients we would need another structure (sessions)
        if not self._logged_in:
            try:
                self._check_credentials(request.username, request.password)
                token = self._create_token()  # a new token is created each successful login.

                # set token in the validators
                for validator in self._authenticity:
                    validator.token = token
                self._logged_in = True
                return itf_msg.LoginResponse(token=token)
            except ValueError:
                context.abort(grpc.StatusCode.UNAUTHENTICATED)
        else:
            return itf_msg.LoginResponse()

    def Logout(self, request, context):
        self._logout()

    def _logout(self):
        # remove tokens
        for interceptor in self._authenticity:
            interceptor.token = None

        # set availability to false
        for interceptor in self._availability:
            interceptor.available = False

        self._logged_in = False

    def stop(self):
        self._logout()

    def set_server(self, server):
        interface.add_GatewayServicer_to_server(self, server)
        interface.add_MonitorServicer_to_server(self._monitor, server)
        interface.add_ExplorerServicer_to_server(self._explorer, server)
        ctl.add_ControllerServicer_to_server(self._controller, server)

    def get_interceptors(self):
        return self._authenticity + self._availability

    def _check_credentials(self, username, password):
        with credentials.read() as r:
            crd = r.query(credentials.Credentials) \
                .filter_by(username=username).one()
            if crd:
                salt = crd.salt
                hash_ = crd.hash_
                if hash_ != security.get_hash(salt, password):
                    raise ValueError('Not signed in')
            else:
                raise ValueError('Not signed in')

    def _create_token(self):
        return secrets.token_hex()
