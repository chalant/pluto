import secrets

from concurrent import futures

import grpc

import sqlalchemy as sa
from sqlalchemy import orm

from protos import interface_pb2 as itf_msg
from protos import interface_pb2_grpc as itf_rpc

from contrib.control.interface import directory_db_schema as sch
from contrib.control.interface import editor, monitor
from contrib.control.interface import authentication as auth
from contrib.control.interface import paths
from contrib.control.interface import directory

DB_URI = 'sqlite:///{}'

_METADATA_FILE = 'metadata.sqlite'
_DIRECTORY = 'Strategies'

try:
    engine = sa.create_engine(DB_URI.format(paths.get_file(_METADATA_FILE, _DIRECTORY)))
except FileNotFoundError:
    engine = sa.create_engine(DB_URI.format(paths.create_file(_METADATA_FILE, _DIRECTORY)))
    sch.metadata.create_all(engine)

session = orm.sessionmaker(engine)

class Account(itf_rpc.GatewayServicer):
    def __init__(self):
        self._editor = None
        self._monitor = None
        self._logged_in = False

        ss_validator = auth.CredentialsValidator(grpc.stream_stream_rpc_method_handler)
        su_validator = auth.CredentialsValidator(grpc.stream_unary_rpc_method_handler)
        us_validator = auth.CredentialsValidator(grpc.unary_stream_rpc_method_handler)
        uu_validator = auth.CredentialsValidator(grpc.unary_unary_rpc_method_handler)

        self._server = grpc.server(futures.ThreadPoolExecutor(),
                                   interceptors=(
                                       ss_validator,
                                       su_validator,
                                       us_validator,
                                       uu_validator))

        self._validators = (ss_validator, su_validator, us_validator, uu_validator)

    def Login(self, request, context):
        #todo: logout after some inactivity
        # create a thread that logs out the after a timeout is reached
        # we need to reset the timeout each time the authenticated client performs an action
        if not self._logged_in:
            try:
                token = self._create_token()  # a new token is created each successful login.
                server = self._server
                self._check_credentials(request.username, request.password)
                self._directory = drt = directory.Directory(_DIRECTORY, session)
                self._editor = edt = editor.Editor(drt)
                self._monitor = mtr = monitor.Monitor(drt)
                itf_rpc.add_EditServicer_to_server(edt, server)
                itf_rpc.add_MonitorServicer_to_server(mtr, server)
                for validator in self._validators:
                    validator.token = token
                self._logged_in = True
                return itf_msg.LoginResponse(token=token) #todo: generate token
            except ValueError:
                context.abort(grpc.StatusCode.PERMISSION_DENIED)
        else:
            return itf_msg.LoginResponse()


    def Logout(self, request, context):
        self.logout()

    def logout(self):
        #closes everything
        self._editor.close()
        self._monitor.close()

        self._editor = None
        self._monitor = None

        #remove tokens from validators
        for validator in self._validators:
            validator.token = None

        self._logged_in = False

    def start(self, port, address='localhost'):
        self._server.start(port)

    def stop(self, grace=0):
        self.logout()
        self._server.stop(grace)

    def _check_credentials(self, username, password):
        pass

    def _create_token(self):
        return secrets.token_hex()