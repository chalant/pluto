import secrets

from concurrent import futures

import grpc

import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from protos import interface_pb2 as itf_msg
from protos import interface_pb2_grpc as itf_rpc

from contrib.interface import editor
from contrib.interface.utils import grpc_interceptors as interceptors
from contrib.interface import monitor, directory, credentials
from contrib.interface.utils import db_utils as utils, security

_METADATA_FILE = 'metadata'
_DIRECTORY = 'Strategies'

metadata = sa.MetaData()
Base = declarative_base(metadata=metadata)
engine = utils.create_engine(_METADATA_FILE, metadata, _DIRECTORY)
Session = utils.get_session_maker(engine)


class StrategyMetadata(Base):
    __tablename__ = 'directory_metadata'

    id = sa.Column(sa.String, primary_key=True, nullable=False)
    name = sa.Column(sa.String)
    directory_path = sa.Column(sa.String, nullable=False)
    locked = sa.Column(sa.Boolean, nullable=False)

class Account(itf_rpc.GatewayServicer):
    def __init__(self, credentials):
        '''

        Parameters
        ----------
        credentials: contrib.control.interface.credentials.Credentials
                    the initial credentials
        '''

        self._credentials = credentials
        self._directory = drt = directory.Directory(_DIRECTORY, Session)

        self._editor = edt = editor.Editor(drt)
        self._monitor = mtr = monitor.Monitor(drt)


        self._logged_in = False

        self._authenticity = ath = interceptors.get_interceptors('authenticity')
        self._availability = avl = interceptors.get_interceptors('availability')

        self._server = server = grpc.server(
            futures.ThreadPoolExecutor(),
            interceptors=(ath + avl))

        itf_rpc.add_EditServicer_to_server(edt, server)
        itf_rpc.add_MonitorServicer_to_server(mtr, server)

    def Login(self, request, context):
        #todo: logout after some inactivity
        # create a thread that logs out the client after a timeout is reached
        # we need to reset the timeout each time the authenticated client performs an action
        if not self._logged_in:
            try:
                self._check_credentials(request.username, request.password)
                token = self._create_token()  # a new token is created each successful login.
                self._directory = drt = directory.Directory(_DIRECTORY, Session)
                self._editor = editor.Editor(drt)
                self._monitor = monitor.Monitor(drt)

                for validator in self._authenticity:
                    validator.token = token
                self._logged_in = True
                return itf_msg.LoginResponse(token=token)
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
        for interceptor in self._authenticity:
            interceptor.token = None

        #set availability to false
        for interceptor in self._availability:
            interceptor.available = False

        self._logged_in = False

    def start(self, port, address='localhost:'):
        self._server.start(address+port)

    def stop(self, grace=0):
        self.logout()
        self._server.stop(grace)

    def _check_credentials(self, username, password):
        with credentials.read() as r:
            crd = r.query(credentials.Credentials)\
                .filter_by(username=username).one()
            if crd:
                salt = crd.salt
                hash_ = crd.hash_
                if hash_ != security.get_hash(salt, crd.password):
                    raise ValueError
            else:
                raise ValueError

    def _create_token(self):
        return secrets.token_hex()