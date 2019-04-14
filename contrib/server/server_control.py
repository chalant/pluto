import click

import grpc

from contrib.coms.utils import server_utils
from contrib.coms.protos import controller_service_pb2_grpc as ctrl

#todo: on the first run of this script, the user is prompted to put a password and
# a username.

"""
client module for managing strategies etc.
"""
class Control(object):
    def __init__(self):
        self._stub = None

    def initialize(self, server_address, cert_auth=None):
        #todo: the cert_auth is either load
        self._stub = ctrl.ControllerStub(
            server_utils.create_channel(server_address, cert_auth))



control = Control()

@click.command('init')
@click.option('--server_address', default=None)
@click.option('--cert_auth', type=click.File('rb'), default=None)
def init(address, cert_auth):
    # todo: prompt a password and username request
    control.initialize(address, cert_auth)

if __name__ == '__main__':
    pass
