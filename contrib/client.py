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
        self._working_dir = None

    def initialize(self, server_address, domain, working_dir=None):
        """For initialization: sets up the environment etc."""
        #todo: the cert_auth is either load
        self._stub = ctrl.ControllerStub(grpc.insecure_channel(server_address))

    def run(self, path):
        pass

    def register(self, username, password):
        print('Registered: {}'.format(username))



control = Control()

@click.command()
@click.option('--username', prompt='Username')
@click.password_option()
def register(username, password):
    control.register(username, password)

@click.command()
@click.option('--server_address', default='[::]:50051')
@click.option('--domain', default={'DataType':['Equity'], 'CountryCode':'US'})
def init(address, domain):
    # todo: prompt a password and username request
    control.initialize(address, domain)

def simulate(path):
    pass


if __name__ == '__main__':
    register()
    init()
