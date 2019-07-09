import time
import click

import grpc

from concurrent import futures


from contrib.control import controller
from contrib.control import controller_pb2_grpc as ctrl_rpc

# def create_csr(key, url):
#     '''creates a certificate request or returns a certificate if one exists...'''
#     # create the subject of the certificate request
#     subject = crt.CertificateSubject()
#     subject.common_name = 'controller'
#     subject.alternative_names = [url]
#     # TODO: how do pod ip's,services etc work?
#     # each pod gets its own ip address
#     # additional addresses: pod ip, pod dns, master IP...
#     builder = crt.CertificateSigningRequestBuilder()
#     builder.name = 'controller'
#     builder.usages = ['digital signature', 'key encipherment',
#                       'data encipherment', 'server auth']
#     builder.groups = ['system: authenticated']
#     return builder.get_certificate_signing_request(subject, key)


def start_server(address, mode):
    server = grpc.server(futures.ThreadPoolExecutor())
    server.add_insecure_port(address)
    if mode == 'TEST':
        ctrl_rpc.add_ControllerServicer_to_server(controller.TestController(), server)
    elif mode == 'PAPER':
        ctrl_rpc.add_ControllerServicer_to_server(controller.PaperController(), server)
    server.start()
    click.echo('Server listening at : {}'.format(address))
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

#TODO: maybe prompt some setup questions (like password and user_name...)
@click.group()
def cli():
    pass

#TODO: a broker can be mounted by the user (client) the broker must expose a certain
# interface. => the broker is a service... if there is no broker mounted, we use a
# simulated broker.
#TODO: should mount certificate etc. for secure communication (optional)
#todo: must pass some credentials before running the server...
@cli.command()
@click.option('--address', default='[::]:50051')
@click.option('--mode', default='SIMULATION')
def start(address):
   start_server(address)


if __name__ == '__main__':
    cli()
