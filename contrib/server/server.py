import time
import logbook

import click

import grpc

from concurrent import futures

from contrib.coms.protos import controller_service_pb2_grpc as ctl_rpc


class ControllerServicer(ctl_rpc.ControllerServicer):
    def __init__(self):
        pass

    def Register(self, request, context):
        pass

    def ReceivePerformancePacket(self, request, context):
        pass

    def Stop(self, request, context):
        pass

    def Start(self, request, context):
        pass



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


def start_server(address):
    server = grpc.server(futures.ThreadPoolExecutor(10))
    server.add_insecure_port(address)
    ctl_rpc.add_ControllerServicer_to_server(ControllerServicer(), server)
    server.start()
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

#todo: must pass some credentials before running the server...
@click.command('start')
@click.option('--address', default='[::]:50051')
def start_command(address):
   start_server(address)


if __name__ == '__main__':
    start_command()
