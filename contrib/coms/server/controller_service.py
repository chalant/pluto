from contrib.coms.utils import server_utils as su
from contrib.coms.protos import controller_service_pb2 as ctl_msg
from contrib.coms.protos import controller_service_pb2_grpc as ctl_rpc
from contrib.coms.protos import controllable_service_pb2_grpc as cbl_rpc
import grpc

from concurrent import futures

import uuid

class ControllerService(ctl_rpc.ControllerServicer):
    """controls controllables, has multiple clocks"""
    def __init__(self, cert_auth=None):
        self._controllables = {}
        self._servers = {}
        self._cert_auth = cert_auth

    def Register(self, request, context):
        pass

    def ReceivePerformancePacket(self, request, context):
        pass