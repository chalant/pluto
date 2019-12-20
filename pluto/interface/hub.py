from protos import interface_pb2_grpc

class Hub(interface_pb2_grpc.HubServicer):
    def GetDirectory(self, request, context):
        pass

    def StoreDirectory(self, request_iterator, context):
        pass