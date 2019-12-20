# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from protos import data_pb2 as protos_dot_data__pb2
from protos import interface_pb2 as protos_dot_interface__pb2


class GatewayStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.Login = channel.unary_unary(
        '/Gateway/Login',
        request_serializer=protos_dot_interface__pb2.LoginRequest.SerializeToString,
        response_deserializer=protos_dot_interface__pb2.LoginResponse.FromString,
        )
    self.Logout = channel.unary_unary(
        '/Gateway/Logout',
        request_serializer=protos_dot_interface__pb2.LogoutRequest.SerializeToString,
        response_deserializer=protos_dot_interface__pb2.LogoutResponse.FromString,
        )


class GatewayServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def Login(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Logout(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_GatewayServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'Login': grpc.unary_unary_rpc_method_handler(
          servicer.Login,
          request_deserializer=protos_dot_interface__pb2.LoginRequest.FromString,
          response_serializer=protos_dot_interface__pb2.LoginResponse.SerializeToString,
      ),
      'Logout': grpc.unary_unary_rpc_method_handler(
          servicer.Logout,
          request_deserializer=protos_dot_interface__pb2.LogoutRequest.FromString,
          response_serializer=protos_dot_interface__pb2.LogoutResponse.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'Gateway', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))


class ManagerStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.InspectCode = channel.unary_unary(
        '/Manager/InspectCode',
        request_serializer=protos_dot_interface__pb2.CodeInspectionRequest.SerializeToString,
        response_deserializer=protos_dot_interface__pb2.CodeInspectionResponse.FromString,
        )
    self.Deploy = channel.unary_unary(
        '/Manager/Deploy',
        request_serializer=protos_dot_interface__pb2.DeployRequest.SerializeToString,
        response_deserializer=protos_dot_interface__pb2.DeploymentResponse.FromString,
        )


class ManagerServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def InspectCode(self, request, context):
    """for managing strategies: we can put a strategy into a live or a paper environment or both
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def Deploy(self, request, context):
    """locks a strategy so that it can't be modfied or overwritten
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_ManagerServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'InspectCode': grpc.unary_unary_rpc_method_handler(
          servicer.InspectCode,
          request_deserializer=protos_dot_interface__pb2.CodeInspectionRequest.FromString,
          response_serializer=protos_dot_interface__pb2.CodeInspectionResponse.SerializeToString,
      ),
      'Deploy': grpc.unary_unary_rpc_method_handler(
          servicer.Deploy,
          request_deserializer=protos_dot_interface__pb2.DeployRequest.FromString,
          response_serializer=protos_dot_interface__pb2.DeploymentResponse.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'Manager', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))


class ExplorerStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.StrategyList = channel.unary_stream(
        '/Explorer/StrategyList',
        request_serializer=protos_dot_interface__pb2.StrategyFilter.SerializeToString,
        response_deserializer=protos_dot_interface__pb2.StrategyResponse.FromString,
        )


class ExplorerServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def StrategyList(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_ExplorerServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'StrategyList': grpc.unary_stream_rpc_method_handler(
          servicer.StrategyList,
          request_deserializer=protos_dot_interface__pb2.StrategyFilter.FromString,
          response_serializer=protos_dot_interface__pb2.StrategyResponse.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'Explorer', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))


class HubStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.GetDirectory = channel.unary_stream(
        '/Hub/GetDirectory',
        request_serializer=protos_dot_interface__pb2.DirectoryRequest.SerializeToString,
        response_deserializer=protos_dot_data__pb2.Chunk.FromString,
        )
    self.StoreDirectory = channel.stream_unary(
        '/Hub/StoreDirectory',
        request_serializer=protos_dot_data__pb2.Chunk.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )


class HubServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def GetDirectory(self, request, context):
    """service for getting and updating the strategy directory
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def StoreDirectory(self, request_iterator, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_HubServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'GetDirectory': grpc.unary_stream_rpc_method_handler(
          servicer.GetDirectory,
          request_deserializer=protos_dot_interface__pb2.DirectoryRequest.FromString,
          response_serializer=protos_dot_data__pb2.Chunk.SerializeToString,
      ),
      'StoreDirectory': grpc.stream_unary_rpc_method_handler(
          servicer.StoreDirectory,
          request_deserializer=protos_dot_data__pb2.Chunk.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'Hub', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
