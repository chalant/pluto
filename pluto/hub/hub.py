from protos import hub_pb2
from protos import hub_pb2_grpc

from pluto.utils import graph

class HubClient(object):
    def __init__(self):
        pass

    def get_graph(self):
        pass

#TODO: we need a base path to store things... database or file system? => abstract this
#TODO: check for credentials... (One hub per client?) => credentials are added on app launch
# or added from environment...
class Hub(hub_pb2_grpc.HubServicer):
    def __init__(self, graph_builder, graph_path):

        #todo: load the graph, empty if there is no files.
        self._builder = builder = graph_builder
        self._path = graph_path
        # todo: root is loaded from disk. If it is none, we create it.
        graph_def = graph.load_graph(graph_path)

        if not graph_def:
            self._root = root = builder.group('hub')

            self._strategies = builder.group('strategies')
            self._domains = domains = builder.group('domains')
            self._sessions = sessions = builder.group('sessions')
            self._envs = envs = builder.group('envs')

            root.add_element(domains)
            root.add_element(sessions)
            root.add_element(envs)

            # store the graph...
            graph.freeze(root, graph_path)

    def Deploy(self, request, context):
        pass

    def UploadGraph(self, request_iterator, context):
        '''receives and stores the files'''
        #todo: parse the received data...
        pass

    def DownloadGraph(self, request, context):
        pass

    def GetController(self, request, context):
        pass

    def GetDataBundle(self, request, context):
        pass
