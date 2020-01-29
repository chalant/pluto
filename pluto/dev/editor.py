import grpc

import io

from protos import interface_pb2 as itf_msg
from protos import development_pb2_grpc as dev_rpc
from protos import data_pb2

from pluto.control.modes import simulation_mode
from pluto.control.loop import simulation_loop as loop
from pluto.control import commands
from pluto.control.controller import controller

class Editor(dev_rpc.EditorServicer):
    def __init__(self, directory):
        '''

        Parameters
        ----------
        directory: contrib.interface.directory.Directory
        '''
        self._directory = directory
        self._loop = None

    def New(self, request, context):
        with self._directory.write_event() as d:
            stg = d.add_strategy(request.name)
            for b in stg.get_implementation():
                yield data_pb2.Chunk(chunk=b)

    def GetStrategy(self, request, context):
        '''

        Parameters
        ----------
        request
        context

        Returns
        -------
        typing.Generator
        '''
        with self._directory.read() as d:
            stg = d.get_strategy(request.strategy_id)

            for b in stg.get_implementation():
                yield data_pb2.Chunk(chunk=b)

    def Save(self, request_iterator, context):
        #todo: if a strategy is locked, then copy it and create a new id for this
        # strategy, so that we don't overwrite the locked strategy
        with self._directory.write_event() as d:
            buffer = ''
            for chunk in request_iterator:
                buffer += chunk.chunk
            stg = itf_msg.Strategy()
            stg.ParseFromString(buffer)
            s = d.get_strategy(stg.id)
            try:
                s.store_implementation(stg.strategy)
            except RuntimeError:
                ns = d.add_strategy(s.name)
                ns.store_implementation(stg.strategy)