from abc import abstractmethod

from google.protobuf.timestamp_pb2 import Timestamp

import datetime as dt

from contrib.coms.protos import clock_pb2
from contrib.coms.protos import clock_pb2_grpc

class ClockServer(clock_pb2_grpc.ClockServicer):
	def __init__(self,time_source):
		self._time_source = time_source

	def Delay(self, request, context):
		return self._to_proto_timestamp(dt.datetime.utcnow())

	def Sync(self, request, context):
		'''sends to messages: one with empty timestamp and a follow-up'''
		for i in range(2):
			if i > 0:
				return clock_pb2.SyncReply(timestamp = self._to_proto_timestamp(self._time_source.get_time()))
			else:
				return clock_pb2.SyncReply()

	def _to_proto_timestamp(self,datetime):
		return Timestamp().FromDatetime(datetime)