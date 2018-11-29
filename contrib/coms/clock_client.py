import datetime as dt

from contrib.coms.protos import clock_pb2 as cl
from contrib.coms.protos import clock_pb2_grpc as cl_rpc

class Clock(object):
	def __init__(self,channel):
		self._stub = cl_rpc.ClockStub(channel)

		#variables to adjust time...
		self._time_skew = 0.0
		self._delay = 0.0
		self._synchronize_time()

	def __iter__(self):
		#Will generate events following the clock...
		pass

	def _synchronize_time(self):
		i = 0
		sync_dt = None
		server_dt = None
		for reply in self._stub.Sync(cl.SyncRequest()):
			if i==0:
				sync_dt = dt.datetime.utcnow()
			else:
				server_dt = reply.timestamp.ToDatetime()
			i += 1

		self._time_skew = server_dt - sync_dt
		delay_request_dt = dt.datetime.utcnow() + self._time_skew
		delay_reply_dt = self._to_datetime(self._stub.Delay(cl.DelayRequest).timestamp)
		self._delay = (delay_reply_dt - delay_request_dt) / 2

	def _adjust_time(self,datetime):
		return datetime + self._time_skew + self._delay

	def _to_datetime(self,protos_timestamp):
		return protos_timestamp.ToDatetime()






