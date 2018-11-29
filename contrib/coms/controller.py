import grpc

from contrib.coms.protos import params_pb2
from contrib.coms.protos import controller_pb2 as ctl
from contrib.coms.protos import controller_pb2_grpc as ctl_rpc
from contrib.coms.protos import controllable_pb2 as cbl
from contrib.coms.protos import controllable_pb2_grpc as cbl_rpc
from contrib.coms.protos import trade_account_pb2 as ta_msg
from contrib.coms.protos import trade_account_pb2_grpc as ta_rpc


class TradeAccount(ta_rpc.TradeAccountServicer):
	def AccountState(self, request, context):
		pass

	def PortfolioState(self, request, context):
		pass

	def Orders(self, request, context):
		pass

	def BatchOrder(self, request_iterator, context):
		pass

	def CancelAllOrdersForAsset(self, request, context):
		pass

	def PositionsState(self, request, context):
		pass

	def Transactions(self, request, context):
		pass

	def SingleOrder(self, request, context):
		pass


class StrategyController(object):
	'''encapsulates a the strategy service... also provides a service to the client
	for placing trades?
	should also synchronize the clients with the server clock...'''

	def __init__(self, name, controllable_stub, server_channel):
		self._name = name
		self._capital = 0.0
		# connects to the controllable using the provided channel
		self._ctr = controllable_stub
		# each strategy controller has a server that listens to some generated port...
		self._server = grpc.server()
		ta_rpc.add_TradeAccountServicer_to_server(TradeAccount(), self._server)

	@property
	def capital(self):
		return self._capital

	@capital.setter
	def capital(self, value):
		self._capital = value

	def run(self, frequency):
		'''this should be non-blocking so that the controller can fire multiple
		clients at once...'''
		self._ctr.Run(params_pb2.RunParams(capital_base=self._capital))


# TODO: use tornado?
class Controller(ctl_rpc.ControllerServicer):
	def __init__(self, address, channel_creator):
		self._controllables = []
		self._channel_creator = channel_creator
		self._server = grpc.server()
		self._server.add_insecure_port(channel_creator(address))

	def Register(self, request, context):
		# add the controllable to list...
		name = request.name
		# create the strategy url based on name, also resolve name conflicts
		url = self._create_controllable_url(name)
		self._controllables.append(
			StrategyController(
				name,
				cbl_rpc.ControllableStub(
					self._create_channel(request.url)
				),
				self._channel_creator(url)
			)
		)
		# send the generated access url to the client
		return ctl.RegisterReply(url=url)

	def _create_controllable_url(self, name):
		'''creates a unique url base on the name/id of the client'''
		return ''

	def _create_channel(self, url):
		'''create a channel, which is secure or not, depending on the environment variables'''
		return ''
