from abc import ABC,abstractmethod

from contrib.coms.protos import controllable_pb2 as cr
from contrib.coms.protos import controllable_pb2_grpc as cr_grpc
from contrib.coms.protos import controller_pb2_grpc
from contrib.coms.protos import trade_account_pb2 as tr_msg
from contrib.coms.protos import trade_account_pb2_grpc as tr_rpc

class Strategy(ABC):
	'''abstract class to be implemented by the user, and will be used in the controllable
	object'''

	def set_account(self,account):
		'''Bind an account to this strategy. This is called by the controllable class'''
		pass

	@abstractmethod
	def _initialize(self,context):
		"this must be implemented"
		raise NotImplementedError

	def _handle_data(self,context,data):
		pass

	def _before_trading_starts(self):
		"these functions can be implemented by the developer, the before trading starts"
		pass

	def run(self, request, context):
		'''creates a trading_algorithm class and runs it.
		depending on the parameters, this method either runs in "backtesting mode" or live mode
		if it is live mode, it registers to the server...'''
		capital_base = request.capital_base
		cr.PerformancePacket()

class Controllable(cr_grpc.ControllableServicer):
	def __init__(self,strategy,controllable_url,controller_url):
		self._stub = controller_pb2_grpc.ControllerStub(self._create_channel(controller_url))
		self._url = controllable_url
		self._str = strategy
		self._account_stub = None

	def _register(self):
		return self._stub.Register(name = self._str.id,url = self._url)

	@abstractmethod
	def  _create_channel(self,url):
		raise NotImplementedError

	def Run(self, request, context):
		#TODO: before running the algorithm, we must ingest the most recent data from some source.
		#the account stub must be encapsulated in a blotter
		self._str.set_channel(self._create_account_stub(self._get_url(self._register()),request.live))
		capital_base = request.capital_base
		'''runs the strategy'''
		raise NotImplementedError

	def _get_url(self,register_response):
		return register_response.url

	def _create_account_stub(self,url,live):
		#TODO: create a blotter, since it is through it that metrics can be created
		#instantiates a new account stub, depending whether we are trading live or not
		if live:
			#create a live blotter and pass it the trade account stub...
			return tr_rpc.TradeAccountStub(self._create_channel(url))
		else:
			raise NotImplementedError




