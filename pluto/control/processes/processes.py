import abc
import hashlib

from pandas import Timestamp

from grpc import RpcError

import structs

from google.protobuf import empty_pb2 as emp

from dev import events

from pluto.coms.utils import conversions

from pluto.interface.utils import paths
from pluto.interface.utils.service_access import _framework_id

from protos import controllable_pb2 as cbl
from protos import clock_pb2


class ProcessFactory(abc.ABC):
    def create_process(self, session_id, framework_url):
        return self._create_process(
            framework_url,
            session_id,
            paths.root())

    @abc.abstractmethod
    def _create_process(self, framework_url, session_id, root_dir):
        raise NotImplementedError

    def set_monitor_service(self, monitor_service):
        pass

    def set_broker_service(self, broker_service):
        pass


# todo: add logging for all process failures
# todo: should there be a limit for recovery attempts?
# todo: recovery must not block

class Process(object):
    __slots__ = ['_process_id', '_controllable']

    def __init__(self, process_id, controllable):
        self._process_id = process_id
        self._controllable = controllable

    @property
    def process_id(self):
        return self._process_id

    def parameter_update(self, params):
        return self._controllable.ParameterUpdate(params, ())

    def clock_update(self, clock_event):
        return self._controllable.ClockUpdate(
            clock_pb2.ClockEvent(
                timestamp=conversions.to_proto_timestamp(clock_event['ts']),
                real_ts=conversions.to_proto_timestamp(Timestamp.utcnow()),
                event=clock_event['event'],
                signals=clock_event['signals']), ())

    def account_update(self, broker_state):
        return self._controllable.UpdateAccount(
            broker_state, ())

    def stop(self):
        # upon receiving the stop message, the controllable will
        # perform the necessary steps (liquidate positions, update its metrics
        # with data from broker, send back performance metrics). Once all that is
        # done, it will "unblock", then the process will be shutdown.
        # note: the call will be released when all the orders have been processed
        # this means that the process will still receive broker updates
        self._controllable.Stop(emp.Empty, ())
        # self._stop()

    def join(self):
        self._controllable.Join(emp.Empty, ())

    def watch(self, watcher):
        # # subscribe the watcher...
        # self._controllable.watch(watcher)

        # subscribe to monitor events for this process.
        event_name = 'monitor.{}'.format(self._process_id)

        events.add_event(event_name)

        events.subscribe(
            event_name,
            watcher.performance_update)

        # make a watch request...
        self._controllable.Watch(emp.Empty(), ())

    def stop_watching(self):
        self._controllable.StopWatching(emp.Empty(), ())

    def __hash__(self):
        return self._process_id


class ProcessInitializer(object):
    def __init__(self, controllable):
        # self._execute_events = execute_events

        self._controllable = controllable
        self._strategy_id = None

    def initialize(self, setup, strategy_id):
        # send parameters to the controllable as a stream of bytes
        self._strategy_id = strategy_id

        return self._controllable.Initialize(
            cbl.RunSetup(
                strategy_id=strategy_id),
            ())

    def start(self, setup):

        session_id = setup.session_id

        process_id = hashlib.md5(str(self._strategy_id + session_id).encode()).hexdigest()

        # convert python objects into protobuf messages

        commission_models = {
            'equity': structs.create_data(setup.commission_models['equity']),
            'futures': structs.create_data(setup.commission_models['futures'])}

        slippage_models = {
            'equity': structs.create_data(setup.slippage_models['equity']),
            'futures': structs.create_data(setup.slippage_models['futures'])
        }

        if setup.tax_models:
            tax_models = {
                'equity': structs.create_data(setup.tax_models['equity']),
                'futures': structs.create_data(setup.tax_models['futures'])
            }
        else:
            tax_models = {}

        # starts a thread
        fts = setup.fractional_trade_setup
        ts = setup.trade_setup

        self._controllable.Run(
            cbl.RunRequest(
                id=process_id,
                session_id=session_id,
                capital=setup.capital,
                leverage=setup.leverage,
                start=conversions.to_proto_timestamp(setup.start),
                end=conversions.to_proto_timestamp(setup.end),
                engine_setup=cbl.EngineSetup(
                    commission_models=commission_models,
                    slippage_models=slippage_models,
                    tax_models=tax_models),
                broker_setup=cbl.BrokerSetup(
                    fractional=setup.fractional,
                    f_trade_setup=cbl.FractionalTradeSetup(
                        minimum_purchase=fts.minimum_purchase,
                        minimal_fraction=fts.minimal_fraction,
                        order_types=fts.order_types,
                        asset_types=fts.asset_types,
                        can_short=fts.can_short),
                    trade_setup=cbl.TradeSetup(
                        order_types=ts.order_types,
                        asset_types=ts.asset_types,
                        can_short=ts.can_short),
                ),
                universe_name=setup.trading_universe,
                clear_perf=setup.clear_perf),
            ())

        return Process(process_id, self._controllable)

    # def recover(self):
    #     self._controllable = self._create_controllable(
    #         _framework_id,
    #         self._framework_url,
    #         self._session_id,
    #         self._root_dir,
    #         True,
    #         self._execute_events)

    # @abc.abstractmethod
    # def _create_controllable(self,
    #                          framework_id,
    #                          framework_url,
    #                          session_id,
    #                          root_dir,
    #                          recover=False,
    #                          execute_events=False):
    #     raise NotImplementedError
    #
    # @abc.abstractmethod
    # def _stop(self):
    #     raise NotImplementedError
