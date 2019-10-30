from contrib.control.broker import broker

from protos import broker_pb2

class SimulationBroker(broker.Broker):
    def _update_account(self, dt, sessions):
        #send an empty brokers state
        data = broker_pb2.BrokerState().SerializeToString()
        for session in sessions:
            session.update_account(data)

    def _liquidate_one(self, session):
        session._liquidate()