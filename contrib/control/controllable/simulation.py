from contrib.control.controllable import controllable

#todo: we need all the classes involved with the controllable: algo class, account, etc.

class SimulationControllable(controllable.Controllable):
    def update_account(self, data):
        #todo: updates account state
        pass

    def update_data_bundle(self, data):
        pass

    def update_calendar(self, data):
        pass

    def initialize(self, data):
        pass

    def stop(self, params):
        pass