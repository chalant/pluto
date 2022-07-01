from dev import events

events.add_event('')


# class for orders...
class ControllerBlotter(object):
    def __init__(self, broker_service):
        self._broker_service = broker_service

    def get_broker_data(self):
        pass

