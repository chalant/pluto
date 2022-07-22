from pluto.finance.broker import broker
from pluto.finance.blotter import simulation_blotter

# todo: should we classify blotters by calendar/clock?


class SimulationBroker(broker.Broker):
    def __init__(self, engine_setup, cancel_policy=None):
        super(SimulationBroker, self).__init__()

        self._blotters = {}

        self._blotter = simulation_blotter.SimulationBlotter(
            engine_setup,
            cancel_policy
        )

    def add_blotter(self, calendar):
        self._blotters[calendar] = simulation_blotter.SimulationBlotter(
            self._engine_setup,
            self._cancel_policy
        )

    def session_start(self, dt):
        pass

    def every_bar(self, dt):
        pass

    def session_end(self):
        pass

    def _update(self, dt, evt, calendar):
        blotter = self._blotters[calendar]

        if evt == SESSION_START:
            blotter.process_splits()
        elif evt == BAR:
            pass

    def order(self, session_id, order):
        return

    def cancel(self, session_id, order_id, relay_status=True):
        pass

    def cancel_all_orders_for_asset(self, session_id, asset, warn=False, relay_status=True):
        pass

    def reject(self, session_id, order_id, reason=''):
        pass

    def hold(self, session_id, order_id, reason=''):
        pass

    def execute_cancel_policy(self, session_id, event):
        pass
