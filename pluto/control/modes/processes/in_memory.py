from pluto.control.modes.processes import process_factory
from pluto.control.controllable import server


class InMemoryProcess(process_factory.Process):
    def _create_controllable(self, framework_url, session_id):
        return server.ControllableService(framework_url)

    def _initialize(self, controllable, iterator):
        controllable.Initialize(iterator, ())

    def _clock_update(self, controllable, clock_event):
        controllable.ClockUpdate(clock_event, ())

    def _parameter_update(self, controllable, params):
        controllable.UpdataParameters(params)

    def _account_update(self, controllable, iterator):
        controllable.AccountUpdate(iterator, ())

    def _stop(self, controllable, param):
        controllable.Stop(param, ())


class InMemoryProcessFactory(process_factory.ProcessFactory):
    def _create_process(self, framework_url, session_id):
        return InMemoryProcess(framework_url, session_id)
