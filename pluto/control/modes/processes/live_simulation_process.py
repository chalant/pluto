from pluto.control.modes.processes import process_factory as pf

class LiveSimulationProcessFactory(pf.ProcessFactory):
    def __init__(self, process_factory):
        self._process_factory = process_factory

    def _create_process(self, framework_url, session_id, root_dir):
        pass