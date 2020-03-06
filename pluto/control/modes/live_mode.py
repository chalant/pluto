from pluto.control.modes import mode
from pluto.control.events_log import events_log

class LiveControlMode(mode.ControlMode):
    def __init__(self, framework_url):
        super(LiveControlMode, self).__init__(framework_url, )


    def _broker_update(self, dt, evt, event_writer, broker, processes):
        '''

        Parameters
        ----------
        dt
        evt
        event_writer: pluto.control.events_log.events_log._EventsLogWriter
        broker: pluto.broker.broker.Broker
        processes: typing.Iterable[pluto.control.modes.processes.process_factory.ProcessFactory]

        '''
        # called before clock update by the loop

        # the broker_state is an object of type message
        broker_state = self._broker.update(dt, evt)
        # todo: update all processes accounts with broker data...
        # todo: this isn't done in simulation mode...
        for process in processes:
            process.account_update(broker_state)
        event_writer.write_event('broker', broker_state)

    def _create_event_writer(self):
        return events_log.writer('live')

