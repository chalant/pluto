from pluto.coms.utils import conversions

from dev import events

from pluto.interface import monitor

from protos import controller_pb2


class DailyPerformancePlotter(object):
    def performance_update(self, packet):
        # append until end...

        perf = controller_pb2.PerformancePacket()
        perf.ParseFromString(packet.packet)

        perf = conversions.from_proto_performance_packet(perf)

        # todo: we also need minute updates...
        # todo:

        daily_perf = perf.get('daily_perf', None)
        daily_perf.update(perf['cumulative_risk_metrics']),

        cum_perf = perf['cumulative_perf']
        cum_perf.pop('period_close')
        cum_perf.pop('period_open')
        cum_perf.pop('capital_used')
        cum_perf.pop('starting_exposure')
        cum_perf.pop('ending_exposure')
        cum_perf.pop('starting_value')
        cum_perf.pop('starting_cash')
        cum_perf.pop('returns')
        cum_perf.pop('pnl')

        # todo: plot returns as they come
        # todo: append results...

        daily_perf.update(cum_perf)

        if packet.end:
            events.post_event("backtesting.ended", None)


# todo
class MinutePerformancePlotter(object):
    def performance_update(self, packet):
        # append until end...

        perf = controller_pb2.PerformancePacket()
        perf.ParseFromString(packet.packet)

        perf = conversions.from_proto_performance_packet(perf)

        # todo: we also need minute updates...
        # todo:

        daily_perf = perf.get('daily_perf', None)
        daily_perf.update(perf['cumulative_risk_metrics']),

        cum_perf = perf['cumulative_perf']
        cum_perf.pop('period_close')
        cum_perf.pop('period_open')
        cum_perf.pop('capital_used')
        cum_perf.pop('starting_exposure')
        cum_perf.pop('ending_exposure')
        cum_perf.pop('starting_value')
        cum_perf.pop('starting_cash')
        cum_perf.pop('returns')
        cum_perf.pop('pnl')

        # todo: plot returns as they come
        # todo: append results...

        daily_perf.update(cum_perf)

        if packet.end:
            events.post_event("backtesting.ended", None)
