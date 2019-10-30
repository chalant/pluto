from contrib.data.download.sources import source

#todo: iex-cloud has a weighting system: if you go above a certain threshold you can't make
# further requests until a monthly reset, unless you have "pay-as-you-go" activated, which will
# charge a fee at subsequent request.

# todo: we should prioritize requests that targets "exclusive" items... (items that are not common
#  to other sources) ex: stuff like insider-transactions: only iex has that item.

class IEXCloud(source.Source):
    def _get_exchanges(self):
        pass

    def _get_data_types(self, exchange):
        pass

    def _get_item(self, data_type):
        pass