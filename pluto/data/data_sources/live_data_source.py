import typing

import pandas

from zipline.protocol import Position

from pluto.data.data_sources import data_source
from pluto.configuration import configuration


class LiveDataSource(data_source.AbstractDataSource):
    def __init__(self):
        alpaca_credentials = configuration['alpaca']
        polygon_credentials = configuration['polygon.io']

    def get_last_sale_prices(self,
                             targets: typing.List[
                                typing.Tuple[
                                    pandas.Timestamp,
                                    pandas.Timestamp,
                                    str,
                                    typing.List[Position]]],
                             handle_non_market_minutes=False) -> typing.Dict[
            str,
            typing.List[typing.Tuple[int, float]]]:
        pass

    def get_bar_data(self, ts: pandas.Timestamp, assets):
        pass
