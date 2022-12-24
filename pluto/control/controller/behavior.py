import typing
from abc import ABC, abstractmethod

import pandas as pd

from pluto.data import data_source as ds
from pluto.control.controller import process_manager as pm

from protos.clock_pb2 import (
    BAR,
    TRADE_END,
    SESSION_END)


class AbstractControllerBehavior(ABC):
    @abstractmethod
    def update_watchlist(self, strategy_manager: pm.StrategyManager) -> None:
        # self._watchlist = strategy_manager.get_watchlist()
        pass

    @abstractmethod
    def ingest_watchlist_bar_data(
            self,
            ts: pd.Timestamp,
            data_source: ds.AbstractDataSource,
            manager: pm.StrategyManager,
            signals) -> None:
        """
        should ingest data for the watchlist in the live database
        Parameters
        ----------
        ts
        data_source
        manager
        signals

        Returns
        -------

        """

    @abstractmethod
    def ingest_bar_data(
            self,
            ts: pd.Timestamp,
            data_source: ds.AbstractDataSource,
            signals: typing.List) -> None:
        """
        Should ingest data for all assets in the historical database, then
        transfer data in the database copy.

        Parameters
        ----------
        ts
        data_source
        signals

        Returns
        -------

        """


class SimulationControllerBehavior(AbstractControllerBehavior):
    def update_watchlist(self, strategy_manager: pm.StrategyManager) -> None:
        pass

    def ingest_watchlist_bar_data(
            self,
            ts: pd.Timestamp,
            data_source: ds.AbstractDataSource,
            manager: pm.StrategyManager,
            signals) -> None:
        pass

    def ingest_bar_data(
            self,
            ts: pd.Timestamp,
            data_source: ds.AbstractDataSource,
            signals: typing.List[typing.Tuple[pd.Timestamp, pd.Timestamp, str]]) -> None:
        pass


class LiveControllerBehavior(AbstractControllerBehavior):
    # todo: maybe we should use a merge collapse engine for live database
    def __init__(self):
        self._all_assets = set()

        self._watchlist = None

    def update_watchlist(self, strategy_manager: pm.StrategyManager) -> None:

        self._watchlist = watchlist = strategy_manager.get_watchlist()

        new_watchlist = set(watchlist.values())

        all_assets = self._all_assets

        # assets that are not in the live database that we need to copy
        # from the main database (these assets were never in the database, so ingesting them is safe)
        to_ingest = new_watchlist.difference(all_assets)

        if to_ingest:
            # todo: copy data for assets to ingest from main database to live database
            #  here.
            pass

        # keep track of already ingested assets
        self._all_assets = all_assets | to_ingest

    def ingest_watchlist_bar_data(
            self,
            ts: pd.Timestamp,
            data_source: ds.AbstractDataSource,
            manager: pm.StrategyManager,
            signals) -> None:

        # ingest data for positions and watchlist

        assets = []

        # retrieve assets for which we have positions
        for cts, c_evt, calendar in signals:
            if c_evt == BAR or c_evt == TRADE_END:
                for position in manager.get_positions(calendar.name):
                    assets.append(position.asset)

        assets = set(assets)

        watchlist = self._watchlist

        if watchlist:
            # filter assets we need data from.

            assets = assets | set([
                watchlist[calendar.name]
                for cts, c_evt, calendar in signals
                if c_evt == BAR or c_evt == TRADE_END])

        if assets:
            # todo: write in the live database...
            for bar in data_source.get_bar_data(ts, assets):
                pass

    def ingest_bar_data(
            self,
            ts: pd.Timestamp,
            data_source: ds.AbstractDataSource, signals: typing.List) -> None:
        calendars_to_ingest = [calendar for cts, evt, calendar in signals if evt == SESSION_END]

        # todo: ingest data for assets that match the calendar name

        if self._watchlist:
            # todo:
            #  once the ingestion is done, drop live database and copy data from watched assets to new live database
            #  only ingest data for assets in the watchlist. Note: this have the effect
            #  of also cleaning up assets that are not in the watchlist...
            #  calling update watchlist will copy data for
            pass

        # todo: download data for all assets in the database each session_end event...
        #  this should download minute data for all assets in our database. this also
        #  aggregates data into daily bars to be used by the pipeline loaders.
