import datetime

import pandas as pd
import numpy as np

from zipline.pipeline import domain

class Domain(domain.Domain):
    def __init__(self,
                 controllable,
                 data_query_offset=-np.timedelta64(45, 'm')):
        self._controllable = controllable
        self._data_query_offset = (
            # add one minute because `open_time` is actually the open minute
            # label which is one minute _after_ market open...
                data_query_offset - np.timedelta64(1, 'm')
        )
        if data_query_offset >= datetime.timedelta(0):
            raise ValueError(
                'data must be ready before market open (offset must be < 0)',
            )

    def all_sessions(self):
        return self._controllable.trading_calendar.all_sessions

    def calendar(self):
        return self._controllable.trading_calendar

    @property
    def country_code(self):
        return ''

    def data_query_cutoff_for_sessions(self, sessions):
        #note: if data isn't available before the next open session, query will be cutoff...
        opens = self.calendar.opens.loc[sessions].values
        missing_mask = pd.isnull(opens)
        if missing_mask.any():
            missing_days = sessions[missing_mask]
            raise ValueError(
                'cannot resolve data query time for sessions that are not on'
                ' the %s calendar:\n%s' % (
                    self.calendar.name,
                    missing_days,
                ),
            )

        return pd.DatetimeIndex(opens + self._data_query_offset, tz='UTC')

    def __repr__(self):
        return "EquityCalendarDomain({})".format(
            self._controllable.trading_calendar.name,
        )
