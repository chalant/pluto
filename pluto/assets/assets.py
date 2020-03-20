import sqlalchemy as sa
import numpy as np
import pandas as pd

from zipline.assets import asset_writer
from zipline.assets import assets
from zipline.utils.numpy_utils import as_column

class AssetDBWriter(asset_writer.AssetDBWriter):
    #todo: we need to recompute asset lifetimes each start of session
    # in live, the end_date is always the current session. (We need a rolling window of
    # asset lifetimes)
    def append(self):
        #todo: append data to the asset_db_writer
        pass

class AssetFinder(assets.AssetFinder):
    def lifetimes(self, dates, include_start_date, country_codes):
        lifetimes = self._compute_asset_lifetimes(country_codes)

        raw_dates = as_column(dates.asi8)
        if include_start_date:
            mask = lifetimes.start <= raw_dates
        else:
            mask = lifetimes.start < raw_dates
        mask &= (raw_dates <= lifetimes.end)

        return pd.DataFrame(mask, index=dates, columns=lifetimes.sid)

    def _compute_asset_lifetimes(self, country_codes):
        sids = starts = ends = []
        equities_cols = self.equities.c

        results = sa.select((
            equities_cols.sid,
            equities_cols.start_date,
            equities_cols.end_date,
        )).where(
            (self.exchanges.c.exchange == equities_cols.exchange)
        ).execute().fetchall()
        if results:
            sids, starts, ends = zip(*results)

        sid = np.array(sids, dtype='i8')
        start = np.array(starts, dtype='f8')
        end = np.array(ends, dtype='f8')
        start[np.isnan(start)] = 0  # convert missing starts to 0
        end[np.isnan(end)] = np.iinfo(int).max  # convert missing end to INTMAX
        return assets.Lifetimes(sid, start.astype('i8'), end.astype('i8'))
