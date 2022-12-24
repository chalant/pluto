import pandas as pd

from zipline.finance.asset_restrictions import *
from zipline.utils.numpy_utils import vectorized_is_element
from zipline.assets import Asset


class IsNotIn(object):
    def __init__(self, controllable):
        self._controllable = controllable

    def __contains__(self, item):
        return item not in self._controllable.watchlist


class WatchList(Restrictions):
    """
    Static restrictions stored in memory that are constant regardless of dt
    for each asset.

    Parameters
    ----------
    controllable : iterable of assets
        The assets to be allowed
    """

    def __init__(self, controllable):
        self._allowed_set = IsNotIn(controllable)

    def is_restricted(self, assets, dt):
        """
        An asset is restricted for all dts if it is in the static list.
        """
        if isinstance(assets, Asset):
            return assets in self._allowed_set
        return pd.Series(
            index=pd.Index(assets),
            data=vectorized_is_element(assets, self._allowed_set))
