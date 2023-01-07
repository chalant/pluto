from zipline.data import data_portal


class DataPortal(data_portal.DataPortal):
    def get_splits(self, assets, dt):
        # todo: query from click-house database
        return

    def get_stock_dividends(self, sid, trading_days):
        # todo: query dividends from click-house database
        return
