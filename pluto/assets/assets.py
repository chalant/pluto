from zipline.assets import asset_writer

class AssetDBWriter(asset_writer.AssetDBWriter):
    #todo: we need to recompute asset lifetimes each start of session
    # in live, the end_date is always the current session. (We need a rolling window of
    # asset lifetimes)
    def append(self):
        #todo: append data to the asset_db_writer
        pass
