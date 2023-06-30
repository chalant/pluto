class EquityAsset(object):
    __slots__ = ('symbol', 'first_ts', 'last_ts')

    def __init__(self, symbol, first_ts, last_ts):
        self.symbol = symbol
        self.first_ts = first_ts
        self.last_ts = last_ts


def get_equity_assets(connection, sessions, table_name, calendar_name):
    # returns for which we have no data yet.
    res = []

    query = f'''
        SELECT symbol, first_timestamp, last_timestamp 
        FROM equity_assets
        JOIN {table_name}
        ON {table_name}.sid = equity_assets.sid
        JOIN calendars
        ON {table_name}.exchange = calendars.exchange
        WHERE calendar_name=?'''

    for res in connection.execute(query, (calendar_name,)):

        symbol, first_ts, last_ts = res

        # means that symbol has never been ingested...
        if last_ts == -1:
            last_ts = sessions[0]
            first_ts = sessions[0]

        # only download if the data is not up-to-date
        if last_ts < sessions[-1]:
            res.append(EquityAsset(
                symbol,
                first_ts,
                last_ts))
    return res

def write_equity_assets(connection, sessions):
    pass
