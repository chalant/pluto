import clickhouse_driver


def create_table(clickhouse_client: clickhouse_driver.Client):
    create = """
    CREATE TABLE data.minute_bars
        (
            sid UInt32,
            open Float32,
            high Float32,
            low Float32,
            close Float32
            volume UInt32,
            day DateTime,
        )
        ENGINE = MergeTree()
        ORDER BY (sid, day)
    """

    clickhouse_client.execute(create)


class ClickHouseMinuteBarsWriter(object):
    def __init__(self):
        pass

    def write(self, data):
        pass