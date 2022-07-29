import clickhouse_driver

_INSERT = """INSERT INTO daily_bars VALUES (x)"""


def create_table(clickhouse_client: clickhouse_driver.Client):
    create = """
    CREATE TABLE data.daily_bars
        (
            sid UInt32,
            open UInt32,
            high UInt32,
            low UInt32,
            close UInt32
            volume UInt32,
            day DateTime
        )
        ENGINE = MergeTree()
        ORDER BY (sid, day)
    """

    clickhouse_client.execute(create)


class ClickHouseDailyBarWriter(object):
    def __init__(self, clickhouse_client: clickhouse_driver.Client):
        self._client = clickhouse_client

    def write(self, data):
        clt = self._client

        with clt.disconnect_on_error(_INSERT, None):
            clt.connection.send_query(_INSERT, query_id=None)

            sample_block = clt.receive_sample_block()
            rv = None

            if sample_block:
                columns = [x[0] for x in sample_block.columns_with_types]

                def generator():
                    for df in data:
                        yield [df[column] for column in columns]

                rv = clt.send_data(
                    sample_block,
                    generator(),
                    columnar=True)

                clt.receive_end_of_query()

