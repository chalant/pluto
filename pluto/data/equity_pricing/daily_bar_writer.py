import clickhouse_driver

_INSERT = """
INSERT INTO daily_bars (sid, open, high, low, close, volume, day) VALUES """


def create_table(clickhouse_client: clickhouse_driver.Client):
    create = """
        CREATE TABLE IF NOT EXISTS daily_bars
        (
            sid UInt32,
            open UInt32,
            high UInt32,
            low UInt32,
            close UInt32,
            volume UInt32,
            day DateTime64(8, 'UTC')
        )
        ENGINE = MergeTree()
        ORDER BY (sid, day)
    """

    clickhouse_client.execute(create)


def drop_table(clickhouse_client: clickhouse_driver.Client, db_name):
    drop = """
    DROP TABLE IF EXISTS daily_bars
    
    """.format(db_name)

    clickhouse_client.execute(drop)


class ClickHouseDailyBarWriter(object):
    def __init__(self, db_config, bundle_name):

        self._db_config = db_config
        self._bundle_name = bundle_name

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password'],
                database=bundle_name) as cl:
            create_table(cl)

    def write(self, data):
        db_config = self._db_config

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                settings={
                    'use_client_time_zone': False
                },
                password=db_config['password'],
                database=self._bundle_name) as clt:

            with clt.disconnect_on_error(_INSERT, None):
                clt.connection.send_query(_INSERT, query_id=None)
                clt.connection.send_external_tables(None)
                # note this blocks if we don't send external tables...

                sample_block = clt.receive_sample_block()
                rv = None

                if sample_block:
                    columns = [x[0] for x in sample_block.columns_with_types]

                    # def generator():
                    #     for df in data:
                    #         yield [df[column] for column in columns]

                    rv = clt.send_data(
                        sample_block,
                        data)

                    clt.receive_end_of_query()

