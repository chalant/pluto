import typing

import clickhouse_driver


def drop_table(clickhouse_client: clickhouse_driver.Client, db_name):
    drop = """DROP TABLE IF EXISTS daily_bars"""

    clickhouse_client.execute(drop)


class DailyBars(object):
    insert = """INSERT INTO daily_bars (sid, open, high, low, close, volume, day) VALUES """
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
    drop = """DROP TABLE IF EXISTS daily_bars"""


class MinuteBars(object):
    insert = """INSERT INTO minute_bars (sid, open, high, low, close, volume, dt) VALUES """
    create = """
            CREATE TABLE IF NOT EXISTS minute_bars
            (
                sid UInt32,
                open UInt32,
                high UInt32,
                low UInt32,
                close UInt32,
                volume UInt32
                dt DateTime64(8, 'UTC')
            )
            ENGINE = MergeTree()
            ORDER BY (sid, dt)
        """
    drop = """DROP TABLE IF EXISTS minute_bars"""


def write(
        data_generator: typing.Generator,
        db_config: dict,
        bundle_name: str,
        db_table: typing.Union[typing.Type[MinuteBars], typing.Type[DailyBars]]):

    insert = db_table.insert

    with clickhouse_driver.Client(
            db_config['host'],
            db_config['port'],
            settings={
                'use_client_time_zone': False
            },
            password=db_config['password'],
            database=bundle_name) as clt:
        with clt.disconnect_on_error(insert, None):
            clt.connection.send_query(insert, query_id=None)
            clt.connection.send_external_tables(None)
            # note this blocks if we don't send external tables...

            sample_block = clt.receive_sample_block()
            rv = None

            if sample_block:
                columns = [x[0] for x in sample_block.columns_with_types]

                # def generator():
                #     for df in data:
                #         yield [df[column] for column in columns]

                rv = clt.send_data(sample_block, data_generator)
                clt.receive_end_of_query()

class ClickHouseDataWriter(object):
    def __init__(self, db_config, bundle_name, db_table):

        self._db_config = db_config
        self._bundle_name = bundle_name

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                password=db_config['password'],
                database=bundle_name) as cl:

            cl.execute(db_table.create)

        self._db_table = db_table

    def write(self, data):
        db_config = self._db_config
        insert = self._db_table.insert

        with clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                settings={
                    'use_client_time_zone': False
                },
                password=db_config['password'],
                database=self._bundle_name) as clt:

            with clt.disconnect_on_error(insert, None):
                clt.connection.send_query(insert, query_id=None)
                clt.connection.send_external_tables(None)
                # note this blocks if we don't send external tables...

                sample_block = clt.receive_sample_block()
                rv = None

                if sample_block:
                    columns = [x[0] for x in sample_block.columns_with_types]

                    # def generator():
                    #     for df in data:
                    #         yield [df[column] for column in columns]

                    rv = clt.send_data(sample_block, data)
                    clt.receive_end_of_query()



