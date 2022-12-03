import clickhouse_driver


def get_client(db_config, bundle_name):
    return clickhouse_driver.Client(
                db_config['host'],
                db_config['port'],
                settings={
                    'use_client_time_zone': False
                },
                password=db_config['password'],
                database=bundle_name)
