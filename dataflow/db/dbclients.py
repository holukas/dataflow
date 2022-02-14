# https://influxdb-python.readthedocs.io/en/latest/api-documentation.html#influxdbclient
# https://github.com/influxdata/influxdb-client-python/blob/master/examples/query.py
# https://www.influxdata.com/blog/getting-started-with-influxdb-and-pandas/

from influxdb_client import InfluxDBClient
from influxdb_client import WriteOptions

# from modules import filereader
# import modules.filereader as filereader


def get_write_client(conf_db: dict):
    client = InfluxDBClient(url=conf_db['url'], token=conf_db['token'], org=conf_db['org'])
    write_client = client.write_api(write_options=WriteOptions(
        batch_size=500, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000,
        max_retries=5, max_retry_delay=30_000, exponential_base=2))
    return client, write_client


def get_query_client(conf_db: dict):
    # dbconf = filereader.read_configfile(config_file='../../../configs/dbconf.yaml')
    client = InfluxDBClient(url=conf_db['url'], token=conf_db['token'], org=conf_db['org'])
    query_client = client.query_api()
    return client, query_client
