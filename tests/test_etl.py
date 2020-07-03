import clickhouse_driver

from synch import Global, get_reader
from synch.replication.etl import etl_full


def test_full_etl_postgres():
    alias = "postgres_db"
    reader = get_reader(alias)
    try:
        etl_full(reader, Global.settings, "postgres", {"test": "id"}, alias, True)
    except clickhouse_driver.errors.ServerException as e:
        assert e.code == 86


def test_full_etl_mysql():
    alias = "mysql_db"
    reader = get_reader(alias)
    etl_full(reader, Global.settings, "test", {"test": "id"}, alias, True)
