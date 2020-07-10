import clickhouse_driver

from synch.replication.etl import etl_full


def test_full_etl_postgres():
    try:
        etl_full("postgres_db", "postgres", {"test": "id"}, True)
    except clickhouse_driver.errors.ServerException as e:
        assert e.code == 86


def test_full_etl_mysql():
    etl_full("mysql_db", "test", {"test": "id"}, True)
