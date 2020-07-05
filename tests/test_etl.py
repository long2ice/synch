import clickhouse_driver
import pytest

from conftest import local
from synch.replication.etl import etl_full


@pytest.mark.skipif(not local, reason="can't etl when not local")
def test_full_etl_postgres():
    try:
        etl_full("postgres_db", "postgres", {"test": "id"}, True)
    except clickhouse_driver.errors.ServerException as e:
        assert e.code == 86


@pytest.mark.skipif(not local, reason="can't etl when not local")
def test_full_etl_mysql():
    etl_full("mysql_db", "test", {"test": "id"}, True)
