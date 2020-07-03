import clickhouse_driver
import pytest

from conftest import local
from synch import Global, get_reader
from synch.replication.etl import etl_full


@pytest.mark.skipif(not local, reason="can't etl when not local")
def test_full_etl_postgres():
    alias = "postgres_db"
    reader = get_reader(alias)
    try:
        etl_full(reader, Global.settings, "postgres", {"test": "id"}, alias, True)
    except clickhouse_driver.errors.ServerException as e:
        assert e.code == 86


@pytest.mark.skipif(not local, reason="can't etl when not local")
def test_full_etl_mysql():
    alias = "mysql_db"
    reader = get_reader(alias)
    etl_full(reader, Global.settings, "test", {"test": "id"}, alias, True)
