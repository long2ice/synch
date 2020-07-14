from conftest import alias_postgres, get_postgres_database
from synch.factory import get_reader
from synch.reader.postgres import Postgres


def test_get_pk():
    postgres = Postgres(alias_postgres)
    ret = postgres.get_primary_key(get_postgres_database(), "test")
    assert ret == "id"


def test_execute_sql():
    reader = get_reader(alias_postgres)
    sql = "select 1"
    ret = reader.execute(sql)[0]
    assert ret == [1]
