from synch.factory import get_reader
from synch.reader.postgres import Postgres


def test_get_pk():
    postgres = Postgres("postgres_db")
    ret = postgres.get_primary_key("postgres", "test")
    assert ret == "id"


def test_execute_sql():
    reader = get_reader("postgres_db")
    sql = "select 1"
    ret = reader.execute(sql)[0]
    assert ret == [1]
