from synch.reader.postgres import Postgres


def test_get_pk():
    postgres = Postgres("postgres_db")
    ret = postgres.get_primary_key("postgres", "test")
    assert ret == "id"
