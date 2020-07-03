from synch import Global
from synch.reader.postgres import Postgres


def test_get_pk():
    settings = Global.settings
    postgres_db = settings.get_source_db("postgres_db")
    postgres = Postgres(postgres_db)
    ret = postgres.get_primary_key("postgres", "test")
    assert ret == "id"
