from mysql2ch import Global
from mysql2ch.reader.postgres import Postgres


def test_get_primary_pk():
    p = Postgres(Global.settings)
    p.get_primary_key("test", "test")
