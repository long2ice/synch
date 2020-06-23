from mysql2ch.reader.postgres import Postgres


def test_get_primary_pk():
    p = Postgres()
    p.get_primary_key('postgres', 'test')
