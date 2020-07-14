from conftest import get_mysql_database
from synch.factory import get_writer


def test_table_exists():
    writer = get_writer()
    database = get_mysql_database()
    assert writer.check_table_exists(database, "test") is True
    assert writer.check_table_exists(database, "aaa") is False


def test_database_exists():
    writer = get_writer()
    database = get_mysql_database()
    assert writer.check_database_exists(database,) is True
    assert writer.check_database_exists("aaa",) is False
