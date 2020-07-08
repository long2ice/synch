from synch.factory import get_writer


def test_table_exists():
    writer = get_writer()
    assert writer.check_table_exists("test", "test") is True
    assert writer.check_table_exists("test", "aaa") is False


def test_database_exists():
    writer = get_writer()
    assert writer.check_database_exists("test",) is True
    assert writer.check_database_exists("aaa",) is False
