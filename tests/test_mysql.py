from synch import get_reader


def test_get_pk():
    reader = get_reader("mysql_db")
    ret = reader.get_primary_key("test", "test")
    assert ret == "id"
