from synch.factory import get_reader


def test_get_pk():
    reader = get_reader("mysql_db")
    ret = reader.get_primary_key("test", "test")
    assert ret == "id"


def test_get_binlog():
    reader = get_reader("mysql_db")
    ret = reader.get_binlog_pos()
    assert isinstance(ret, tuple)


def test_execute_sql():
    reader = get_reader("mysql_db")
    sql = "select 1"
    ret = reader.execute(sql)[0]
    assert ret == {"1": 1}
