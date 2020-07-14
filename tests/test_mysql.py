from conftest import alias_mysql, get_mysql_database
from synch.factory import get_reader


def test_get_pk():
    reader = get_reader(alias_mysql)
    ret = reader.get_primary_key(get_mysql_database(), "test")
    assert ret == "id"


def test_get_binlog():
    reader = get_reader(alias_mysql)
    ret = reader.get_binlog_pos()
    assert isinstance(ret, tuple)


def test_execute_sql():
    reader = get_reader(alias_mysql)
    sql = "select 1"
    ret = reader.execute(sql)[0]
    assert ret == {"1": 1}
