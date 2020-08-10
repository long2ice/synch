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


def test_delete_events(mocker):
    writer = get_writer()
    mocker.patch.object(writer, "execute", return_value=True, autospec=True)

    database = get_mysql_database()
    sql, params = writer.delete_events(database, "test", "id", ["1", "2"])
    assert sql == "alter table synch_mysql_test.test delete where id in %(pks)s"
    assert params == {"pks": ("1", "2")}

    sql, params = writer.delete_events(database, "test", "id", ["2"])
    assert sql == "alter table synch_mysql_test.test delete where id in %(pks)s"
    assert params == {"pks": ("2",)}

    sql, params = writer.delete_events(database, "test", "id", [1, 2])
    assert sql == "alter table synch_mysql_test.test delete where id in %(pks)s"
    assert params == {"pks": (1, 2)}

    sql, params = writer.delete_events(database, "test", "id", [2])
    assert sql == "alter table synch_mysql_test.test delete where id in %(pks)s"
    assert params == {"pks": (2,)}

    sql, params = writer.delete_events(database, "test", ("id", "id2"), [(1, 2), (2, 3)])
    assert (
        sql == "alter table synch_mysql_test.test delete where (id=1 and id2=2) or (id=2 and id2=3)"
    )
    assert params is None

    sql, params = writer.delete_events(database, "test", ("id", "id2"), [("1", "2"), ("2", "3")])
    assert (
        sql
        == "alter table synch_mysql_test.test delete where (id='1' and id2='2') or (id='2' and id2='3')"
    )
    assert params is None
