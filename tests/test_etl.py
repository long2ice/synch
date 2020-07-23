from decimal import Decimal

import pytest

from conftest import alias_mysql, alias_postgres, get_mysql_database, get_postgres_database
from synch.factory import get_reader, get_writer
from synch.replication.etl import etl_full


@pytest.mark.usefixtures("truncate_postgres_table")
def test_full_etl_postgres():
    database = get_postgres_database()

    sql = "insert into test(id,amount) values(1,1)"
    get_reader(alias_postgres).execute(sql)

    etl_full(alias_postgres, database, {"test": "id"}, True)

    sql = f"select * from {database}.test"
    ret = get_writer().execute(sql)
    assert ret == [(1, Decimal("1"))]


@pytest.mark.usefixtures("truncate_mysql_table")
def test_full_etl_mysql():
    database = get_mysql_database()

    sql = f"insert into {database}.test(amount) values(1.00)"
    get_reader(alias_mysql).execute(sql)

    etl_full(alias_mysql, database, {"test": "id"}, True)

    sql = f"select * from {database}.test"

    ret = get_writer().execute(sql)
    assert ret == [(1, Decimal("1"))]
