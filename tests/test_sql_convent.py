from unittest import TestCase

from mysql2ch.convent import SqlConvent


class TestSqlConvent(TestCase):
    def test_add_char(self):
        sql = "alter table test add name varchar(20) not null after id"
        ret = SqlConvent.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name String after id")

    def test_add_bool(self):
        sql = "alter table test add name bool not null after id"
        ret = SqlConvent.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name UInt8 after id")

    def test_add_int(self):
        sql = "alter table test add name int not null after id"
        ret = SqlConvent.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name Int32 after id")

    def test_add_decimal(self):
        sql = "alter table test add name decimal(10,2) not null after id"
        ret = SqlConvent.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name Decimal(10,2) after id")

    def test_add_datetime(self):
        sql = "alter table test add name datetime not null after id"
        ret = SqlConvent.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name DateTime after id")

    def test_drop(self):
        sql = "alter table test drop column name"
        ret = SqlConvent.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test drop column name")
