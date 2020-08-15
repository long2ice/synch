from unittest import TestCase

from synch.convert import SqlConvert


class TestSqlConvent(TestCase):
    def test_add_char(self):
        sql = "alter table test add name varchar(20) not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name String after id")

        sql = "alter table test.test add name varchar(20) not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name String after id")

    def test_add_bool(self):
        sql = "alter table test add name bool not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name UInt8 after id")

    def test_add_int(self):
        sql = "alter table test add name int not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name Int32 after id")

    def test_add_decimal(self):
        sql = "alter table test add name decimal(10,2) not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name Decimal(10,2) after id")

    def test_add_datetime(self):
        sql = "alter table test add name datetime not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test add column name DateTime after id")

    def test_drop(self):
        sql = "alter table test drop column name"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test drop column name")

        sql = "alter table test drop name"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test drop column name")

    def test_change_column(self):
        sql = "alter table test change `column` column2 int null"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, "alter table test.test rename column `column` to column2")

    def test_get_table_name(self):
        sql = "alter table scheme.table1 add column test"
        ret = SqlConvert.get_table_name(sql)
        self.assertEqual(ret, "table1")

        sql = "alter table  `scheme`.`table1` add column test"
        ret = SqlConvert.get_table_name(sql)
        self.assertEqual(ret, "table1")
