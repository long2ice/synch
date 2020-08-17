from unittest import TestCase

from synch.convert import SqlConvert


class TestSqlConvent(TestCase):
    def test_add_char(self):
        sql = "alter table test add name varchar(20) not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test add column name String"))

        sql = "alter table test.test add name varchar(20) not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test add column name String"))

    def test_add_int(self):
        sql = "alter table test add name int not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test add column name Int32"))

    def test_add_decimal(self):
        sql = "alter table test add name decimal(10,2) not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test add column name Decimal(10,2)"))

    def test_add_datetime(self):
        sql = "alter table test add name datetime not null after id"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test add column name DateTime"))

    def test_drop(self):
        sql = "alter table test drop column name"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test drop column name"))

        sql = "alter table test drop name"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test drop column name"))

    def test_change_column(self):
        sql = "alter table test change `column` column2 int null"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(ret, ("test", "alter table test.test rename column `column` to column2"))

    def test_modify_column(self):
        sql = "alter table test modify column `channel` varchar(20) not null comment '渠道'"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(
            ret, ("test", "alter table test.test modify column `channel` String comment '渠道'")
        )

    def test_modify_column_with_default(self):
        sql = "alter table test modify column `giving_money` decimal(16,2) not null default 0 comment '赠送金额'"
        ret = SqlConvert.to_clickhouse("test", sql)
        self.assertEqual(
            ret,
            (
                "test",
                "alter table test.test modify column `giving_money` Decimal(16,2) default '0' comment '赠送金额'",
            ),
        )
