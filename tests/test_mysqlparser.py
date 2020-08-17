from unittest import TestCase

from synch.convert import SqlConvert


class TestMySQLParser(TestCase):
    def test_drop_column(self):
        ret = SqlConvert.get_parse_ret("alter table test drop column name")
        self.assertEqual(ret.statement_type, "ALTER")
        self.assertEqual(ret.table_name, "test")
        self.assertEqual(ret.alter_action, "DROP COLUMN")
        self.assertEqual(ret.column_name, "name")
        self.assertEqual(ret.data_type, "")
        self.assertEqual(ret.null, "")
        self.assertEqual(ret.column_position, "")

    def test_add_char_olumn(self):
        sql = "alter table test add name varchar(20) not null after id"
        ret = SqlConvert.get_parse_ret(sql)
        self.assertEqual(ret.statement_type, "ALTER")
        self.assertEqual(ret.table_name, "test")
        self.assertEqual(ret.alter_action, "ADD COLUMN")
        self.assertEqual(ret.column_name, "name")
        self.assertEqual(str(ret.data_type), "['VARCHAR', '20']")
        self.assertEqual(ret.null, False)
        self.assertEqual(ret.column_position, "id")

    def test_change_column(self):
        sql = "alter table test change `column` column2 int null"
        ret = SqlConvert.get_parse_ret(sql)
        self.assertEqual(ret.statement_type, "ALTER")
        self.assertEqual(ret.table_name, "test")
        self.assertEqual(ret.alter_action, "CHANGE COLUMN")
        self.assertEqual(ret.column_name, "`column`")
        self.assertEqual(str(ret.data_type), "['INT']")
        self.assertEqual(ret.null, True)
        self.assertEqual(ret.new_column_name, "column2")
