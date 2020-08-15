import ast
from dataclasses import dataclass

import mysqlparse
from pyparsing import ParseResults


@dataclass
class ParseRet:
    statement_type: str
    table_name: str
    alter_action: str
    column_name: str
    new_column_name: str
    data_type: ParseResults
    null: str
    column_position: str


class SqlConvert:
    _type_mapping = {
        "date": "Date",
        "datetime": "DateTime",
        "bool": "UInt8",
        "float": "Float32",
        "double": "Float64",
        "varchar": "String",
        "decimal": "Decimal{}",
        "tinyint": "Int8",
        "int": "Int32",
        "smallint": "Int16",
        "mediumint": "Int32",
        "bigint": "Int64",
        "timestamp": "DateTime",
        "char": "FixedString",
        "bigchar": "String",
    }

    @classmethod
    def get_parse_ret(cls, query: str) -> ParseRet:
        parsed = mysqlparse.parse(query)
        statement = parsed.statements[0]  # type:ast.stmt
        statement_type = statement.statement_type
        table_name = statement.table_name
        alter_specification = statement.alter_specification[0]
        alter_action = alter_specification.alter_action
        column_name = alter_specification.column_name
        data_type = alter_specification.data_type
        null = alter_specification.null
        new_column_name = alter_specification.new_column_name
        column_position = alter_specification.column_position
        return ParseRet(
            statement_type=statement_type,
            table_name=table_name,
            alter_action=alter_action,
            column_name=column_name,
            data_type=data_type,
            null=null,
            column_position=column_position,
            new_column_name=new_column_name,
        )

    @classmethod
    def get_real_data_type(cls, data_type: ParseResults, null: bool):
        data_type = data_type.asList()
        data_type_0 = data_type[0]
        data_type_1 = ""
        if data_type_0 == "DECIMAL":
            data_type_1 = f"({data_type[1]},{data_type[3]})"
        elif len(data_type) > 1:
            data_type_1 = data_type[1]
        real_data_type = cls._type_mapping.get(data_type_0.lower()).format(data_type_1)
        if null:
            return f"Nullable({real_data_type})"
        return real_data_type

    @classmethod
    def to_clickhouse(cls, schema: str, query: str):
        """
        parse ddl query to clickhouse
        :param schema:
        :param query:
        :return:
        """
        query = query.replace(f"{schema}.", "")
        ret = cls.get_parse_ret(query)
        alter_action = ret.alter_action
        sql = None
        column_name = ret.column_name
        if alter_action == "ADD COLUMN":
            sql = f"alter table {schema}.{ret.table_name} add column {column_name} {cls.get_real_data_type(ret.data_type, ret.null)}"
        elif alter_action == "DROP COLUMN":
            sql = f"alter table {schema}.{ret.table_name} drop column {column_name}"
        elif alter_action == "CHANGE COLUMN":
            sql = f"alter table {schema}.{ret.table_name} rename column {column_name} to {ret.new_column_name}"
        elif alter_action == "MODIFY COLUMN":
            sql = f"alter table {schema}.{ret.table_name} modify column {column_name} {cls.get_real_data_type(ret.data_type, ret.null)}"
        return sql
