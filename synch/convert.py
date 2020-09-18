import ast
import logging
from dataclasses import dataclass

import mysqlparse
from pyparsing import ParseResults

from synch.common import cluster_sql

logger = logging.getLogger("synch.convert")


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
    comment: str
    default: str
    decimals: str
    length: str


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
        "text": "String",
        "longtext": "String",
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
        default = alter_specification.default
        decimals = alter_specification.decimals
        length = alter_specification.length

        comment = alter_specification.comment
        return ParseRet(
            statement_type=statement_type,
            table_name=table_name,
            alter_action=alter_action,
            column_name=column_name,
            data_type=data_type,
            null=null,
            column_position=column_position,
            new_column_name=new_column_name,
            comment=comment,
            decimals=decimals,
            default=default,
            length=length,
        )

    @classmethod
    def get_real_data_type(cls, ret: ParseRet):
        data_type = ret.data_type.asList()
        data_type_0 = data_type[0]
        data_type_1 = ""
        if ret.decimals:
            data_type_1 = f"({ret.length},{ret.decimals})"
        elif len(data_type) > 1:
            data_type_1 = data_type[1]
        real_data_type = cls._type_mapping.get(data_type_0.lower()).format(data_type_1)
        if ret.null:
            return f"Nullable({real_data_type})"
        return real_data_type

    @classmethod
    def to_clickhouse(cls, schema: str, query: str, cluster_name: str = None):
        """
        parse ddl query to clickhouse
        :param schema:
        :param query:
        :param cluster_name
        :return:
        """
        query = query.replace(f"{schema}.", "")
        try:
            ret = cls.get_parse_ret(query)
        except Exception as e:
            logger.warning(
                f"Parse query error, query: {query}, err: {e}", stack_info=True, exc_info=True
            )
            return "", ""
        alter_action = ret.alter_action
        sql = None
        column_name = ret.column_name
        if ret.comment:
            comment = f" comment '{ret.comment}'"
        else:
            comment = ""
        if ret.default:
            default = f" default '{ret.default}'"
        else:
            default = ""
        if alter_action == "ADD COLUMN":
            sql = f"alter table {schema}.{ret.table_name}{cluster_sql(cluster_name)} add column {column_name} {cls.get_real_data_type(ret)}{default}{comment}"
        elif alter_action == "DROP COLUMN":
            sql = f"alter table {schema}.{ret.table_name}{cluster_sql(cluster_name)} drop column {column_name}"
        elif alter_action == "CHANGE COLUMN":
            sql = f"alter table {schema}.{ret.table_name}{cluster_sql(cluster_name)} rename column {column_name} to {ret.new_column_name}"
        elif alter_action == "MODIFY COLUMN":
            sql = f"alter table {schema}.{ret.table_name}{cluster_sql(cluster_name)} modify column {column_name} {cls.get_real_data_type(ret)}{default}{comment}"
        return ret.table_name, sql
