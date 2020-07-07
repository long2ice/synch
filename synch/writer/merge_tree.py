import logging
from typing import Dict, List, Union

from synch.enums import ClickHouseEngine
from synch.reader import Reader
from synch.writer import ClickHouse

logger = logging.getLogger("synch.writer.merge_tree")


class ClickHouseMergeTree(ClickHouse):
    engine = ClickHouseEngine.merge_tree

    def delete_events(self, schema: str, table: str, pk: Union[tuple, str], pk_list: List):
        """
        delete record by pk
        """
        if isinstance(pk, tuple):
            sql = f"alter table {schema}.{table} delete where "
            pks_list = []
            for pk_value in pk_list:
                item = []
                for index, pk_item in enumerate(pk):
                    item.append(f"{pk_item}={pk_value[index]}")
                pks_list.append("(" + " and ".join(item) + ")")
            sql += " or ".join(pks_list)
        else:
            if len(pk_list) > 1:
                pks = ",".join(str(pk) for pk in pk_list)
            else:
                pks = pk_list[0]
            sql = f"alter table {schema}.{table} delete where {pk} in ({pks})"
        self.execute(sql)
        return sql

    def get_table_create_sql(
        self,
        reader: Reader,
        schema: str,
        table: str,
        pk,
        partition_by: str = None,
        engine_settings: str = None,
        **kwargs,
    ):
        partition_by_str = ""
        engine_settings_str = ""
        if partition_by:
            partition_by_str = f" PARTITION BY {partition_by} "
        if engine_settings:
            engine_settings_str = f" SETTINGS {engine_settings} "
        select_sql = reader.get_source_select_sql(schema, table)
        return f"CREATE TABLE {schema}.{table} ENGINE = {self.engine} {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"

    def get_full_insert_sql(self, reader: Reader, schema: str, table: str, sign_column: str = None):
        return f"insert into {schema}.{table} {reader.get_source_select_sql(schema, table, )}"

    def handle_event(
        self,
        tables_dict: Dict,
        pk,
        schema: str,
        table: str,
        action: str,
        tmp_event_list: Dict,
        event: Dict,
    ):
        values = self.pre_handle_values(tables_dict.get(table).get("skip_decimal"), event["values"])
        event["values"] = values
        tmp_event_list.setdefault(table, {}).setdefault(action, {})
        if not pk:
            logger.warning(f"No pk found in table {schema}.{table}, skip...")
            return tmp_event_list
        else:
            if isinstance(pk, tuple):
                pk_value = {values[pk[0]], values[pk[1]]}
            else:
                pk_value = values[pk]
            tmp_event_list[table][action][pk_value] = event
        return tmp_event_list
