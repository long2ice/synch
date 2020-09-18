from typing import Dict

from synch.common import cluster_sql
from synch.enums import ClickHouseEngine
from synch.reader import Reader
from synch.writer.merge_tree import ClickHouseMergeTree


class ClickHouseCollapsingMergeTree(ClickHouseMergeTree):
    engine = ClickHouseEngine.collapsing_merge_tree

    def get_table_create_sql(
        self,
        reader: Reader,
        schema: str,
        table: str,
        pk,
        partition_by: str = None,
        engine_settings: str = None,
        sign_column: str = None,
    ):
        super(ClickHouseCollapsingMergeTree, self).get_table_create_sql(
            reader, schema, table, pk, partition_by, engine_settings, sign_column=sign_column
        )
        select_sql = reader.get_source_select_sql(schema, table, sign_column)
        partition_by_str = ""
        engine_settings_str = ""
        if partition_by:
            partition_by_str = f" PARTITION BY {partition_by} "
        if engine_settings:
            engine_settings_str = f" SETTINGS {engine_settings} "
        return f"CREATE TABLE {schema}.{table}{cluster_sql(self.cluster_name)}ENGINE = {self.engine}({sign_column}) {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"

    def get_full_insert_sql(self, reader: Reader, schema: str, table: str, sign_column: str = None):
        return f"insert into {schema}.{table} {reader.get_source_select_sql(schema, table, sign_column)}"

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
        sign_column = tables_dict.get(table).get("sign_column")
        if action == "delete":
            values[sign_column] = -1
        elif action == "update":
            values[sign_column] = 1
        elif action == "insert":
            values[sign_column] = 1
        event["values"] = values
        tmp_event_list.setdefault(table, []).append(event)
        return tmp_event_list
