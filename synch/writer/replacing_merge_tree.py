from synch.common import cluster_sql
from synch.enums import ClickHouseEngine
from synch.reader import Reader
from synch.writer.merge_tree import ClickHouseMergeTree


class ClickHouseReplacingMergeTree(ClickHouseMergeTree):
    engine = ClickHouseEngine.replacing_merge_tree

    def get_table_create_sql(
        self,
        reader: Reader,
        schema: str,
        table: str,
        pk,
        partition_by: str = None,
        engine_settings: str = None,
        version_column: str = None,
        **kwargs,
    ):
        super(ClickHouseReplacingMergeTree, self).get_table_create_sql(
            reader,
            schema,
            table,
            pk,
            partition_by,
            engine_settings,
            version_column=version_column,
            **kwargs,
        )
        select_sql = reader.get_source_select_sql(schema, table)
        partition_by_str = ""
        engine_settings_str = ""
        if partition_by:
            partition_by_str = f" PARTITION BY {partition_by} "
        if engine_settings:
            engine_settings_str = f" SETTINGS {engine_settings} "
        if version_column:
            return f"CREATE TABLE {schema}.{table}{cluster_sql(self.cluster_name)} ENGINE = {self.engine}({version_column}) {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"
        else:
            return f"CREATE TABLE {schema}.{table}{cluster_sql(self.cluster_name)} ENGINE = {self.engine} {partition_by_str} ORDER BY {pk} {engine_settings_str} AS {select_sql} limit 0"
