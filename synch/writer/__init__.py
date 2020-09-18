import abc
import logging
from copy import deepcopy
from decimal import Decimal
from typing import Dict, List, Union

import clickhouse_driver

from synch.common import cluster_sql
from synch.enums import ClickHouseEngine
from synch.reader import Reader

logger = logging.getLogger("synch.replication.clickhouse")


class ClickHouse:
    engine: ClickHouseEngine
    len_event = 0
    is_stop = False
    is_insert = False
    event_list = {}

    def __init__(self, host: str, user: str, password: str, cluster_name: str = None):
        host, port = host.split(":")
        self._client = clickhouse_driver.Client(host=host, port=port, password=password, user=user)
        self.cluster_name = cluster_name

    def get_count(self, schema: str, table: str):
        sql = f"select count(*) as c from {schema}.{table}"
        return self.execute(sql)[0][0]

    def check_table_exists(self, schema: str, table: str):
        sql = f"select 1 from system.tables where database = '{schema}' and name = '{table}'"
        ret = self.execute(sql)
        if ret:
            return bool(ret[0][0])
        return False

    def check_database_exists(self, schema: str):
        sql = f"select 1 from system.databases where name = '{schema}'"
        ret = self.execute(sql)
        if ret:
            return bool(ret[0][0])
        return False

    def create_database(self, schema: str, cluster_name: str = None):
        sql = f"create database if not exists {schema}{cluster_sql(cluster_name)}"
        return self.execute(sql)

    def execute(self, sql, params=None, *args, **kwargs):
        log_sql = sql
        if params:
            log_sql = f"sql={sql},params={params}"
        logger.debug(log_sql)
        return self._client.execute(sql, params=params, *args, **kwargs)

    def fix_table_column_type(
        self, reader, database, table,
    ):
        """
        fix table column type in full etl
        :return:
        """
        sql = f"select COLUMN_NAME, COLUMN_TYPE,IS_NULLABLE from information_schema.COLUMNS where TABLE_NAME = '{table}' and COLUMN_TYPE like '%decimal%'and TABLE_SCHEMA = '{database}'"
        cursor = reader.conn.cursor()
        cursor.execute(sql)
        logger.debug(sql)
        ret = cursor.fetchall()
        cursor.close()
        for item in ret:
            column_name = item.get("COLUMN_NAME")
            is_nullable = item.get("IS_NULLABLE")
            column_type = item.get("COLUMN_TYPE").title()
            if is_nullable:
                fix_sql = f"alter table {database}.{table}{cluster_sql(self.cluster_name)} modify column {column_name} Nullable({column_type})"
            else:
                fix_sql = f"alter table {database}.{table}{cluster_sql(self.cluster_name)} modify column {column_name} {column_type}"
            self.execute(fix_sql)

    def insert_events(self, schema: str, table: str, insert_data: List[Dict]):
        insert_sql = "insert into {0}.{1} values ".format(schema, table)
        self.execute(insert_sql, list(map(lambda x: x.get("values"), insert_data)))

    @abc.abstractmethod
    def get_table_create_sql(
        self,
        reader: Reader,
        schema: str,
        table: str,
        pk,
        partition_by: str,
        engine_settings: str,
        **kwargs,
    ):
        if self.cluster_name:
            self.engine = f"ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{schema}/{table}','{{replica}}')"

    @abc.abstractmethod
    def get_full_insert_sql(self, reader: Reader, schema: str, table: str, sign_column: str = None):
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    def pre_handle_values(self, skip_decimal: bool, values: Dict):
        """
        handle decimal column if skip_decimal
        """
        tmp_values = deepcopy(values)
        if skip_decimal:
            for k, v in values.items():
                if isinstance(v, Decimal):
                    tmp_values[k] = str(v)
            return tmp_values
        return values

    def delete_events(self, schema: str, table: str, pk: Union[tuple, str], pk_list: List):
        pass

    def get_distributed_table_create_sql(self, schema: str, table: str, suffix: str):
        return f"""create table if not exists {schema}.{table}{suffix} on cluster {self.cluster_name}
AS {schema}.{table}
ENGINE = Distributed({self.cluster_name},{schema},{table},rand());"""
