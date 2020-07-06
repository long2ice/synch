import logging
from typing import List, Union

from synch.writer import ClickHouse

logger = logging.getLogger("synch.writer.merge_tree")


class ClickHouseMergeTree(ClickHouse):
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
