import logging
from typing import Dict, List, Union

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
                pks = ",".join(pk_list)
            else:
                pks = pk_list[0]
            sql = f"alter table {schema}.{table} delete where {pk} in ({pks})"
        self.execute(sql)
        return sql

    def start_consume(self, schema: str, tables_pk: Dict, last_msg_id, skip_error: bool):
        logger.info(f"start consumer for {schema} success")
        for msg_id, msg in self.broker.msgs(
            schema, last_msg_id=last_msg_id, block=self.settings.insert_interval * 1000
        ):
            if not msg_id:
                if self.len_event > 0:
                    self.is_insert = True
                    alter_table = False
                    query = None
                else:
                    if self.is_stop:
                        self.finish()
                    continue
            else:
                logger.debug(f"msg_id:{msg_id}, msg:{msg}")
                event = msg
                table = event["table"]
                schema = event["schema"]
                action = event["action"]

                self.event_list.setdefault(table, {}).setdefault(action, {})
                self.len_event += 1
                values = event["values"]
                if action == "query":
                    alter_table = True
                    query = values["query"]
                else:
                    alter_table = False
                    query = None
                    pk = tables_pk.get(table)
                    if not pk:
                        logger.warning(f"No pk found in table {schema}.{table}, skip...")
                        continue
                    else:
                        if isinstance(pk, tuple):
                            pk_value = {values[pk[0]], values[pk[1]]}
                        else:
                            pk_value = values[pk]
                    self.event_list[table][action][pk_value] = event

            if self.len_event == self.settings.insert_num:
                self.is_insert = True

            if self.is_insert or alter_table:
                self.check_mutations(schema)
                events_num = 0
                for table, v in self.event_list.items():
                    pk = tables_pk.get(table)
                    insert = v.get("insert")
                    delete = v.get("delete")
                    if delete:
                        delete_pks = list(delete.keys())
                    else:
                        delete_pks = []
                    if insert:
                        insert_events = list(
                            sorted(insert.values(), key=lambda x: x.get("event_unixtime"))
                        )
                    else:
                        insert_events = []
                    events_num += len(delete_pks)
                    events_num += len(insert_events)
                    if skip_error:
                        try:
                            if delete_pks:
                                self.delete_events(schema, table, pk, delete_pks)
                            if insert_events:
                                self.insert_events(schema, table, insert_events)
                        except Exception as e:
                            logger.error(f"insert event error,error: {e}")
                    else:
                        if delete_pks:
                            self.delete_events(schema, table, pk, delete_pks)
                        if insert_events:
                            self.insert_events(schema, table, insert_events)
                if alter_table:
                    self.alter_table(query, skip_error)

                self.after_insert(schema, events_num)
