import logging
from typing import Dict

from synch.writer import ClickHouse

logger = logging.getLogger("synch.writer.collapsing_merge_tree")


class ClickHouseCollapsingMergeTree(ClickHouse):
    def start_consume(self, schema: str, tables_pk: Dict, last_msg_id, skip_error: bool):
        sign_column = self.settings.schema_settings.get(schema).get("sign_column")
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
                values = event["values"]

                if action == "query":
                    alter_table = True
                    query = values["query"]
                else:
                    alter_table = False
                    query = None
                    if action == "delete":
                        values[sign_column] = -1
                    elif action == "update":
                        values[sign_column] = 1
                    elif action == "insert":
                        values[sign_column] = 1
                    event["values"] = values
                    self.event_list.setdefault(table, []).append(event)
                    self.len_event += 1

            if self.len_event == self.settings.insert_num:
                self.is_insert = True

            if self.is_insert or alter_table:
                events_num = 0
                for table, insert_events in self.event_list.items():
                    events_num += len(insert_events)
                    if skip_error:
                        try:
                            self.insert_events(schema, table, insert_events)
                        except Exception as e:
                            logger.error(f"insert event error,error: {e}")
                    else:
                        self.insert_events(schema, table, insert_events)
                if alter_table:
                    self.alter_table(query, skip_error)

                self.after_insert(schema, events_num)
