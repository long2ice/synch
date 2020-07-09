import datetime
import logging
import signal
from signal import Signals
from typing import Callable, Dict

from synch.enums import ClickHouseEngine
from synch.factory import get_broker, get_writer
from synch.settings import Settings

logger = logging.getLogger("synch.replication.etl")

len_event = 0
event_list = {}
is_insert = False
is_stop = False


def signal_handler(signum: Signals, handler: Callable):
    global is_stop
    global is_insert
    sig = Signals(signum)
    if len_event == 0:
        logger.info(f"shutdown consumer on {sig.name} success")
        exit()
    else:
        logger.info(
            f"shutdown consumer on {sig.name}, wait seconds at most to finish {len_event} events..."
        )
        is_stop = True
        is_insert = True


def finish_continuous_etl(broker):
    logger.info("finish success, bye!")
    broker.close()
    exit()


def continuous_etl(
    alias: str, schema: str, tables_pk: Dict, tables_dict: Dict, last_msg_id, skip_error: bool,
):
    """
    continuous etl from broker and insert into clickhouse
    """
    insert_interval = Settings.insert_interval()
    insert_num = Settings.insert_num()
    if not Settings.debug():
        if insert_interval < 60 or insert_num < 20000:
            logger.warning(
                "If is recommended to set insert_interval=60 and insert_num=20000 when production."
            )
    logger.info(
        f"start consumer for {schema} success at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, last_msg_id={last_msg_id}, insert_interval={insert_interval}, insert_num={insert_num}"
    )
    global len_event
    global event_list
    global is_insert

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    broker = get_broker(alias)
    for msg_id, msg in broker.msgs(schema, last_msg_id=last_msg_id, block=insert_interval * 1000):
        if not msg_id:
            if len_event > 0:
                is_insert = True
                alter_table = False
                query = None
            else:
                if is_stop:
                    finish_continuous_etl(broker)
                continue
        else:
            alter_table = False
            query = None
            logger.debug(f"msg_id:{msg_id}, msg:{msg}")
            len_event += 1
            event = msg
            table = event["table"]
            schema = event["schema"]
            action = event["action"]
            values = event["values"]

            if action == "query":
                alter_table = True
                query = values["query"]
            else:
                engine = tables_dict.get(table).get("clickhouse_engine")
                writer = get_writer(engine)
                event_list = writer.handle_event(
                    tables_dict, tables_pk.get(table), schema, table, action, event_list, event,
                )

            if len_event == insert_num:
                is_insert = True

        if is_insert or alter_table:
            for table, v in event_list.items():
                pk = tables_pk.get(table)
                if isinstance(v, dict):
                    writer = get_writer(ClickHouseEngine.merge_tree)
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
                    if skip_error:
                        try:
                            if delete_pks:
                                writer.delete_events(schema, table, pk, delete_pks)
                            if insert_events:
                                writer.insert_events(schema, table, insert_events)
                        except Exception as e:
                            logger.error(
                                f"insert event error,error: {e}", exc_info=True, stack_info=True
                            )
                    else:
                        if delete_pks:
                            writer.delete_events(schema, table, pk, delete_pks)
                        if insert_events:
                            writer.insert_events(schema, table, insert_events)
                elif isinstance(v, list):
                    writer = get_writer(ClickHouseEngine.collapsing_merge_tree)
                    if v:
                        if skip_error:
                            try:
                                writer.insert_events(schema, table, v)
                            except Exception as e:
                                logger.error(
                                    f"insert event error,error: {e}", exc_info=True, stack_info=True
                                )
                        else:
                            writer.insert_events(schema, table, v)
            if alter_table:
                try:
                    get_writer().execute(query)
                except Exception as e:
                    logger.error(f"alter table error: {e}", exc_info=True, stack_info=True)
            broker.commit(schema)
            logger.info(f"success commit {len_event} events")
            event_list = {}
            is_insert = False
            len_event = 0
            if is_stop:
                finish_continuous_etl(broker)
