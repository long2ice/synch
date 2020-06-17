import logging
import signal
import time
from signal import Signals

from mysql2ch.redis import RedisLogPos

from .factory import Global

logger = logging.getLogger("mysql2ch.producer")


def produce(args):
    settings = Global.settings
    reader = Global.reader

    broker = args.Broker()
    pos_handler = RedisLogPos()

    def signal_handler(signum: Signals, handler):
        sig = Signals(signum)
        log_f, log_p = pos_handler.get_log_pos()
        broker.close()
        logger.info(f"shutdown producer on {sig.name}, current position: {log_f}:{log_p}")
        exit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    log_file, log_pos = pos_handler.get_log_pos()
    if not (log_file and log_pos):
        log_file = settings.init_binlog_file
        log_pos = settings.init_binlog_pos
        if not (log_file and log_pos):
            log_file, log_pos = reader.get_binlog_pos()
        pos_handler.set_log_pos_slave(log_file, log_pos)
    log_pos = int(log_pos)

    logger.info(f"start producer success")
    count = last_time = 0
    tables = []
    schema_table = settings.schema_table
    for k, v in schema_table.items():
        for table in v:
            pk = reader.get_primary_key(k, table)
            if not pk or isinstance(pk, tuple):
                # skip delete and update when no pk and composite pk
                settings.skip_delete_tables.add(f"{k}.{table}")
        tables += v
    only_schemas = list(schema_table.keys())
    only_tables = list(set(tables))

    for schema, table, event, file, pos in reader.binlog_reading(
        only_tables=only_tables,
        only_schemas=only_schemas,
        log_file=log_file,
        log_pos=log_pos,
        server_id=settings.mysql_server_id,
        skip_dmls=settings.skip_dmls,
        skip_delete_tables=settings.skip_delete_tables,
        skip_update_tables=settings.skip_update_tables,
    ):
        if not schema_table.get(schema) or (table and table not in schema_table.get(schema)):
            continue
        broker.send(msg=event, schema=schema)
        pos_handler.set_log_pos_slave(file, pos)
        logger.debug(f"send to queue success: key:{schema},event:{event}")
        logger.debug(f"success set binlog pos:{file}:{pos}")

        now = int(time.time())
        count += 1

        if last_time == 0:
            last_time = now
        if now - last_time >= settings.insert_interval:
            logger.info(f"success send {count} events in {settings.insert_interval} seconds")
            last_time = count = 0
