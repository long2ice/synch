import json
import logging

from kafka import KafkaProducer

from .common import Global, JsonEncoder, init_partitions, partitioner
from .pos import RedisLogPos

logger = logging.getLogger("mysql2ch.producer")


def produce(args):
    settings = Global.settings
    reader = Global.reader

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_server,
        value_serializer=lambda x: json.dumps(x, cls=JsonEncoder).encode(),
        key_serializer=lambda x: x.encode(),
        partitioner=partitioner,
    )
    init_partitions(settings)

    pos_handler = RedisLogPos(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password,
        db=settings.redis_db,
        log_pos_prefix=settings.log_pos_prefix,
        server_id=settings.mysql_server_id,
    )

    log_file, log_pos = pos_handler.get_log_pos()
    if not (log_file and log_pos):
        log_file = settings.init_binlog_file
        log_pos = settings.init_binlog_pos
    else:
        log_pos = int(log_pos)

    try:
        logger.info(f"start producer success!")
        count = 0
        tables = []
        for k, v in settings.schema_table.items():
            tables += v.get("tables")
        only_schemas = list(settings.schema_table.keys())
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
            if table and table not in settings.schema_table.get(schema).get("tables"):
                continue
            producer.send(
                topic=settings.kafka_topic, value=event, key=schema,
            )
            if count == settings.insert_interval:
                count = 0
                logger.info(f"success send {settings.insert_interval} events!")
            logger.debug(f"send to kafka success: key:{schema},event:{event}")
            count += 1
            pos_handler.set_log_pos_slave(file, pos)
            logger.debug(f"success set binlog pos:{file}:{pos}")
    except KeyboardInterrupt:
        log_file, log_pos = pos_handler.get_log_pos()
        message = f"KeyboardInterrupt,current position: {log_file}:{log_pos}"
        logger.info(message)
