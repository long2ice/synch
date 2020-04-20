import json
import logging

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewPartitions

from . import pos_handler, reader, partitioner
from mysql2ch import settings
from .common import JsonEncoder

logger = logging.getLogger('mysql2ch.producer')

producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x, cls=JsonEncoder).encode(),
    key_serializer=lambda x: x.encode(),
    partitioner=partitioner
)


def init_partitions():
    client = KafkaAdminClient(
        bootstrap_servers=settings.KAFKA_SERVER,
    )
    try:
        client.create_partitions(topic_partitions={
            settings.KAFKA_TOPIC: NewPartitions(total_count=len(settings.PARTITIONS.keys()))
        })
    except Exception as e:
        logger.warning(f'init_partitions error:{e}')


def produce(args):
    init_partitions()

    log_file, log_pos = pos_handler.get_log_pos()
    if not (log_file and log_pos):
        log_file = settings.INIT_BINLOG_FILE
        log_pos = settings.INIT_BINLOG_POS
    try:
        logger.info(f'start producer success!')
        count = 0
        all_schema_tables = settings.PARTITIONS.keys()
        for schema, table, event, file, pos in reader.binlog_reading(
                only_tables=settings.TABLES,
                only_schemas=settings.SCHEMAS,
                log_file=log_file,
                log_pos=int(log_pos),
                server_id=int(settings.MYSQL_SERVER_ID)
        ):
            key = f'{schema}.{table}'
            if key not in all_schema_tables:
                continue
            producer.send(
                topic=settings.KAFKA_TOPIC,
                value=event,
                key=key,
            )
            if count == settings.INSERT_INTERVAL:
                count = 0
                logger.info(f'success send {settings.INSERT_INTERVAL} events!')
            logger.debug(f'send to kafka success: key:{key},event:{event}')
            count += 1
            pos_handler.set_log_pos_slave(file, pos)
            logger.debug(f'success set binlog pos:{file}:{pos}')
    except KeyboardInterrupt:
        log_file, log_pos = pos_handler.get_log_pos()
        message = f'KeyboardInterrupt,current position: {log_file}:{log_pos}'
        logger.info(message)
