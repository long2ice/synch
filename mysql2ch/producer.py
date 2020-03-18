import json
import logging

from kafka import KafkaProducer

from . import pos_handler, reader, partitioner
import settings
from .common import JsonEncoder

logger = logging.getLogger('mysql2ch.producer')

producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x, cls=JsonEncoder).encode(),
    key_serializer=lambda x: x.encode(),
    partitioner=partitioner
)


def produce(args):
    log_file, log_pos = pos_handler.get_log_pos()
    if not (log_file and log_pos):
        log_file = settings.INIT_BINLOG_FILE
        log_pos = settings.INIT_BINLOG_POS
    try:
        for schema, table, event, file, pos in reader.binlog_reading(
                only_tables=settings.TABLES,
                only_schemas=settings.SCHEMAS,
                log_file=log_file,
                log_pos=int(log_pos),
                server_id=int(settings.MYSQL_SERVER_ID)
        ):
            try:
                key = f'{schema}.{table}'
                producer.send(
                    topic=settings.KAFKA_TOPIC,
                    value=event,
                    key=key,
                )
                logger.info(f'send to kafka success: key:{key},event:{event}')
                pos_handler.set_log_pos_slave(file, pos)
                logger.debug(f'success set binlog pos:{file}:{pos}')
            except Exception as e:
                logger.error(f'kafka send error: {e}')
                exit()
    except KeyboardInterrupt:
        log_file, log_pos = pos_handler.get_log_pos()
        message = f'KeyboardInterrupt,current position: {log_file}:{log_pos}'
        logger.info(message)
