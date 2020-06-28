import json
import logging

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewPartitions

from synch.broker import Broker
from synch.common import JsonEncoder, object_hook
from synch.settings import Settings

logger = logging.getLogger("synch.brokers.kafka")


class KafkaBroker(Broker):
    consumer: KafkaConsumer = None
    producer: KafkaProducer = None

    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka_servers,
            value_serializer=lambda x: json.dumps(x, cls=JsonEncoder).encode(),
            key_serializer=lambda x: x.encode(),
            partitioner=self._partitioner,
        )
        self._init_partitions()

    def _partitioner(self, key_bytes, all_partitions, available_partitions):
        """
        custom partitioner depend on settings
        :param key_bytes:
        :param all_partitions:
        :param available_partitions:
        :return:
        """
        key = key_bytes.decode()
        partition = self.settings.kafka_partitions.get(key)
        return all_partitions[partition]

    def close(self):
        self.producer and self.producer.close()
        self.consumer and self.consumer.close()

    def send(self, schema: str, msg: dict):
        self.producer.send(self.settings.kafka_topic, key=schema, value=msg)

    def msgs(self, schema: str, last_msg_id, block: int = None):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.settings.kafka_servers,
            value_deserializer=lambda x: json.loads(x, object_hook=object_hook),
            key_deserializer=lambda x: x.decode() if x else None,
            enable_auto_commit=False,
            group_id=schema,
            consumer_timeout_ms=block,
            auto_offset_reset="latest",
        )
        partition = self.settings.kafka_partitions.get(schema)
        topic_partition = TopicPartition(self.settings.kafka_topic, partition)
        self.consumer.assign([topic_partition])
        if last_msg_id:
            self.consumer.seek(topic_partition, last_msg_id)
        while True:
            msgs = self.consumer.poll(block, max_records=self.settings.insert_num)
            if not msgs:
                yield None, msgs
            else:
                for msg in msgs.get(topic_partition):
                    yield msg.offset, msg.value

    def commit(self, schema: str = None):
        self.consumer.commit()

    def _init_partitions(self):
        settings = self.settings
        client = KafkaAdminClient(bootstrap_servers=settings.kafka_servers,)
        try:
            client.create_partitions(
                topic_partitions={
                    settings.kafka_topic: NewPartitions(
                        total_count=len(settings.kafka_partitions.keys())
                    )
                }
            )
        except Exception as e:
            logger.debug(f"init_partitions error:{e}")
