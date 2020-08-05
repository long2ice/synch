import json
import logging

import kafka.errors
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic

from synch.broker import Broker
from synch.common import JsonEncoder, object_hook
from synch.settings import Settings

logger = logging.getLogger("synch.brokers.kafka")


class KafkaBroker(Broker):
    consumer: KafkaConsumer = None
    producer: KafkaProducer = None

    def __init__(self, alias):
        super().__init__(alias)
        self.servers = Settings.get("kafka").get("servers")
        self.topic = f'{Settings.get("kafka").get("topic_prefix")}.{alias}'
        self.databases = Settings.get_source_db(alias).get("databases")
        self.producer = KafkaProducer(
            bootstrap_servers=self.servers,
            value_serializer=lambda x: json.dumps(x, cls=JsonEncoder).encode(),
            key_serializer=lambda x: x.encode(),
        )
        self._init_topic()

    def close(self):
        self.producer and self.producer.close()
        self.consumer and self.consumer.close()

    def send(self, schema: str, msg: dict):
        self.producer.send(self.topic, key=schema, value=msg)

    def _get_kafka_partition(self, schema: str) -> int:
        for index, database in enumerate(self.databases):
            if database.get("database") == schema:
                return index

    def msgs(self, schema: str, last_msg_id, count: int = None, block: int = None):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.servers,
            value_deserializer=lambda x: json.loads(x, object_hook=object_hook),
            key_deserializer=lambda x: x.decode() if x else None,
            enable_auto_commit=False,
            group_id=schema,
            consumer_timeout_ms=block,
            auto_offset_reset="latest",
        )
        partition = self._get_kafka_partition(schema)
        topic_partition = TopicPartition(self.topic, partition)
        self.consumer.assign([topic_partition])
        if last_msg_id:
            self.consumer.seek(topic_partition, last_msg_id)
        while True:
            msgs = self.consumer.poll(block, max_records=count)
            if not msgs:
                yield None, msgs
            else:
                for msg in msgs.get(topic_partition):
                    yield msg.offset, msg.value

    def commit(self, schema: str = None):
        self.consumer.commit()

    def _init_topic(self):
        client = KafkaAdminClient(bootstrap_servers=self.servers)
        try:
            client.create_topics(
                [NewTopic(self.topic, num_partitions=len(self.databases), replication_factor=1)]
            )
        except kafka.errors.TopicAlreadyExistsError:
            pass
