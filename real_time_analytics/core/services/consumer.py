from confluent_kafka.cimpl import Consumer

from real_time_analytics.core.config import Settings


async def create_and_subscribe_kafka_consumer(group_id, topic):
    """Create a Kafka consumer with the given settings, subscribe to the given topic, and consume a message"""
    settings = {
        "bootstrap.servers": Settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 60000,
        "max.poll.records": 100,
    }

    consumer = Consumer(settings)
    consumer.subscribe([topic])
    return consumer
