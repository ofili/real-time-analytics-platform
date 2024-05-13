from confluent_kafka.cimpl import Producer, KafkaException
from real_time_analytics.core.utils.retry import CircuitBreaker, retry

from real_time_analytics.core.config import Settings
from real_time_analytics.core.logging import logger

circuit_breaker = CircuitBreaker(fail_threshold=5, reset_timeout=10)


@retry(
    max_retries=3,
    backoff_factor=2,
    exceptions=(KafkaException,),
    circuit_breaker=circuit_breaker,
)
async def get_kafka_producer():
    return Producer(
        {
            "bootstrap.servers": Settings.kafka_bootstrap_servers,
            "on_delivery": lambda e, m, data: logger.info(
                f"Delivered message to Kafka: {data}"
            ),
        }
    )
