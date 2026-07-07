import json

from kafka import KafkaProducer

from app.config.settings import get_settings


def build_kafka_producer() -> KafkaProducer:
    settings = get_settings()
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )
