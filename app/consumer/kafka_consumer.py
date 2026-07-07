import json

from kafka import KafkaConsumer

from app.config.settings import get_settings


def build_kafka_consumer(group_id: str) -> KafkaConsumer:
    settings = get_settings()
    return KafkaConsumer(
        settings.kafka_topic_inventory_events,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
