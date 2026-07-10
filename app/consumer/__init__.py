"""Kafka consumers package."""

from app.consumer.kafka_consumer import KafkaInventoryConsumer, build_kafka_consumer

__all__ = ["KafkaInventoryConsumer", "build_kafka_consumer"]
