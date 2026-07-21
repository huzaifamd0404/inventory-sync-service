import logging
import signal
from types import FrameType
from typing import Protocol

from app.config.logging import configure_logging
from app.config.settings import get_settings
from app.consumer.batch_kafka_consumer import BatchKafkaInventoryConsumer
from app.consumer.kafka_consumer import KafkaInventoryConsumer

logger = logging.getLogger(__name__)


class ConsumerLifecycle(Protocol):
    def consume_forever(self) -> None: ...

    def stop(self) -> None: ...


def _register_signal_handlers(consumer: ConsumerLifecycle) -> None:
    def _handle_signal(signum: int, _: FrameType | None) -> None:
        logger.info("inventory_consumer_shutdown_requested", extra={"signal": signum})
        consumer.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("inventory_consumer_worker_starting")
    consumer: ConsumerLifecycle
    if settings.batch_processing_enabled:
        consumer = BatchKafkaInventoryConsumer.from_settings(settings=settings)
        logger.info("inventory_consumer_mode_selected", extra={"mode": "batch"})
    else:
        consumer = KafkaInventoryConsumer.from_settings(settings=settings)
        logger.info("inventory_consumer_mode_selected", extra={"mode": "single"})

    _register_signal_handlers(consumer)
    consumer.consume_forever()
    logger.info("inventory_consumer_worker_stopped")


if __name__ == "__main__":
    main()
