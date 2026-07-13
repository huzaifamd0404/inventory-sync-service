import logging

from app.config.logging import configure_logging
from app.config.settings import get_settings
from app.consumer.kafka_consumer import KafkaInventoryConsumer

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger.info("inventory_consumer_worker_starting")
    KafkaInventoryConsumer.from_settings(settings=settings).consume_forever()


if __name__ == "__main__":
    main()
