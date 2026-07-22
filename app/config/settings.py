from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="Inventory Sync Service")
    app_version: str = Field(default="0.1.0")
    app_env: str = Field(default="development")
    app_port: int = Field(default=8000)
    log_level: str = Field(default="INFO")

    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="inventory")
    postgres_user: str = Field(default="inventory_user")
    postgres_password: str = Field(default="inventory_pass")
    database_echo: bool = Field(default=False)
    database_pool_size: int = Field(default=10, ge=1)
    database_max_overflow: int = Field(default=20, ge=0)
    database_pool_timeout: int = Field(default=30, ge=1)
    database_pool_recycle: int = Field(default=1800, ge=1)

    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)

    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic_inventory_events: str = Field(default="inventory.events")
    kafka_topic_inventory_updates: str = Field(default="inventory_updates")
    kafka_topic_inventory_dlq: str = Field(default="inventory_dlq")
    kafka_client_id: str = Field(default="inventory-sync-service")
    kafka_producer_retries: int = Field(default=5, ge=0)
    kafka_producer_retry_backoff_ms: int = Field(default=200, ge=0)
    kafka_producer_linger_ms: int = Field(default=5, ge=0)
    kafka_producer_request_timeout_ms: int = Field(default=30000, ge=1000)
    kafka_producer_delivery_timeout_ms: int = Field(default=120000, ge=1000)
    kafka_producer_max_block_ms: int = Field(default=10000, ge=1000)
    kafka_publish_attempts: int = Field(default=3, ge=1)
    kafka_publish_retry_backoff_seconds: float = Field(default=0.25, ge=0)
    kafka_publish_timeout_seconds: int = Field(default=10, ge=1)
    kafka_topic_sales_events: str = Field(default="sales_events")
    kafka_consumer_group_id: str = Field(default="inventory-sync-consumer")
    kafka_consumer_sales_group_id: str = Field(default="sales-sync-consumer")
    kafka_consumer_max_attempts: int = Field(default=3, ge=1)
    kafka_consumer_retry_initial_backoff_seconds: float = Field(default=0.5, ge=0)
    kafka_consumer_retry_backoff_multiplier: float = Field(default=2.0, ge=1.0)
    kafka_consumer_retry_max_backoff_seconds: float = Field(default=30.0, ge=0)

    # Batch processing configuration
    batch_processing_enabled: bool = Field(default=True)
    batch_size: int = Field(default=100, ge=1, le=10000)
    batch_max_wait_ms: int = Field(default=5000, ge=1, le=60000)
    kafka_consumer_poll_timeout_ms: int = Field(default=1000, ge=100, le=30000)

    enable_dependency_health_checks: bool = Field(default=False)

    @property
    def sqlalchemy_database_uri(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/0"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
