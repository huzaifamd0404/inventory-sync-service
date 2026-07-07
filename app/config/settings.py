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

    redis_host: str = Field(default="localhost")
    redis_port: int = Field(default=6379)

    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic_inventory_events: str = Field(default="inventory.events")

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
