from redis import Redis

from app.config.settings import get_settings


def get_redis_client() -> Redis:
    settings = get_settings()
    return Redis.from_url(settings.redis_url, decode_responses=True)


def ping_redis() -> bool:
    return bool(get_redis_client().ping())
