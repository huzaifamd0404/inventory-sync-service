"""Redis batch operations using pipelines for efficient cache updates."""
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class RedisBatchError(Exception):
    """Raised when Redis batch operations fail."""


@dataclass(frozen=True)
class CacheOperation:
    """Represents a single cache operation."""

    key: str
    value: str | None = None  # None indicates delete operation
    ttl_seconds: int | None = None


class RedisBatchClient:
    """
    Handles batched Redis operations using pipelines for efficiency.

    Implements:
    - Pipeline batching to reduce round trips
    - Error handling and recovery
    - Metrics collection
    - Logging for observability
    """

    def __init__(self, redis_client: Redis) -> None:
        """
        Initialize Redis batch client.

        Args:
            redis_client: Redis client instance
        """
        self._redis = redis_client

    def batch_set(self, operations: list[CacheOperation]) -> tuple[int, int]:
        """
        Execute a batch of set operations using a pipeline.

        Args:
            operations: List of cache operations to execute

        Returns:
            Tuple of (successful_operations, failed_operations)

        Raises:
            RedisBatchError: If pipeline execution fails
        """
        if not operations:
            return 0, 0

        try:
            pipe = self._redis.pipeline(transaction=False)

            for operation in operations:
                if operation.value is not None:
                    if operation.ttl_seconds:
                        pipe.setex(
                            operation.key,
                            operation.ttl_seconds,
                            operation.value,
                        )
                    else:
                        pipe.set(operation.key, operation.value)
                else:
                    # Delete operation
                    pipe.delete(operation.key)

            results = pipe.execute()

            successful = sum(1 for result in results if result)
            failed = len(results) - successful

            logger.debug(
                "redis_batch_set_completed",
                extra={
                    "batch_size": len(operations),
                    "successful": successful,
                    "failed": failed,
                },
            )

            return successful, failed
        except RedisError as exc:
            logger.exception(
                "redis_batch_set_error",
                extra={
                    "batch_size": len(operations),
                    "error": str(exc),
                },
            )
            raise RedisBatchError(f"Redis batch set failed: {exc}") from exc

    def batch_delete(self, keys: list[str]) -> tuple[int, int]:
        """
        Execute a batch of delete operations using a pipeline.

        Args:
            keys: List of keys to delete

        Returns:
            Tuple of (keys_deleted, failed_operations)

        Raises:
            RedisBatchError: If pipeline execution fails
        """
        if not keys:
            return 0, 0

        try:
            pipe = self._redis.pipeline(transaction=False)

            for key in keys:
                pipe.delete(key)

            results = pipe.execute()

            deleted = sum(results)
            failed = len(results) - deleted

            logger.debug(
                "redis_batch_delete_completed",
                extra={
                    "batch_size": len(keys),
                    "deleted": deleted,
                    "failed": failed,
                },
            )

            return deleted, failed
        except RedisError as exc:
            logger.exception(
                "redis_batch_delete_error",
                extra={
                    "batch_size": len(keys),
                    "error": str(exc),
                },
            )
            raise RedisBatchError(f"Redis batch delete failed: {exc}") from exc

    def batch_get(self, keys: list[str]) -> tuple[dict[str, str], int]:
        """
        Execute a batch of get operations using a pipeline.

        Args:
            keys: List of keys to retrieve

        Returns:
            Tuple of (dict of key-value pairs, failed_operations)

        Raises:
            RedisBatchError: If pipeline execution fails
        """
        if not keys:
            return {}, 0

        try:
            pipe = self._redis.pipeline(transaction=False)

            for key in keys:
                pipe.get(key)

            results = pipe.execute()

            values = {key: val for key, val in zip(keys, results) if val is not None}
            failed = sum(1 for val in results if val is None)

            logger.debug(
                "redis_batch_get_completed",
                extra={
                    "batch_size": len(keys),
                    "found": len(values),
                    "not_found": failed,
                },
            )

            return values, failed
        except RedisError as exc:
            logger.exception(
                "redis_batch_get_error",
                extra={
                    "batch_size": len(keys),
                    "error": str(exc),
                },
            )
            raise RedisBatchError(f"Redis batch get failed: {exc}") from exc

    def batch_mset(self, data: dict[str, str]) -> int:
        """
        Execute a multi-set operation using a pipeline.

        Args:
            data: Dictionary of key-value pairs to set

        Returns:
            Number of keys set

        Raises:
            RedisBatchError: If operation fails
        """
        if not data:
            return 0

        try:
            pipe = self._redis.pipeline(transaction=False)
            pipe.mset(data)
            pipe.execute()

            logger.debug(
                "redis_batch_mset_completed",
                extra={"batch_size": len(data)},
            )

            return len(data)
        except RedisError as exc:
            logger.exception(
                "redis_batch_mset_error",
                extra={
                    "batch_size": len(data),
                    "error": str(exc),
                },
            )
            raise RedisBatchError(f"Redis batch mset failed: {exc}") from exc


def get_redis_batch_client(redis_client: Redis | None = None) -> RedisBatchClient:
    """
    Factory function for creating Redis batch client.

    Args:
        redis_client: Optional Redis client instance

    Returns:
        RedisBatchClient instance
    """
    if redis_client is None:
        from app.cache.redis_client import get_redis_client

        redis_client = get_redis_client()

    return RedisBatchClient(redis_client)
