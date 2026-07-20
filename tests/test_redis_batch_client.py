"""Tests for Redis batch operations."""
from __future__ import annotations

import pytest

from app.cache.redis_batch_client import CacheOperation, RedisBatchClient, RedisBatchError
from redis.exceptions import RedisError


class FakeRedis:
    """Mock Redis client for testing."""

    def __init__(self, fail_on_execute: bool = False) -> None:
        self.values: dict[str, str] = {}
        self.fail_on_execute = fail_on_execute

    def set(self, key: str, value: str) -> bool:
        self.values[key] = value
        return True

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def delete(self, key: str) -> int:
        if key in self.values:
            del self.values[key]
            return 1
        return 0

    def mset(self, data: dict[str, str]) -> bool:
        self.values.update(data)
        return True

    def pipeline(self, transaction: bool = False):
        return FakePipeline(self, fail_on_execute=self.fail_on_execute)


class FakePipeline:
    """Mock Redis pipeline for testing."""

    def __init__(self, redis_client: FakeRedis, fail_on_execute: bool = False) -> None:
        self._redis = redis_client
        self._commands: list[tuple] = []
        self._fail_on_execute = fail_on_execute

    def set(self, key: str, value: str) -> "FakePipeline":
        self._commands.append(("set", key, value))
        return self

    def setex(self, key: str, ttl: int, value: str) -> "FakePipeline":
        self._commands.append(("setex", key, ttl, value))
        return self

    def delete(self, key: str) -> "FakePipeline":
        self._commands.append(("delete", key))
        return self

    def get(self, key: str) -> "FakePipeline":
        self._commands.append(("get", key))
        return self

    def mset(self, data: dict[str, str]) -> "FakePipeline":
        self._commands.append(("mset", data))
        return self

    def execute(self) -> list:
        if self._fail_on_execute:
            raise RedisError("Redis connection failed")

        results = []
        for cmd in self._commands:
            if cmd[0] == "set":
                self._redis.set(cmd[1], cmd[2])
                results.append(True)
            elif cmd[0] == "setex":
                self._redis.set(cmd[1], cmd[3])
                results.append(True)
            elif cmd[0] == "delete":
                result = self._redis.delete(cmd[1])
                results.append(result)
            elif cmd[0] == "get":
                result = self._redis.get(cmd[1])
                results.append(result)
            elif cmd[0] == "mset":
                self._redis.mset(cmd[1])
                results.append(True)
        return results


class TestRedisBatchClient:
    """Tests for Redis batch operations."""

    def test_batch_set_operations(self) -> None:
        """Test batch set operations."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        operations = [
            CacheOperation(key="key1", value="value1"),
            CacheOperation(key="key2", value="value2"),
            CacheOperation(key="key3", value="value3"),
        ]

        successful, failed = batch_client.batch_set(operations)

        assert successful == 3
        assert failed == 0
        assert redis_client.values["key1"] == "value1"
        assert redis_client.values["key2"] == "value2"
        assert redis_client.values["key3"] == "value3"

    def test_batch_set_with_ttl(self) -> None:
        """Test batch set operations with TTL."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        operations = [
            CacheOperation(key="key1", value="value1", ttl_seconds=3600),
            CacheOperation(key="key2", value="value2", ttl_seconds=7200),
        ]

        successful, failed = batch_client.batch_set(operations)

        assert successful == 2
        assert failed == 0
        assert redis_client.values["key1"] == "value1"
        assert redis_client.values["key2"] == "value2"

    def test_batch_delete_operations(self) -> None:
        """Test batch delete operations."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        # Set some keys first
        redis_client.set("key1", "value1")
        redis_client.set("key2", "value2")
        redis_client.set("key3", "value3")

        # Delete them in batch
        deleted, failed = batch_client.batch_delete(["key1", "key2", "key3"])

        assert deleted == 3
        assert "key1" not in redis_client.values
        assert "key2" not in redis_client.values
        assert "key3" not in redis_client.values

    def test_batch_get_operations(self) -> None:
        """Test batch get operations."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        # Set some keys first
        redis_client.set("key1", "value1")
        redis_client.set("key2", "value2")

        # Get them in batch
        values, missing = batch_client.batch_get(["key1", "key2", "key3"])

        assert len(values) == 2
        assert values["key1"] == "value1"
        assert values["key2"] == "value2"
        assert missing == 1  # key3 not found

    def test_batch_mset_operations(self) -> None:
        """Test batch mset operations."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }

        keys_set = batch_client.batch_mset(data)

        assert keys_set == 3
        assert redis_client.values["key1"] == "value1"
        assert redis_client.values["key2"] == "value2"
        assert redis_client.values["key3"] == "value3"

    def test_batch_set_empty_operations(self) -> None:
        """Test batch set with empty operations."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        successful, failed = batch_client.batch_set([])

        assert successful == 0
        assert failed == 0

    def test_batch_delete_empty_keys(self) -> None:
        """Test batch delete with empty keys."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        deleted, failed = batch_client.batch_delete([])

        assert deleted == 0
        assert failed == 0

    def test_batch_get_empty_keys(self) -> None:
        """Test batch get with empty keys."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        values, missing = batch_client.batch_get([])

        assert len(values) == 0
        assert missing == 0

    def test_batch_mset_empty_data(self) -> None:
        """Test batch mset with empty data."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        keys_set = batch_client.batch_mset({})

        assert keys_set == 0

    def test_batch_delete_operations_raises_on_redis_error(self) -> None:
        """Test batch delete raises RedisBatchError on Redis error."""
        redis_client = FakeRedis(fail_on_execute=True)
        batch_client = RedisBatchClient(redis_client)

        with pytest.raises(RedisBatchError):
            batch_client.batch_delete(["key1", "key2"])

    def test_batch_get_operations_raises_on_redis_error(self) -> None:
        """Test batch get raises RedisBatchError on Redis error."""
        redis_client = FakeRedis(fail_on_execute=True)
        batch_client = RedisBatchClient(redis_client)

        with pytest.raises(RedisBatchError):
            batch_client.batch_get(["key1", "key2"])

    def test_batch_mset_operations_raises_on_redis_error(self) -> None:
        """Test batch mset raises RedisBatchError on Redis error."""
        redis_client = FakeRedis(fail_on_execute=True)
        batch_client = RedisBatchClient(redis_client)

        with pytest.raises(RedisBatchError):
            batch_client.batch_mset({"key1": "value1"})

    def test_batch_operations_with_large_dataset(self) -> None:
        """Test batch operations with large dataset."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        # Create 1000 operations
        operations = [
            CacheOperation(key=f"key{i}", value=f"value{i}") for i in range(1000)
        ]

        successful, failed = batch_client.batch_set(operations)

        assert successful == 1000
        assert failed == 0
        assert len(redis_client.values) == 1000

    def test_batch_delete_with_non_existent_keys(self) -> None:
        """Test batch delete with non-existent keys."""
        redis_client = FakeRedis()
        batch_client = RedisBatchClient(redis_client)

        # Try to delete keys that don't exist
        deleted, failed = batch_client.batch_delete(["nonexistent1", "nonexistent2"])

        # Should succeed but with 0 deleted
        assert deleted == 0
        assert failed == 0
