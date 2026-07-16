import pytest

from app.services.retry_service import RetryService


class RetryableError(Exception):
    pass


def test_retry_service_retries_with_exponential_backoff() -> None:
    sleep_calls: list[float] = []
    attempts = 0

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RetryableError("transient")
        return "ok"

    service = RetryService(
        max_attempts=4,
        initial_backoff_seconds=0.5,
        backoff_multiplier=2.0,
        max_backoff_seconds=5.0,
        sleep=lambda seconds: sleep_calls.append(seconds),
    )

    result, retry_result = service.execute(
        operation=operation,
        retryable_exceptions=(RetryableError,),
        operation_name="test_operation",
    )

    assert result == "ok"
    assert retry_result.attempts == 3
    assert sleep_calls == [0.5, 1.0]


def test_retry_service_raises_when_attempts_exhausted() -> None:
    service = RetryService(
        max_attempts=3,
        initial_backoff_seconds=0,
        backoff_multiplier=2.0,
        max_backoff_seconds=10,
        sleep=lambda _: None,
    )

    with pytest.raises(RetryableError):
        service.execute(
            operation=lambda: (_ for _ in ()).throw(RetryableError("still failing")),
            retryable_exceptions=(RetryableError,),
            operation_name="test_operation",
        )
