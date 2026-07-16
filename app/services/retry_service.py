from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class RetryResult:
    attempts: int


class RetryService:
    def __init__(
        self,
        max_attempts: int,
        initial_backoff_seconds: float,
        backoff_multiplier: float,
        max_backoff_seconds: float,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._max_attempts = max_attempts
        self._initial_backoff_seconds = initial_backoff_seconds
        self._backoff_multiplier = backoff_multiplier
        self._max_backoff_seconds = max_backoff_seconds
        self._sleep = sleep

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

    def execute(
        self,
        operation: Callable[[], T],
        retryable_exceptions: tuple[type[Exception], ...],
        operation_name: str,
        on_retry: Callable[[int, Exception, float], None] | None = None,
    ) -> tuple[T, RetryResult]:
        for attempt in range(1, self._max_attempts + 1):
            try:
                return operation(), RetryResult(attempts=attempt)
            except retryable_exceptions as exc:
                if attempt >= self._max_attempts:
                    logger.error(
                        "retry_exhausted",
                        extra={
                            "operation_name": operation_name,
                            "attempt": attempt,
                            "max_attempts": self._max_attempts,
                            "error": str(exc),
                        },
                    )
                    raise

                backoff_seconds = self._calculate_backoff(attempt)
                if on_retry is not None:
                    on_retry(attempt, exc, backoff_seconds)
                self._sleep(backoff_seconds)

    def _calculate_backoff(self, attempt: int) -> float:
        if self._initial_backoff_seconds <= 0:
            return 0
        raw_backoff = self._initial_backoff_seconds * (self._backoff_multiplier ** (attempt - 1))
        return min(raw_backoff, self._max_backoff_seconds)
