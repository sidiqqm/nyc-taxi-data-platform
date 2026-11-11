"""Retry with exponential backoff and jitter, without an extra dependency."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

logger = logging.getLogger(__name__)
T = TypeVar("T")


def retry_call(
    func: Callable[[], T],
    *,
    retry_on: tuple[type[BaseException], ...],
    max_attempts: int,
    base_delay: float,
    max_delay: float = 60.0,
    sleep: Callable[[float], None] = time.sleep,
    description: str = "operation",
) -> T:
    """Call func(); on an exception in retry_on wait and try again, up to max_attempts.

    The wait before attempt n+1 is base_delay * 2**(n-1), capped at max_delay, then
    multiplied by a random factor in [0.5, 1.0] so parallel jobs do not retry in lockstep.
    Exceptions not listed in retry_on propagate immediately. After the last attempt the
    last exception is re-raised.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except retry_on as exc:
            if attempt == max_attempts:
                raise
            delay = min(max_delay, base_delay * 2 ** (attempt - 1)) * random.uniform(0.5, 1.0)
            logger.warning(
                "%s failed (attempt %d/%d): %s; retrying in %.1fs",
                description,
                attempt,
                max_attempts,
                exc,
                delay,
            )
            sleep(delay)
    raise AssertionError("unreachable: max_attempts must be at least 1")