"""Bounded-concurrency + retry primitives for the async domain layer.

Every LLM/embedding stage is network-I/O bound, so the right way to speed it up
is to run many awaits at once under a ceiling — not threads (the GIL is
irrelevant while a coroutine is parked on a socket) and not unbounded fan-out
(which would trip provider rate limits). These two helpers are the whole story:

    * :func:`with_retry`     — one call, made resilient (timeout + jittered backoff).
    * :func:`gather_bounded` — many calls, run at most ``limit`` at a time.

They take *factories* (zero-arg callables returning a fresh coroutine) rather
than coroutine objects, because a coroutine can only be awaited once — a retry
needs to build a new one each attempt.
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Awaitable, Callable, Sequence, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

CoroFactory = Callable[[], Awaitable[T]]

_MAX_BACKOFF_SECONDS = 30.0


class RetryExhaustedError(RuntimeError):
    """Raised when :func:`with_retry` uses up every attempt.

    ``last_exc`` holds the final underlying failure so callers can inspect or
    re-classify it.
    """

    def __init__(self, label: str, attempts: int, last_exc: BaseException) -> None:
        super().__init__(f"{label or 'operation'} failed after {attempts} attempt(s): {last_exc!r}")
        self.last_exc = last_exc


def _backoff_delay(attempt: int, base_delay: float) -> float:
    """Exponential backoff with full jitter: uniform(0, min(cap, base*2**attempt))."""
    ceiling = min(_MAX_BACKOFF_SECONDS, base_delay * (2 ** attempt))
    return random.uniform(0, ceiling)


async def with_retry(
    factory: CoroFactory[T],
    *,
    attempts: int = 4,
    base_delay: float = 1.0,
    timeout: float | None = None,
    retry_on: tuple[type[BaseException], ...] = (),
    label: str = "",
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> T:
    """Await ``factory()``; on a retryable failure, back off and try again.

    Args:
        factory: Zero-arg callable returning a fresh awaitable per attempt.
        attempts: Total attempts including the first.
        base_delay: Base seconds for exponential backoff with full jitter.
        timeout: Per-attempt timeout (``asyncio.wait_for``); None disables it.
        retry_on: Exception types that trigger a retry. ``asyncio.TimeoutError``
            is always retried when a timeout is set. Anything else propagates
            immediately (a bug or a permanent error should not be retried).
        label: Human label for logs/metrics.
        on_retry: Optional hook ``(attempt_index, exc)`` fired before each backoff
            (e.g. to bump a retry metric).

    Returns:
        The successful result.

    Raises:
        RetryExhaustedError: All attempts exhausted; ``.last_exc`` has the cause.
    """
    retryable = retry_on + ((asyncio.TimeoutError,) if timeout is not None else ())
    last_exc: BaseException | None = None

    for attempt in range(attempts):
        try:
            coro = factory()
            if timeout is not None:
                return await asyncio.wait_for(coro, timeout=timeout)
            return await coro
        except retryable as exc:  # type: ignore[misc]
            last_exc = exc
            if on_retry is not None:
                on_retry(attempt, exc)
            if attempt + 1 >= attempts:
                break
            delay = _backoff_delay(attempt, base_delay)
            logger.warning(
                "retrying %s after %s (attempt %d/%d), backing off %.2fs",
                label or "operation", type(exc).__name__, attempt + 1, attempts, delay,
            )
            await asyncio.sleep(delay)

    assert last_exc is not None  # loop only breaks after setting last_exc
    raise RetryExhaustedError(label, attempts, last_exc)


async def gather_bounded(
    factories: Sequence[CoroFactory[T]],
    *,
    limit: int,
    label: str = "",
    return_exceptions: bool = True,
) -> list[T | BaseException]:
    """Run ``factories`` concurrently, at most ``limit`` in flight at once.

    Results are returned in the same order as ``factories``. By default a failing
    item yields its exception in the result list (item-level isolation) rather
    than cancelling its siblings — the caller decides what a partial failure
    means. Set ``return_exceptions=False`` to fail fast on the first error.
    """
    if not factories:
        return []
    semaphore = asyncio.Semaphore(max(1, limit))

    async def _run(index: int, factory: CoroFactory[T]) -> T:
        async with semaphore:
            try:
                return await factory()
            except Exception:
                logger.exception("gather_bounded item %d failed (%s)", index, label or "task")
                raise

    tasks = [_run(i, f) for i, f in enumerate(factories)]
    return await asyncio.gather(*tasks, return_exceptions=return_exceptions)
