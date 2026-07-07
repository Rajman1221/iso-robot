"""In-memory cache of successful external verifications.

Once a user's external token is verified, we trust it for a sliding window
(``external_auth_cache_minutes``, default 30) so frequent calls — especially
pipeline-status polling — don't hammer the external backend on every request.

Notes / limitations:
- The cache key is a SHA-256 hash of the token, never the raw token.
- This store is per-process. With multiple uvicorn workers each worker keeps its
  own cache; a shared Redis-backed store can replace this later for a cluster.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class VerifiedContext:
    """The trusted identity captured at verification time."""

    user_id: str
    email: str
    org_id: str


@dataclass
class _Entry:
    context: VerifiedContext
    expires_at: float


class VerificationCache:
    """Async-safe, sliding-TTL cache keyed by a hash of the external token."""

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = ttl_seconds
        self._store: Dict[str, _Entry] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    async def get(self, token: str) -> Optional[VerifiedContext]:
        """Return the cached context if present and unexpired, sliding the TTL."""
        key = self._key(token)
        now = time.monotonic()
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                self._store.pop(key, None)
                return None
            entry.expires_at = now + self._ttl  
            return entry.context

    async def set(self, token: str, context: VerifiedContext) -> None:
        key = self._key(token)
        now = time.monotonic()
        async with self._lock:
            self._purge_expired(now)
            self._store[key] = _Entry(context=context, expires_at=now + self._ttl)

    def _purge_expired(self, now: float) -> None:
        expired = [k for k, e in self._store.items() if e.expires_at <= now]
        for k in expired:
            self._store.pop(k, None)


_cache: Optional[VerificationCache] = None


def get_verification_cache(ttl_seconds: float) -> VerificationCache:
    """Return the process-wide cache, creating it on first use."""
    global _cache
    if _cache is None:
        _cache = VerificationCache(ttl_seconds)
    return _cache
