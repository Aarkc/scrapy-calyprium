"""Authentication for the spider-facing Forge endpoints.

Every Forge call the SDK makes (run stats, traces, Prism checkpoint,
recrawl crawl-complete / filter-stale / stale-urls, targets submit /
mark-crawled / pending) authenticates the same way:

1. ``Authorization: Bearer <CALYPRIUM_API_KEY>`` — the run's per-user spider
   key (scope ``scraper:write``). Preferred: it is scoped to the owning user,
   so spider code that reads it can't impersonate anyone else (AAR-32/AAR-47).
2. ``X-Service-Secret: <FORGE_SERVICE_SECRET>`` + ``X-User-Id`` — legacy
   service-to-service auth. Used only when no API key is configured, or as a
   one-way fallback when Forge rejects the key (401/403) during the rollout
   window where an older Forge doesn't accept spider keys yet.
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Any, Callable, Dict, Optional, TypeVar

logger = logging.getLogger(__name__)

_AUTH_REJECTED = (401, 403)

R = TypeVar("R")


def _setting(settings: Any, key: str) -> str:
    value = None
    if settings is not None:
        try:
            value = settings.get(key)
        except AttributeError:
            value = None
    return value or os.getenv(key, "") or ""


class ForgeAuth:
    """Resolves (and, on rejection, downgrades) Forge request credentials."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        service_secret: Optional[str] = None,
        user_id: Optional[str] = None,
    ):
        self.api_key = api_key or ""
        self.service_secret = service_secret or ""
        self.user_id = user_id or "internal"
        self._use_secret = not self.api_key
        self._lock = threading.Lock()

    @classmethod
    def from_settings(cls, settings: Any) -> "ForgeAuth":
        user_id = (
            _setting(settings, "RECRAWL_USER_ID")
            or _setting(settings, "SPIDER_USER_ID")
            or "internal"
        )
        return cls(
            api_key=_setting(settings, "CALYPRIUM_API_KEY"),
            service_secret=_setting(settings, "FORGE_SERVICE_SECRET"),
            user_id=user_id,
        )

    def __bool__(self) -> bool:
        return bool(self.api_key or self.service_secret)

    @property
    def mode(self) -> str:
        if self._use_secret:
            return "service_secret" if self.service_secret else "none"
        return "bearer"

    def headers(self) -> Dict[str, str]:
        if not self._use_secret:
            return {"Authorization": f"Bearer {self.api_key}"}
        if self.service_secret:
            return {"X-Service-Secret": self.service_secret, "X-User-Id": self.user_id}
        return {}

    def fallback(self) -> bool:
        """Switch from the API key to the service secret, once. Returns True if
        the caller should retry with the new ``headers()``."""
        with self._lock:
            if self._use_secret or not self.service_secret:
                return False
            self._use_secret = True
        logger.warning(
            "Forge rejected the spider API key; falling back to FORGE_SERVICE_SECRET "
            "(upgrade Forge to accept spider keys on spider-facing endpoints)"
        )
        return True

    def call(self, send: Callable[[Dict[str, str]], R]) -> R:
        """Run ``send(headers)``; retry once with the service secret if Forge
        rejects the API key. ``send`` returns an httpx/requests response."""
        response = send(self.headers())
        status = getattr(response, "status_code", None)
        if status in _AUTH_REJECTED and self.fallback():
            response = send(self.headers())
        return response
