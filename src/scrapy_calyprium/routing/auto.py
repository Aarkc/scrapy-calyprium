"""Spider-side auto-routing orchestrator.

Wires together:

- `LocalFetcher` (httpcloak / curl_cffi backend, raw bytes)
- `DomainCache` (per-spider, AAR-14 circuit breaker)
- `SolveClient` (Mimic /api/solve cold path)

Flow per request:

  1. If cache says heavy and not due for re-probe → caller should fall through
     to legacy MimicMiddleware which uses /api/fetch + browser. We don't try
     to do browser navigation locally.
  2. If cache has cookie slots → pick one, do local httpcloak with cookies,
     check is_blocked. On success: return. On block: mark slot failed,
     fall through.
  3. Otherwise → local httpcloak without cookies. On success: cache as
     "light" and return. On block: fall through.
  4. Call /api/solve to get fresh cookies → cache as a new slot → retry
     local httpcloak with the new cookies.
  5. If solve returned success=False (real domain block) → mark domain heavy
     and surface a 403 to the caller. Caller decides whether to fall through
     to the legacy browser path or fail the Scrapy request.

Returns `RouteResult`. The caller (MimicMiddleware) wraps it in a Scrapy
Response.

AAR-17.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from scrapy_calyprium.routing.block_detect import is_blocked
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import (
    LocalFetchError,
    LocalFetchResult,
    LocalFetcher,
)
from scrapy_calyprium.routing.solve_client import SolveClient, SolveError

logger = logging.getLogger(__name__)


@dataclass
class RouteResult:
    """Outcome of an auto-route attempt."""
    fetch: Optional[LocalFetchResult]
    routing_method: str  # "httpcloak_light" | "httpcloak_cookies" | "solve_then_replay" | "fallback_legacy"
    blocked: bool
    domain_level: str
    needs_legacy_fallback: bool = False
    error: Optional[str] = None


class SpiderAutoRouter:
    def __init__(
        self,
        fetcher: LocalFetcher,
        cache: DomainCache,
        solve_client: SolveClient,
        proxy_url: Optional[str] = None,
    ):
        self.fetcher = fetcher
        self.cache = cache
        self.solve_client = solve_client
        self.proxy_url = proxy_url

    async def fetch(self, url: str, *, domain: str) -> RouteResult:
        # Step 1: heavy with no re-probe due → caller should fall through to legacy
        level = self.cache.get_level(domain)
        if level == "heavy":
            if not self.cache.is_due_for_reprobe(domain):
                return RouteResult(
                    fetch=None,
                    routing_method="fallback_legacy",
                    blocked=True,
                    domain_level=level,
                    needs_legacy_fallback=True,
                )
            logger.info(
                "AutoRouter: %s heavy but due for re-probe, trying httpcloak", domain,
            )

        # Step 2: cookies in pool → try replay
        entry = self.cache.get(domain)
        if entry and entry.level == "cookies":
            slot = entry.next_slot()
            if slot:
                try:
                    result = await self.fetcher.fetch(
                        url=url,
                        cookies=slot.cookies,
                        user_agent=slot.user_agent,
                        proxy_url=self.proxy_url,
                        proxy_session_id=slot.proxy_session_id,
                        preset=slot.preset,
                    )
                except LocalFetchError as exc:
                    logger.info(
                        "AutoRouter: cookie replay infra error for %s: %s",
                        domain, exc,
                    )
                    self.cache.record_slot_failure(domain, slot.slot_id, status_code=None)
                else:
                    if not is_blocked(result.status_code, result.body):
                        self.cache.record_slot_success(domain, slot.slot_id)
                        return RouteResult(
                            fetch=result,
                            routing_method="httpcloak_cookies",
                            blocked=False,
                            domain_level="cookies",
                        )
                    self.cache.record_slot_failure(
                        domain, slot.slot_id, status_code=result.status_code,
                    )

        # Step 3: try httpcloak without cookies (light path)
        try:
            result = await self.fetcher.fetch(
                url=url,
                proxy_url=self.proxy_url,
            )
        except LocalFetchError as exc:
            logger.info(
                "AutoRouter: light httpcloak failed for %s: %s", domain, exc,
            )
            return RouteResult(
                fetch=None,
                routing_method="fallback_legacy",
                blocked=True,
                domain_level=self.cache.get_level(domain),
                needs_legacy_fallback=True,
                error=str(exc),
            )

        if not is_blocked(result.status_code, result.body):
            self.cache.set_light(domain)
            return RouteResult(
                fetch=result,
                routing_method="httpcloak_light",
                blocked=False,
                domain_level="light",
            )

        # Step 4: ask Mimic for a solve
        logger.info(
            "AutoRouter: %s blocked at httpcloak (status=%d), calling /api/solve",
            domain, result.status_code,
        )
        try:
            solve = await self.solve_client.solve(domain=domain, target_url=url)
        except SolveError as exc:
            logger.warning("AutoRouter: solve failed for %s: %s", domain, exc)
            return RouteResult(
                fetch=result,
                routing_method="fallback_legacy",
                blocked=True,
                domain_level=self.cache.get_level(domain),
                needs_legacy_fallback=True,
                error=str(exc),
            )

        if not solve.success:
            self.cache._maybe_promote_heavy(domain, "solve_returned_no_cookies")
            return RouteResult(
                fetch=result,
                routing_method="fallback_legacy",
                blocked=True,
                domain_level="heavy",
                needs_legacy_fallback=True,
                error=solve.error or "solve returned no cookies",
            )

        # Cache the new slot and replay
        slot = self.cache.set_cookies_from_solve(
            domain=domain,
            cookies=solve.cookies,
            user_agent=solve.user_agent,
            proxy_session_id=solve.proxy_session_id,
            preset=solve.preset,
        )
        try:
            replay = await self.fetcher.fetch(
                url=url,
                cookies=slot.cookies,
                user_agent=slot.user_agent,
                proxy_url=self.proxy_url,
                proxy_session_id=slot.proxy_session_id,
                preset=slot.preset,
            )
        except LocalFetchError as exc:
            logger.warning("AutoRouter: post-solve replay infra error for %s: %s", domain, exc)
            return RouteResult(
                fetch=None,
                routing_method="solve_then_replay",
                blocked=True,
                domain_level="cookies",
                needs_legacy_fallback=True,
                error=str(exc),
            )

        if is_blocked(replay.status_code, replay.body):
            self.cache.record_slot_failure(
                domain, slot.slot_id, status_code=replay.status_code,
            )
            return RouteResult(
                fetch=replay,
                routing_method="solve_then_replay",
                blocked=True,
                domain_level="cookies",
                needs_legacy_fallback=True,
            )

        return RouteResult(
            fetch=replay,
            routing_method="solve_then_replay",
            blocked=False,
            domain_level="cookies",
        )
