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

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

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
    # Identifier of the cookie slot used for this fetch (cookies / solve_then_replay
    # paths only). The middleware stuffs this into request.meta so the spider's
    # parse callback can call back to record_slot_failure when the response
    # was structurally valid (status 200, passes is_blocked) but contained no
    # useful data — a silent block. Without this signal the rate cap never
    # learns about Cloudflare interstitials served with status 200.
    slot_id: Optional[str] = None


class SpiderAutoRouter:
    def __init__(
        self,
        fetcher: LocalFetcher,
        cache: DomainCache,
        solve_client: SolveClient,
        proxy_url: Optional[str] = None,
        target_pool_size: int = 8,
        refill_interval: float = 5.0,
    ):
        self.fetcher = fetcher
        self.cache = cache
        self.solve_client = solve_client
        self.proxy_url = proxy_url
        # Per-domain locks for solve coalescing. When 32 concurrent spider
        # requests for the same domain all hit the block path, only one
        # actually calls /api/solve; the rest wait on the lock and reuse the
        # cookie pool the winner populated.
        self._solve_locks: Dict[str, asyncio.Lock] = {}

        # Phase 5: proactive cookie pool expansion. The hot path solve
        # coalescing collapses N concurrent requests into 1 solve, which is
        # right for the FIRST request to a domain (don't blow your solve
        # budget on a single page). But after that, the spider keeps using
        # that one slot until it dies — and at concurrency 32 the slot
        # tends to die in seconds because every request that gets a 403
        # bumps fail_count toward MAX_SLOT_FAILURES=3. The IP reputation
        # rotation in Phase 4 has nowhere to send traffic if the pool is
        # always size 1.
        #
        # The refill loop runs in the background per active domain and
        # mints replacement slots until the pool reaches target_pool_size.
        # Refill solves bypass the per-domain solve_lock (they're not
        # racing other callers, they're filling capacity) but still respect
        # Mimic's per-domain rate limit.
        self.target_pool_size = target_pool_size
        self.refill_interval = refill_interval
        self._refill_tasks: Dict[str, asyncio.Task] = {}
        self._refill_stop = False
        # Last hot-path refill check per domain. The original design used a
        # long-lived asyncio.create_task background loop, but in Scrapy's
        # Twisted-driven environment that task's `await asyncio.sleep` never
        # gets pumped — Scrapy's reactor only services the asyncio loop
        # when there's a current await chain. Driving refill from the fetch
        # hot path is more robust: every request opportunistically checks
        # whether a refill is due, and if so spawns a fire-and-forget solve
        # via asyncio.create_task (which IS pumped because the spider's own
        # await chain is active).
        self._last_refill_check: Dict[str, float] = {}
        self._refill_in_flight: Dict[str, bool] = {}

    def _solve_lock(self, domain: str) -> asyncio.Lock:
        lock = self._solve_locks.get(domain)
        if lock is None:
            lock = asyncio.Lock()
            self._solve_locks[domain] = lock
        return lock

    def _ensure_refill_task(self, domain: str) -> None:
        """Hot-path refill check.

        Called on every fetch() after a successful solve. Cheap fast path
        (one dict lookup + one time comparison). When a refill is actually
        due, spawns a single fire-and-forget solve via asyncio.create_task.
        Multiple concurrent fetches racing this check are gated by the
        per-domain `_refill_in_flight` flag so we don't burst-fire 32
        solves on the same tick.

        Replaces the original long-lived background loop, which doesn't
        survive Scrapy's Twisted/asyncio bridge — `asyncio.sleep` inside a
        detached task never wakes because the asyncio loop is only pumped
        when there's an active await chain from a Scrapy callback.
        """
        import time as _t
        now = _t.time()
        last = self._last_refill_check.get(domain, 0.0)
        if now - last < self.refill_interval:
            return
        self._last_refill_check[domain] = now

        if self._refill_in_flight.get(domain):
            return

        entry = self.cache.get(domain)
        if entry is None or entry.level != "cookies":
            return
        live = len(entry.live_slots())
        if live >= self.target_pool_size:
            return
        if len(entry.slots) >= self.target_pool_size * 2:
            # Lots of dead slots accumulated — skip until they expire to
            # avoid unbounded growth from a hostile domain.
            return

        # Spawn one fire-and-forget refill solve. The flag is cleared in
        # the helper coroutine's finally block.
        self._refill_in_flight[domain] = True
        try:
            asyncio.create_task(self._refill_one(domain, live))
        except RuntimeError:
            self._refill_in_flight[domain] = False

    async def _refill_one(self, domain: str, live_at_check: int) -> None:
        """Mint a single fresh slot. Called via fire-and-forget from
        _ensure_refill_task. Errors are swallowed — refill is best-effort
        and a transient failure should not affect spider throughput."""
        try:
            try:
                solve = await self.solve_client.solve(domain=domain)
            except SolveError as exc:
                logger.info(
                    "AutoRouter: refill solve failed for %s: %s", domain, exc,
                )
                return
            if not solve.success:
                logger.info(
                    "AutoRouter: refill solve returned no cookies for %s",
                    domain,
                )
                return
            self.cache.set_cookies_from_solve(
                domain=domain,
                cookies=solve.cookies,
                user_agent=solve.user_agent,
                proxy_session_id=solve.proxy_session_id,
                preset=solve.preset,
                egress_ip=solve.egress_ip,
            )
            entry = self.cache.get(domain)
            new_live = len(entry.live_slots()) if entry else 0
            logger.info(
                "AutoRouter: refill added slot for %s (egress_ip=%s, "
                "pool=%d->%d/%d)",
                domain, solve.egress_ip, live_at_check, new_live,
                self.target_pool_size,
            )
        except Exception as exc:
            logger.warning(
                "AutoRouter: refill_one crashed for %s: %s", domain, exc,
            )
        finally:
            self._refill_in_flight[domain] = False

    def stop_refill(self) -> None:
        """No-op since the long-lived loop was replaced by hot-path checks.
        Kept for backwards compatibility with shutdown handlers."""
        self._refill_stop = True

    def _report_ip_outcome(
        self,
        *,
        domain: str,
        slot,
        outcome: str,
        status_code: Optional[int] = None,
    ) -> None:
        """Fire-and-forget POST to Mimic /api/ip-health/report.

        Closes the per-(domain, IP) reputation feedback loop. The spider's
        local httpcloak replays don't flow through Mimic's server-side
        routing, so without this Mimic only learns about failures from
        its own /api/fetch traffic — a small fraction of total volume.
        With it, Mimic's IP blacklist sees real spider observations and
        the next solve's pre-screen loop can rotate around the burned
        physical IP.

        Skipped silently when the slot has no resolved egress_ip — there's
        nothing to feed the IP-level reputation tracker. The session-level
        tracker still gets the report, but per-(domain, ip) doesn't.
        """
        if slot is None or not slot.egress_ip:
            return
        try:
            asyncio.create_task(
                self.solve_client.report_ip_outcome(
                    proxy_session_id=slot.proxy_session_id,
                    domain=domain,
                    outcome=outcome,
                    status_code=status_code,
                    egress_ip=slot.egress_ip,
                )
            )
        except RuntimeError:
            # No running loop — happens in some test paths. Reputation
            # is a soft signal; dropping a single report is fine.
            pass

    def report_silent_failure(
        self,
        domain: str,
        slot_id: Optional[str],
        reason: str = "no_useful_data",
    ) -> None:
        """Spider-side feedback channel for silent block detection.

        When a spider's parse callback receives a structurally-valid 200
        response that nevertheless contains no useful data (e.g. a Cloudflare
        compatibility-mode page with real markup but no product JSON), it
        calls this method so the routing layer can roll back the slot's
        success counter and feed the rate-cap learner. Without it, the
        router happily keeps firing the same dead slot at full speed.

        Two cases:
        - slot_id is set (cookie replay path): record a slot failure with
          status_code=200, which feeds the rate-cap learner.
        - slot_id is None (light path / cold discovery): we have no slot to
          blame. Drop the "light" classification so the next request escalates
          through solve instead of replaying a useless light fetch.
        """
        if slot_id:
            logger.info(
                "AutoRouter: silent failure on %s slot %s (reason=%s)",
                domain, slot_id, reason,
            )
            # Resolve the slot to get its egress_ip for the IP report
            entry = self.cache.get(domain)
            slot = None
            if entry:
                slot = next((s for s in entry.slots if s.slot_id == slot_id), None)
            self.cache.record_slot_failure(domain, slot_id, status_code=200)
            if slot is not None:
                self._report_ip_outcome(
                    domain=domain, slot=slot, outcome="blocked",
                    status_code=200,
                )
            return

        # Light-path silent failure — drop the cache entry so the next
        # request re-probes and (hopefully) escalates to /api/solve.
        entry = self.cache.get(domain)
        if entry and entry.level == "light":
            logger.info(
                "AutoRouter: light-path silent failure on %s (reason=%s); "
                "dropping light classification to force re-probe",
                domain, reason,
            )
            # Drop the entry entirely so the next request hits step 3 again
            # and escalates through the solve flow if blocked.
            self.cache._entries.pop(domain, None)

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
                # Adaptive rate limiting: defer if this slot is already at
                # the learned cap. The router waits inside the request
                # context (counts against Scrapy's CONCURRENT_REQUESTS),
                # which naturally backpressures the spider's queue without
                # needing a separate global throttle.
                if entry.learned_rpm_cap is not None:
                    while slot.requests_per_minute() >= entry.learned_rpm_cap:
                        # Try a different slot first
                        alt = entry.next_slot()
                        if alt and alt.slot_id != slot.slot_id and (
                            alt.requests_per_minute() < entry.learned_rpm_cap
                        ):
                            slot = alt
                            break
                        # All slots at cap — wait briefly for the rolling
                        # window to advance, then re-check.
                        await asyncio.sleep(0.5)
                        slot = entry.next_slot() or slot

                # Record the request BEFORE sending so concurrent peers see
                # an updated RPM and pick a different slot.
                self.cache.record_request(domain, slot.slot_id)

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
                    # Infra failures are NOT reported to IP reputation —
                    # not the IP's fault.
                else:
                    if not is_blocked(result.status_code, result.body):
                        self.cache.record_slot_success(domain, slot.slot_id)
                        self._report_ip_outcome(
                            domain=domain, slot=slot, outcome="success",
                        )
                        # Hot-path refill check — opportunistically grow
                        # the pool toward target_pool_size if we're under.
                        self._ensure_refill_task(domain)
                        return RouteResult(
                            fetch=result,
                            routing_method="httpcloak_cookies",
                            blocked=False,
                            domain_level="cookies",
                            slot_id=slot.slot_id,
                        )
                    self.cache.record_slot_failure(
                        domain, slot.slot_id, status_code=result.status_code,
                    )
                    self._report_ip_outcome(
                        domain=domain, slot=slot, outcome="blocked",
                        status_code=result.status_code,
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

        # Step 4: ask Mimic for a solve.
        # Coalesce concurrent solves for the same domain — when a swarm of
        # spider requests all hit the block path at once, only one calls
        # /api/solve; the rest wait on the per-domain lock and reuse the
        # cookies the winner caches. Without this we hammer Mimic and trip
        # its per-domain rate limiter, then fall through to legacy and lose
        # the AAR-12 binary-correctness benefit.
        logger.info(
            "AutoRouter: %s blocked at httpcloak (status=%d), calling /api/solve",
            domain, result.status_code,
        )

        slot = None
        async with self._solve_lock(domain):
            # Re-check the cache — another concurrent caller may have already
            # populated a fresh slot while we were waiting on the lock.
            entry = self.cache.get(domain)
            if entry and entry.level == "cookies" and entry.live_slots():
                slot = entry.next_slot()
                logger.debug(
                    "AutoRouter: %s solve coalesced — reusing fresh slot from concurrent solve",
                    domain,
                )

            if slot is None:
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

                # Cache the new slot. Carry the egress_ip Mimic resolved
                # for this proxy_session_id so the spider's failure feedback
                # can name a physical IP rather than just a session token.
                slot = self.cache.set_cookies_from_solve(
                    domain=domain,
                    cookies=solve.cookies,
                    user_agent=solve.user_agent,
                    proxy_session_id=solve.proxy_session_id,
                    preset=solve.preset,
                    egress_ip=solve.egress_ip,
                )
                # Phase 5: kick off the background refill loop now that
                # we have at least one slot for this domain. Idempotent —
                # safe to call repeatedly.
                self._ensure_refill_task(domain)
        # Track the post-solve replay against the new slot's RPM window too
        self.cache.record_request(domain, slot.slot_id)
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
            self._report_ip_outcome(
                domain=domain, slot=slot, outcome="blocked",
                status_code=replay.status_code,
            )
            return RouteResult(
                fetch=replay,
                routing_method="solve_then_replay",
                blocked=True,
                domain_level="cookies",
                needs_legacy_fallback=True,
            )

        self._report_ip_outcome(
            domain=domain, slot=slot, outcome="success",
        )
        return RouteResult(
            fetch=replay,
            routing_method="solve_then_replay",
            blocked=False,
            domain_level="cookies",
            slot_id=slot.slot_id,
        )
