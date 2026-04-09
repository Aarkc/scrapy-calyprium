"""Phase 5: proactive cookie pool expansion.

Verifies that SpiderAutoRouter opportunistically refills the cookie pool
on the fetch hot path. The original design used a long-lived background
asyncio task, but that doesn't survive Scrapy's Twisted/asyncio bridge
(detached task's `await asyncio.sleep` never wakes). The current design
checks the pool size on each successful fetch, throttled to one check
every refill_interval seconds per domain, and fires a fire-and-forget
solve if below target.
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, Optional
from uuid import uuid4

import pytest

from scrapy_calyprium.routing.auto import SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import LocalFetchResult
from scrapy_calyprium.routing.solve_client import SolveError, SolveResult


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeFetcher:
    def __init__(self):
        self.calls: List[Dict] = []
        self.responses: List = []

    def queue(self, *items):
        self.responses.extend(items)

    async def fetch(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"FakeFetcher: no queued response for {url}")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FakeSolveClient:
    def __init__(self):
        self.solve_calls: List[Dict] = []
        self.report_calls: List[Dict] = []
        self._next_session = 0

    async def solve(self, **kwargs):
        self.solve_calls.append(kwargs)
        # Each solve mints a unique session+ip so the cache adds a new slot
        self._next_session += 1
        return SolveResult(
            success=True,
            cookies=[{"name": "c", "value": str(self._next_session)}],
            user_agent="UA",
            proxy_session_id=f"sess-{self._next_session}",
            engine="camoufox",
            preset="chrome-latest",
            duration_ms=10,
            egress_ip=f"203.0.113.{self._next_session}",
        )

    async def report_ip_outcome(self, **kwargs):
        self.report_calls.append(kwargs)


_OK_BODY = (
    b"<!DOCTYPE html><html><head><title>OK</title></head><body>"
    + b"<a href='/x'>l</a>" * 10 + b"</body></html>"
)


def _ok():
    return LocalFetchResult(
        url="https://example.com/",
        final_url="https://example.com/",
        status_code=200,
        headers={"content-type": "text/html"},
        body=_OK_BODY,
        backend="httpcloak",
    )


def _blocked():
    return LocalFetchResult(
        url="https://example.com/",
        final_url="https://example.com/",
        status_code=403,
        headers={"content-type": "text/html"},
        body=b"<html><body>blocked</body></html>",
        backend="httpcloak",
    )


def _make_router(target=4, interval=0.05):
    return SpiderAutoRouter(
        fetcher=FakeFetcher(),
        cache=DomainCache(),
        solve_client=FakeSolveClient(),
        proxy_url=None,
        target_pool_size=target,
        refill_interval=interval,
    )


# ---------------------------------------------------------------------------
# Refill loop lifecycle
# ---------------------------------------------------------------------------


class TestHotPathRefill:
    @pytest.mark.asyncio
    async def test_refill_fires_on_post_solve_path(self):
        # First fetch hits the solve path, which calls _ensure_refill_task
        # → spawns one refill solve in background → one extra slot.
        router = _make_router(target=3, interval=0.0)
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")

        # Yield several times to let the fire-and-forget refill complete
        for _ in range(5):
            await asyncio.sleep(0)

        # 1 slot from solve + 1 from refill = 2; not yet at target
        live = len(router.cache.get("example.com").live_slots())
        assert live >= 2

    @pytest.mark.asyncio
    async def test_refill_saturates_after_multiple_fetches(self):
        # Each successful cookie-replay fetch checks refill once. With
        # interval=0 every call refills (until pool == target).
        router = _make_router(target=3, interval=0.0)
        # First fetch: solve path (blocked → solve → ok), pool grows to 1+1
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")
        for _ in range(5):
            await asyncio.sleep(0)

        # Subsequent cookie-replay fetches each fire one more refill
        for _ in range(4):
            router.fetcher.queue(_ok())
            await router.fetch(
                "https://example.com/", domain="example.com",
            )
            for _ in range(5):
                await asyncio.sleep(0)

        live = len(router.cache.get("example.com").live_slots())
        assert live == 3, f"expected exactly 3 live slots, got {live}"

    @pytest.mark.asyncio
    async def test_refill_throttled_by_interval(self):
        # With a non-zero interval, two fetches in quick succession should
        # only fire one refill — the second hits the time gate.
        router = _make_router(target=5, interval=10.0)
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")
        for _ in range(5):
            await asyncio.sleep(0)

        first_count = router.solve_client._next_session

        router.fetcher.queue(_ok())
        await router.fetch("https://example.com/", domain="example.com")
        for _ in range(5):
            await asyncio.sleep(0)

        # No new solve because second fetch hit the interval gate
        assert router.solve_client._next_session == first_count

    @pytest.mark.asyncio
    async def test_in_flight_flag_prevents_burst(self):
        # Two _ensure_refill_task calls in the same tick should only fire
        # one refill solve, not two — the in_flight flag deduplicates.
        router = _make_router(target=8, interval=0.0)
        # Seed a single slot manually so refill has something to grow from
        from scrapy_calyprium.routing.domain_cache import (
            CookieSlot, DomainEntry, TTL_COOKIES,
        )
        entry = DomainEntry(level="cookies", ttl=float(TTL_COOKIES))
        entry.slots.append(CookieSlot(
            slot_id="seed", cookies=[], user_agent="UA",
            proxy_session_id="seed-sess", egress_ip="10.0.0.1",
        ))
        router.cache._entries["example.com"] = entry

        router._ensure_refill_task("example.com")
        router._ensure_refill_task("example.com")
        router._ensure_refill_task("example.com")

        for _ in range(5):
            await asyncio.sleep(0)

        # Only one fire-and-forget solve from the burst
        assert router.solve_client._next_session == 1


# ---------------------------------------------------------------------------
# Refill loop guards
# ---------------------------------------------------------------------------


class TestRefillGuards:
    @pytest.mark.asyncio
    async def test_refill_skips_when_no_cookies_entry(self):
        router = _make_router(target=3, interval=0.0)
        # No entry seeded, no fetch — call ensure directly
        router._ensure_refill_task("example.com")
        for _ in range(3):
            await asyncio.sleep(0)
        assert router.solve_client._next_session == 0

    @pytest.mark.asyncio
    async def test_refill_caps_oversaturated_dead_slots(self):
        router = _make_router(target=3, interval=0.0)
        cache = router.cache
        # 6 dead slots → 2x target → refill should bail
        from scrapy_calyprium.routing.domain_cache import (
            CookieSlot, DomainEntry, TTL_COOKIES,
        )
        entry = DomainEntry(level="cookies", ttl=float(TTL_COOKIES))
        for i in range(6):
            slot = CookieSlot(
                slot_id=f"s{i}", cookies=[], user_agent="UA",
                proxy_session_id=f"dead-{i}",
            )
            slot.fail_count = 99  # mark dead
            entry.slots.append(slot)
        cache._entries["example.com"] = entry

        router._ensure_refill_task("example.com")
        for _ in range(5):
            await asyncio.sleep(0)
        assert router.solve_client._next_session == 0

    @pytest.mark.asyncio
    async def test_refill_survives_solve_error(self):
        # A SolveError in the refill helper must not raise into the caller
        # and must clear the in_flight flag so subsequent refills can fire.
        from scrapy_calyprium.routing.solve_client import SolveResult

        class _FlakyClient:
            def __init__(self):
                self.solve_calls = 0

            async def solve(self, **kwargs):
                self.solve_calls += 1
                # First call (hot-path inside fetch): succeeds
                # Second call (first refill): raises
                # Third+ : succeed
                if self.solve_calls == 2:
                    raise SolveError("transient")
                return SolveResult(
                    success=True,
                    cookies=[{"name": "c", "value": "v"}],
                    user_agent="UA",
                    proxy_session_id=f"sess-{self.solve_calls}",
                    engine="camoufox", preset="chrome-latest", duration_ms=1,
                    egress_ip=f"203.0.113.{self.solve_calls}",
                )

            async def report_ip_outcome(self, **kwargs):
                pass

        router = SpiderAutoRouter(
            fetcher=FakeFetcher(),
            cache=DomainCache(),
            solve_client=_FlakyClient(),
            proxy_url=None,
            target_pool_size=3,
            refill_interval=0.0,
        )
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")
        for _ in range(5):
            await asyncio.sleep(0)
        # in_flight flag should be cleared after the SolveError
        assert router._refill_in_flight.get("example.com") is False
        # And a follow-up fetch should be able to fire another refill
        router.fetcher.queue(_ok())
        await router.fetch("https://example.com/", domain="example.com")
        for _ in range(5):
            await asyncio.sleep(0)
        # By now: hot solve (1) + failed refill (2) + at least one
        # follow-up refill (3+) — third call returned success
        assert router.solve_client.solve_calls >= 3
        live = len(router.cache.get("example.com").live_slots())
        assert live >= 2
