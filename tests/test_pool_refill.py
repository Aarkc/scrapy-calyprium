"""Phase 5: proactive cookie pool expansion.

Verifies that SpiderAutoRouter spawns a background refill task after the
first successful solve, that the loop mints fresh slots until it reaches
target_pool_size, and that it backs off when the pool is full or
oversaturated with dead slots.
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


class TestRefillLoopLifecycle:
    @pytest.mark.asyncio
    async def test_refill_starts_after_first_solve(self):
        router = _make_router(target=3, interval=0.05)
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")

        assert "example.com" in router._refill_tasks
        task = router._refill_tasks["example.com"]
        assert not task.done()

        # Let the loop run a few ticks
        await asyncio.sleep(0.2)
        # Should have minted enough slots to reach target
        live = len(router.cache.get("example.com").live_slots())
        assert live >= 3

        router.stop_refill()
        await asyncio.sleep(0.05)

    @pytest.mark.asyncio
    async def test_refill_does_not_exceed_target(self):
        router = _make_router(target=3, interval=0.05)
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")

        await asyncio.sleep(0.4)  # plenty of ticks
        live = len(router.cache.get("example.com").live_slots())
        assert live == 3, (
            f"expected exactly 3 live slots after refill saturation, got {live}"
        )

        router.stop_refill()

    @pytest.mark.asyncio
    async def test_refill_idempotent(self):
        router = _make_router(target=3, interval=0.1)
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")

        first = router._refill_tasks["example.com"]
        # Second call shouldn't replace the task
        router._ensure_refill_task("example.com")
        second = router._refill_tasks["example.com"]
        assert first is second

        router.stop_refill()

    @pytest.mark.asyncio
    async def test_stop_refill_cancels_loop(self):
        router = _make_router(target=3, interval=0.05)
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")

        await asyncio.sleep(0.05)
        router.stop_refill()
        await asyncio.sleep(0.05)
        task = router._refill_tasks["example.com"]
        assert task.done()


# ---------------------------------------------------------------------------
# Refill loop guards
# ---------------------------------------------------------------------------


class TestRefillLoopGuards:
    @pytest.mark.asyncio
    async def test_refill_skips_when_no_cookies_entry(self):
        router = _make_router(target=3, interval=0.05)
        # Manually start the refill loop without seeding any entry
        router._ensure_refill_task("example.com")
        await asyncio.sleep(0.15)
        # No entry → no solve calls
        assert router.solve_client.solve_calls == []
        router.stop_refill()

    @pytest.mark.asyncio
    async def test_refill_caps_oversaturated_dead_slots(self):
        router = _make_router(target=3, interval=0.05)
        cache = router.cache
        # Pre-populate with 6 dead slots — 2x target
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
        await asyncio.sleep(0.2)
        # Refill should bail because total slot count >= 2 * target
        assert router.solve_client.solve_calls == []
        router.stop_refill()

    @pytest.mark.asyncio
    async def test_refill_survives_solve_error(self):
        # The refill loop should keep running after a transient solve failure.
        # Setup: first solve (the hot-path one inside fetch) succeeds, second
        # solve (first refill tick) raises, third+ succeed. We expect the
        # refill loop to come back and eventually fill the pool to target.
        from scrapy_calyprium.routing.solve_client import SolveResult

        class _FlakyClient:
            def __init__(self):
                self.solve_calls = 0
                self.report_calls = []

            async def solve(self, **kwargs):
                self.solve_calls += 1
                if self.solve_calls == 2:  # only the first refill fails
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
                self.report_calls.append(kwargs)

        router = SpiderAutoRouter(
            fetcher=FakeFetcher(),
            cache=DomainCache(),
            solve_client=_FlakyClient(),
            proxy_url=None,
            target_pool_size=3,
            refill_interval=0.05,
        )
        router.fetcher.queue(_blocked(), _ok())
        await router.fetch("https://example.com/", domain="example.com")

        await asyncio.sleep(0.4)
        # 1 hot-path solve + at least 2 successful refills (after the
        # transient failure on tick 1) → 3 total slots, 4+ solve calls.
        assert router.solve_client.solve_calls >= 4
        live = len(router.cache.get("example.com").live_slots())
        assert live >= 3
        router.stop_refill()
