"""AAR-17: tests for the spider-side auto-routing orchestrator.

These tests use a fake LocalFetcher and a fake SolveClient so we can drive
each branch of the routing flow deterministically without needing a network
or a real Mimic instance.
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional

import pytest

from scrapy_calyprium.routing.auto import RouteResult, SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import (
    DomainCache,
    HEAVY_REPROBE_INITIAL_SECONDS,
    PROMOTION_COOLDOWN_SECONDS,
    MAX_SLOT_FAILURES,
    MIN_DOMAIN_FAILURES_FOR_PROMOTION,
)
from scrapy_calyprium.routing.local_fetch import LocalFetchError, LocalFetchResult
from scrapy_calyprium.routing.solve_client import SolveError, SolveResult


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeFetcher:
    """Programmable LocalFetcher stand-in."""

    def __init__(self):
        self.calls: List[Dict] = []
        self.responses: List = []  # list of LocalFetchResult or Exception

    def queue(self, *items):
        self.responses.extend(items)

    async def fetch(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"FakeFetcher: no queued response for {url}")
        next_item = self.responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item


class FakeSolveClient:
    def __init__(self):
        self.calls: List[Dict] = []
        self.responses: List = []

    def queue(self, *items):
        self.responses.extend(items)

    async def solve(self, **kwargs):
        self.calls.append(kwargs)
        if not self.responses:
            raise AssertionError("FakeSolveClient: no queued response")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


_OK_BODY = (
    b"<!DOCTYPE html><html><head><title>Real Page</title></head><body>"
    + b"<nav></nav><main>"
    + b"<a href='/x'>link</a>" * 10
    + b"</main><footer></footer></body></html>"
)


def _ok(status=200, body=_OK_BODY):
    return LocalFetchResult(
        url="https://example.com/",
        final_url="https://example.com/",
        status_code=status,
        headers={"content-type": "text/html"},
        body=body,
        backend="httpcloak",
    )


def _blocked_403():
    return LocalFetchResult(
        url="https://example.com/",
        final_url="https://example.com/",
        status_code=403,
        headers={"content-type": "text/html"},
        body=b"<html><body>blocked</body></html>",
        backend="httpcloak",
    )


def _real_pdf():
    pdf = b"%PDF-1.7\n%\xb5\xed\xae\xfb\n" + bytes(range(256)) * 50
    return LocalFetchResult(
        url="https://example.com/file.pdf",
        final_url="https://example.com/file.pdf",
        status_code=200,
        headers={"content-type": "application/pdf"},
        body=pdf,
        backend="httpcloak",
    )


def _make_router(
    fetcher: Optional[FakeFetcher] = None,
    solve: Optional[FakeSolveClient] = None,
    cache: Optional[DomainCache] = None,
    proxy_url: Optional[str] = None,
):
    return SpiderAutoRouter(
        fetcher=fetcher or FakeFetcher(),
        cache=cache or DomainCache(),
        solve_client=solve or FakeSolveClient(),
        proxy_url=proxy_url,
    )


# ---------------------------------------------------------------------------
# Step 1: light fast path
# ---------------------------------------------------------------------------


class TestLightFastPath:
    @pytest.mark.asyncio
    async def test_light_success_caches_and_returns(self):
        f = FakeFetcher()
        f.queue(_ok())
        cache = DomainCache()
        router = _make_router(fetcher=f, cache=cache)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.blocked is False
        assert result.routing_method == "httpcloak_light"
        assert cache.get_level("example.com") == "light"

    @pytest.mark.asyncio
    async def test_binary_pdf_passes_through_intact(self):
        f = FakeFetcher()
        f.queue(_real_pdf())
        router = _make_router(fetcher=f)

        result = await router.fetch(
            "https://example.com/file.pdf", domain="example.com",
        )

        assert result.blocked is False
        assert result.fetch.body.startswith(b"%PDF-1.7")
        # No corruption — the AAR-12 fix point
        assert b"\xef\xbf\xbd" not in result.fetch.body


# ---------------------------------------------------------------------------
# Step 4: solve then replay
# ---------------------------------------------------------------------------


class TestSolvePath:
    @pytest.mark.asyncio
    async def test_blocked_then_solve_then_replay_succeeds(self):
        f = FakeFetcher()
        f.queue(_blocked_403())  # initial light httpcloak blocked
        f.queue(_ok())            # post-solve replay succeeds

        s = FakeSolveClient()
        s.queue(SolveResult(
            success=True,
            cookies=[{"name": "cf_clearance", "value": "x"}],
            user_agent="Mozilla/5.0 Test",
            proxy_session_id="sess-1",
            engine="camoufox",
            preset="chrome-latest",
            duration_ms=12000,
        ))

        cache = DomainCache()
        router = _make_router(fetcher=f, solve=s, cache=cache)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.blocked is False
        assert result.routing_method == "solve_then_replay"
        assert cache.get_level("example.com") == "cookies"
        assert len(s.calls) == 1
        assert f.calls[1]["cookies"] == [{"name": "cf_clearance", "value": "x"}]
        assert f.calls[1]["user_agent"] == "Mozilla/5.0 Test"
        assert f.calls[1]["proxy_session_id"] == "sess-1"

    @pytest.mark.asyncio
    async def test_solve_returns_no_cookies_falls_through(self):
        f = FakeFetcher()
        f.queue(_blocked_403())
        s = FakeSolveClient()
        s.queue(SolveResult(
            success=False,
            cookies=[],
            user_agent="",
            proxy_session_id="",
            engine="",
            preset="",
            duration_ms=10000,
            error="real domain block",
        ))
        cache = DomainCache()
        router = _make_router(fetcher=f, solve=s, cache=cache)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.blocked is True
        assert result.needs_legacy_fallback is True

    @pytest.mark.asyncio
    async def test_solve_transport_error_falls_through(self):
        f = FakeFetcher()
        f.queue(_blocked_403())
        s = FakeSolveClient()
        s.queue(SolveError("mimic unreachable"))
        router = _make_router(fetcher=f, solve=s)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.blocked is True
        assert result.needs_legacy_fallback is True
        assert "mimic unreachable" in (result.error or "")


# ---------------------------------------------------------------------------
# Step 2: cookie replay path
# ---------------------------------------------------------------------------


class TestCookieReplayPath:
    @pytest.mark.asyncio
    async def test_existing_slot_replays_first(self):
        cache = DomainCache()
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "cf_clearance", "value": "abc"}],
            user_agent="UA-1",
            proxy_session_id="sess-1",
        )

        f = FakeFetcher()
        f.queue(_ok())
        s = FakeSolveClient()
        router = _make_router(fetcher=f, solve=s, cache=cache)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.routing_method == "httpcloak_cookies"
        assert result.blocked is False
        assert s.calls == []
        assert f.calls[0]["cookies"] == [{"name": "cf_clearance", "value": "abc"}]

    @pytest.mark.asyncio
    async def test_failed_replay_skips_light_goes_to_solve(self):
        # When cookies exist but replay fails, the router skips the light
        # probe (domain is already known as "cookies" level) and goes
        # straight to solve. This avoids poisoning the proxy IP with a
        # failed httpcloak probe before the browser solve.
        cache = DomainCache()
        slot = cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "x", "value": "y"}],
            user_agent="UA",
            proxy_session_id="sess",
        )

        f = FakeFetcher()
        f.queue(_blocked_403())  # cookie replay fails
        # NO light probe queued — skipped because domain is "cookies" level
        s = FakeSolveClient()
        s.queue(SolveResult(
            success=True,
            cookies=[{"name": "new", "value": "v"}],
            user_agent="UA-new",
            proxy_session_id="sess-new",
            engine="camoufox",
            preset="chrome-latest",
            duration_ms=10000,
        ))
        f.queue(_ok())  # post-solve replay
        router = _make_router(fetcher=f, solve=s, cache=cache)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.blocked is False
        assert result.routing_method == "solve_then_replay"
        assert slot.fail_count == 1


# ---------------------------------------------------------------------------
# Step 1: heavy domain fall-through
# ---------------------------------------------------------------------------


class TestHeavyFallThrough:
    @pytest.mark.asyncio
    async def test_heavy_domain_signals_legacy_fallback(self):
        from scrapy_calyprium.routing.domain_cache import DomainEntry, TTL_HEAVY

        cache = DomainCache()
        cache._entries["example.com"] = DomainEntry(
            level="heavy", ttl=float(TTL_HEAVY),
        )
        cache._entries["example.com"]._next_reprobe_at = time.time() + 9999

        router = _make_router(cache=cache)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.needs_legacy_fallback is True
        assert result.blocked is True
        assert result.domain_level == "heavy"
        assert result.routing_method == "fallback_legacy"

    @pytest.mark.asyncio
    async def test_heavy_due_for_reprobe_attempts_httpcloak(self):
        from scrapy_calyprium.routing.domain_cache import DomainEntry, TTL_HEAVY

        cache = DomainCache()
        cache._entries["example.com"] = DomainEntry(
            level="heavy", ttl=float(TTL_HEAVY),
        )
        cache._entries["example.com"]._next_reprobe_at = 0

        f = FakeFetcher()
        f.queue(_ok())
        router = _make_router(fetcher=f, cache=cache)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.blocked is False
        assert result.domain_level == "light"
