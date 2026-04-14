"""Phase 4: end-to-end IP reputation feedback loop.

Verifies that:
- Mimic's /api/solve egress_ip is parsed into SolveResult
- DomainCache.set_cookies_from_solve threads it onto the CookieSlot
- SpiderAutoRouter.fetch carries it through the post-solve replay path
- Slot success / failure paths fire the report_ip_outcome callback with the
  resolved egress_ip when one is known
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pytest

from scrapy_calyprium.routing.auto import SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import (
    CookieSlot,
    DomainCache,
)
from scrapy_calyprium.routing.local_fetch import LocalFetchResult
from scrapy_calyprium.routing.solve_client import SolveResult


# ---------------------------------------------------------------------------
# Reused fakes
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
        self.responses: List = []

    def queue(self, *items):
        self.responses.extend(items)

    async def solve(self, **kwargs):
        self.solve_calls.append(kwargs)
        if not self.responses:
            raise AssertionError("FakeSolveClient: no queued response")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def report_ip_outcome(self, **kwargs):
        self.report_calls.append(kwargs)


_OK_BODY = (
    b"<!DOCTYPE html><html><head><title>Real Page</title></head><body>"
    + b"<nav></nav><main>"
    + b"<a href='/x'>link</a>" * 10
    + b"</main><footer></footer></body></html>"
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


def _blocked_403():
    return LocalFetchResult(
        url="https://example.com/",
        final_url="https://example.com/",
        status_code=403,
        headers={"content-type": "text/html"},
        body=b"<html><body>blocked</body></html>",
        backend="httpcloak",
    )


def _solve_result_with_egress(ip: Optional[str] = "203.0.113.10"):
    return SolveResult(
        success=True,
        cookies=[{"name": "cf_clearance", "value": "x", "domain": "example.com", "path": "/"}],
        user_agent="UA",
        proxy_session_id="sess-A",
        engine="camoufox",
        preset="chrome-latest",
        duration_ms=100,
        egress_ip=ip,
    )


def _make_router(fetcher=None, solve=None, cache=None):
    return SpiderAutoRouter(
        fetcher=fetcher or FakeFetcher(),
        cache=cache or DomainCache(),
        solve_client=solve or FakeSolveClient(),
        proxy_url=None,
    )


# ---------------------------------------------------------------------------
# CookieSlot egress_ip storage
# ---------------------------------------------------------------------------


class TestCookieSlotEgressIPField:
    def test_default_is_none(self):
        slot = CookieSlot(
            slot_id="s1", cookies=[], user_agent="UA",
            proxy_session_id="sess-A",
        )
        assert slot.egress_ip is None

    def test_to_dict_includes_egress_ip(self):
        slot = CookieSlot(
            slot_id="s1", cookies=[], user_agent="UA",
            proxy_session_id="sess-A", egress_ip="1.2.3.4",
        )
        assert slot.to_dict()["egress_ip"] == "1.2.3.4"

    def test_from_dict_round_trips_egress_ip(self):
        slot = CookieSlot(
            slot_id="s1", cookies=[], user_agent="UA",
            proxy_session_id="sess-A", egress_ip="1.2.3.4",
        )
        cloned = CookieSlot.from_dict(slot.to_dict())
        assert cloned.egress_ip == "1.2.3.4"

    def test_set_cookies_from_solve_persists_egress_ip(self):
        cache = DomainCache()
        slot = cache.set_cookies_from_solve(
            domain="example.com",
            cookies=[{"name": "c", "value": "v"}],
            user_agent="UA",
            proxy_session_id="sess-A",
            egress_ip="1.2.3.4",
        )
        assert slot.egress_ip == "1.2.3.4"
        entry = cache.get("example.com")
        assert entry.slots[0].egress_ip == "1.2.3.4"


# ---------------------------------------------------------------------------
# SolveClient parses egress_ip from /api/solve response
# ---------------------------------------------------------------------------


class TestSolveResultParsesEgressIP:
    @pytest.mark.asyncio
    async def test_egress_ip_from_response(self, monkeypatch):
        from scrapy_calyprium.routing import solve_client as sc

        class _Resp:
            status_code = 200
            text = ""
            def json(self):
                return {
                    "success": True,
                    "cookies": [{"name": "c", "value": "v"}],
                    "user_agent": "UA",
                    "proxy_session_id": "sess-A",
                    "engine": "camoufox",
                    "preset": "chrome-latest",
                    "duration_ms": 50,
                    "egress_ip": "203.0.113.7",
                }

        class _Client:
            async def post(self, *a, **kw):
                return _Resp()
            async def aclose(self):
                pass

        client = sc.SolveClient(service_url="http://stub")
        monkeypatch.setattr(client, "_get_client", lambda: _async_return(_Client()))
        result = await client.solve(domain="example.com")
        assert result.egress_ip == "203.0.113.7"

    @pytest.mark.asyncio
    async def test_egress_ip_missing_is_none(self, monkeypatch):
        from scrapy_calyprium.routing import solve_client as sc

        class _Resp:
            status_code = 200
            text = ""
            def json(self):
                return {
                    "success": True, "cookies": [],
                    "user_agent": "UA", "proxy_session_id": "sess-A",
                    "engine": "x", "preset": "p", "duration_ms": 1,
                }

        class _Client:
            async def post(self, *a, **kw):
                return _Resp()
            async def aclose(self):
                pass

        client = sc.SolveClient(service_url="http://stub")
        monkeypatch.setattr(client, "_get_client", lambda: _async_return(_Client()))
        result = await client.solve(domain="example.com")
        assert result.egress_ip is None


async def _async_return(value):
    return value


# ---------------------------------------------------------------------------
# SpiderAutoRouter wires egress_ip end-to-end and reports outcomes
# ---------------------------------------------------------------------------


class TestRouterReportsIPOutcome:
    @pytest.mark.asyncio
    async def test_solve_path_threads_egress_ip_to_slot(self):
        fetcher = FakeFetcher()
        # First request: light fetch returns blocked → triggers solve
        # Then post-solve replay returns OK
        fetcher.queue(_blocked_403(), _ok())
        solve = FakeSolveClient()
        solve.queue(_solve_result_with_egress("203.0.113.10"))
        router = _make_router(fetcher=fetcher, solve=solve)

        result = await router.fetch("https://example.com/", domain="example.com")

        assert result.blocked is False
        entry = router.cache.get("example.com")
        assert entry.slots[0].egress_ip == "203.0.113.10"

    @pytest.mark.asyncio
    async def test_post_solve_success_reports_ip_outcome(self):
        fetcher = FakeFetcher()
        fetcher.queue(_blocked_403(), _ok())
        solve = FakeSolveClient()
        solve.queue(_solve_result_with_egress("203.0.113.10"))
        router = _make_router(fetcher=fetcher, solve=solve)

        await router.fetch("https://example.com/", domain="example.com")

        # Yield to event loop so the fire-and-forget create_task runs
        import asyncio
        await asyncio.sleep(0)

        assert any(
            c["outcome"] == "success" and c["egress_ip"] == "203.0.113.10"
            for c in solve.report_calls
        ), f"expected a success report, got {solve.report_calls}"

    @pytest.mark.asyncio
    async def test_post_solve_block_reports_ip_outcome(self):
        fetcher = FakeFetcher()
        # Light fetch blocked → solve → replay also blocked
        fetcher.queue(_blocked_403(), _blocked_403())
        solve = FakeSolveClient()
        solve.queue(_solve_result_with_egress("203.0.113.10"))
        router = _make_router(fetcher=fetcher, solve=solve)

        await router.fetch("https://example.com/", domain="example.com")
        import asyncio
        await asyncio.sleep(0)

        blocked_reports = [
            c for c in solve.report_calls if c["outcome"] == "blocked"
        ]
        assert blocked_reports, f"expected a blocked report, got {solve.report_calls}"
        assert blocked_reports[0]["egress_ip"] == "203.0.113.10"
        assert blocked_reports[0]["status_code"] == 403

    @pytest.mark.asyncio
    async def test_no_egress_ip_skips_report(self):
        # When Mimic returns egress_ip=None, the spider should not report
        fetcher = FakeFetcher()
        fetcher.queue(_blocked_403(), _blocked_403())
        solve = FakeSolveClient()
        solve.queue(_solve_result_with_egress(None))
        router = _make_router(fetcher=fetcher, solve=solve)

        await router.fetch("https://example.com/", domain="example.com")
        import asyncio
        await asyncio.sleep(0)

        assert solve.report_calls == []

    @pytest.mark.asyncio
    async def test_cookie_replay_block_reports_ip_outcome(self):
        # Pre-seed a cached slot with egress_ip. First request hits the
        # cookie replay path (step 2) and gets blocked → should report
        # the block against the slot's egress_ip.
        #
        # After the cookie replay fails (slot.fail_count=1, still live),
        # the router falls through to step 3 (light), then step 4 (solve
        # lock acquire → re-check cache → live slot still exists, reuse
        # it → post-solve replay using same slot). Queue enough fetcher
        # responses to satisfy that whole path: we only care about the
        # FIRST report being "blocked" with the seeded egress_ip.
        cache = DomainCache()
        cache.set_cookies_from_solve(
            domain="example.com",
            cookies=[{"name": "c", "value": "v"}],
            user_agent="UA",
            proxy_session_id="sess-A",
            egress_ip="203.0.113.99",
        )
        fetcher = FakeFetcher()
        # With MAX_SLOT_FAILURES=1, the cookie replay 403 immediately kills
        # the slot. Router then: light fetch → blocked → solve → replay.
        # Queue responses for the full fall-through path.
        fetcher.queue(_blocked_403(), _blocked_403(), _blocked_403())
        solve = FakeSolveClient()
        # Solve must succeed so the router gets to replay; that replay's
        # 403 produces another IP report against the NEW egress IP.
        solve.queue(_solve_result_with_egress("203.0.113.99"))

        router = _make_router(fetcher=fetcher, solve=solve, cache=cache)
        await router.fetch("https://example.com/", domain="example.com")
        import asyncio
        await asyncio.sleep(0)

        cookie_block = [
            c for c in solve.report_calls
            if c["outcome"] == "blocked" and c["egress_ip"] == "203.0.113.99"
        ]
        assert cookie_block, (
            f"expected cookie-replay block report, got {solve.report_calls}"
        )
