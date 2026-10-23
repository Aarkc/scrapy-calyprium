"""A fresh Cloudflare clearance refused on its first replay is replayed once more after a pause.

Cloudflare commonly refuses a cf_clearance's first use or two before it accepts it (cookie warm-up;
domain_cache.MAX_SLOT_FAILURES tolerates the same). Mimic replays a fresh native clearance once more
after NATIVE_REPLAY_RETRY_DELAY_S (2 s) before it gives up on it (PR #241: 9 of 11 native clearances
got "Just a moment" on a replay 1-2 s after the solve). The router's post-solve replay does the same
(SpiderAutoRouter.warmup_replay_delay_s, WARMUP_REPLAY_DELAY_S).
"""
import time

import pytest

from scrapy_calyprium.routing import diagnosis as dg
from scrapy_calyprium.routing.auto import WARMUP_REPLAY_DELAY_S, SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import LocalFetchError, LocalFetchResult
from tests.test_egress_ip_feedback import FakeSolveClient as ReportingSolveClient
from tests.test_local_routing import FakeFetcher, FakeSolveClient, _make_router, _ok, _solve_ok

URL = "https://www.example.com/parts/widget"
DOMAIN = "www.example.com"
CF = (b'<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title></head><body>'
      b"<script>(function(){window._cf_chl_opt={cvId: '3',cType: 'managed'};var cpo=document.createElement"
      b"('script');cpo.src='/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1?ray=8a';}());</script>"
      b"</body></html>")


def _cf(status=403, headers=None):
    return LocalFetchResult(url=URL, final_url=URL, status_code=status, body=CF,
                            headers=headers or {"content-type": "text/html", "server": "cloudflare"},
                            backend="httpcloak")


def _page(status, body):
    return LocalFetchResult(url=URL, final_url=URL, status_code=status, body=body,
                            headers={"content-type": "text/html"}, backend="httpcloak")


class StampedFetcher(FakeFetcher):
    """FakeFetcher that notes when each fetch was made."""

    def __init__(self):
        super().__init__()
        self.stamps = []

    async def fetch(self, url: str, **kwargs):
        self.stamps.append(time.monotonic())
        return await super().fetch(url, **kwargs)


def _replays(fetcher):
    return [c for c in fetcher.calls if c.get("cookies")]


@pytest.mark.asyncio
async def test_a_fresh_clearance_refused_once_is_replayed_after_a_pause():
    f = StampedFetcher()
    f.queue(_cf(), _cf(), _ok())  # light: the challenge; the clearance's first replay: refused; then accepted
    s = FakeSolveClient()
    s.queue(_solve_ok())
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())
    router.warmup_replay_delay_s = 0.05

    result = await router.fetch(URL, domain=DOMAIN)

    assert result.blocked is False and result.routing_method == "solve_then_replay"
    assert len(s.calls) == 1  # no second solve
    first, second = _replays(f)
    assert first["cookies"] == second["cookies"] == [{"name": "cf_clearance", "value": "x"}]
    assert (second["proxy_session_id"], second["user_agent"]) == (first["proxy_session_id"], first["user_agent"])
    assert f.stamps[2] - f.stamps[1] >= 0.04  # replayed again after the pause
    slot = router.cache.get(DOMAIN).slots[0]
    assert slot.fail_count == 1 and slot.is_live  # the warm-up refusal counts on the slot, which lives on


@pytest.mark.asyncio
async def test_the_warmup_refusal_is_not_reported_against_the_ip():
    import asyncio

    from tests.test_egress_ip_feedback import _solve_result_with_egress

    f = FakeFetcher()
    f.queue(_cf(), _cf(), _ok())
    s = ReportingSolveClient()
    s.queue(_solve_result_with_egress("203.0.113.10"))
    router = SpiderAutoRouter(fetcher=f, cache=DomainCache(), solve_client=s, target_pool_size=0,
                              warmup_replay_delay_s=0.01)

    result = await router.fetch(URL, domain=DOMAIN)
    await asyncio.sleep(0)

    assert result.blocked is False
    assert [c["outcome"] for c in s.report_calls] == ["success"]


@pytest.mark.asyncio
async def test_a_clearance_refused_after_the_pause_too_is_refused():
    f = FakeFetcher()
    f.queue(_cf(), _cf(), _cf(503, {"content-type": "text/html", "cf-mitigated": "challenge"}))
    s = FakeSolveClient()
    s.queue(_solve_ok())
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())

    result = await router.fetch(URL, domain=DOMAIN)

    assert result.blocked and result.routing_method == "solve_then_replay"
    assert (result.diagnosis.cls, result.diagnosis.evidence) == (dg.CLEARANCE_REFUSED, "replay_refused")
    assert len(_replays(f)) == 2 and len(s.calls) == 1
    assert router.cache.get(DOMAIN).slots[0].fail_count == 2


@pytest.mark.asyncio
async def test_a_rate_limit_on_the_first_replay_is_not_the_warmup():
    f = FakeFetcher()
    f.queue(_cf(), _page(429, b"<html><body>Too many requests</body></html>"))
    s = FakeSolveClient()
    s.queue(_solve_ok())
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())

    result = await router.fetch(URL, domain=DOMAIN)

    assert result.blocked and len(_replays(f)) == 1


@pytest.mark.asyncio
async def test_only_a_cloudflare_clearance_is_replayed_again():
    """Another vendor's cookie refused on its first replay is not Cloudflare's warm-up."""
    f = FakeFetcher()
    f.queue(_cf(), _cf())
    s = FakeSolveClient()
    s.queue(_solve_ok(cookies=[{"name": "datadome", "value": "x"}]))
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())

    result = await router.fetch(URL, domain=DOMAIN)

    assert result.blocked and len(_replays(f)) == 1


@pytest.mark.asyncio
async def test_without_the_pause_there_is_no_second_replay():
    f = FakeFetcher()
    f.queue(_cf(), _cf())
    s = FakeSolveClient()
    s.queue(_solve_ok())
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())
    router.warmup_replay_delay_s = 0

    result = await router.fetch(URL, domain=DOMAIN)

    assert result.blocked and len(_replays(f)) == 1


@pytest.mark.asyncio
async def test_a_slot_that_was_accepted_before_is_not_warming_up():
    cache = DomainCache()
    slot = cache.set_cookies_from_solve(DOMAIN, cookies=[{"name": "cf_clearance", "value": "x"}],
                                        user_agent="UA", proxy_session_id="sess-1")
    cache.record_slot_success(DOMAIN, slot.slot_id)
    f = FakeFetcher()
    f.queue(_cf())
    router = _make_router(fetcher=f, cache=cache)

    result = await router._replay_with_slot(URL, DOMAIN, slot, None)

    assert result.blocked and len(f.calls) == 1


@pytest.mark.asyncio
async def test_a_transport_error_on_the_second_replay():
    f = FakeFetcher()
    f.queue(_cf(), _cf(), LocalFetchError("connection reset by peer"))
    s = FakeSolveClient()
    s.queue(_solve_ok())
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())

    result = await router.fetch(URL, domain=DOMAIN)

    assert result.blocked and "connection reset" in result.error
    assert len(_replays(f)) == 2


def test_the_pause_is_two_seconds_by_default():
    router = SpiderAutoRouter(fetcher=FakeFetcher(), cache=DomainCache(), solve_client=FakeSolveClient())
    assert router.warmup_replay_delay_s == WARMUP_REPLAY_DELAY_S == 2.0
