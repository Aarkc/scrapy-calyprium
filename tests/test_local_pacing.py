"""Responses the middleware returns itself keep the spider's per-domain pacing.

Scrapy applies DOWNLOAD_DELAY, AutoThrottle and CONCURRENT_REQUESTS_PER_DOMAIN in its downloader's slot,
which a request reaches only after the downloader middlewares passed it on: the local route's responses
skipped all of them, so a spider declaring a 3 s delay sent its requests back to back.
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest
from scrapy import Spider, signals
from scrapy.core.downloader import Slot
from scrapy.http import Request
from scrapy.settings import Settings

from scrapy_calyprium.routing.auto import RouteResult
from scrapy_calyprium.routing.local_fetch import LocalFetchResult

from tests.test_middleware_local_integration import _make_middleware

SPIDER = Spider(name="t")


class FakeDownloader:
    """Scrapy's Downloader as the pacer uses it: one slot per domain, made on first use."""

    def __init__(self, concurrency=1, delay=0.0):
        self.slots = {}
        self.concurrency, self.delay = concurrency, delay

    def _get_slot(self, request, spider):
        key = request.meta.get("download_slot") or request.url.split("/")[2]
        if key not in self.slots:
            self.slots[key] = Slot(self.concurrency, self.delay, False)
        return key, self.slots[key]


class SlowRouter:
    """Records when each fetch started and ended; each takes ``seconds``."""

    def __init__(self, seconds=0.0, elapsed_ms=120):
        self.seconds, self.elapsed_ms = seconds, elapsed_ms
        self.started, self.ended = [], []

    async def fetch(self, url, *, domain, headers=None, **kwargs):
        self.started.append(time.monotonic())
        await asyncio.sleep(self.seconds)
        self.ended.append(time.monotonic())
        return RouteResult(
            fetch=LocalFetchResult(url=url, final_url=url, status_code=200,
                                   headers={"content-type": "text/html"},
                                   body=b"<html><body><main>" + b"<p>x</p>" * 400 + b"</main></body></html>",
                                   elapsed_ms=self.elapsed_ms, backend="httpcloak"),
            routing_method="httpcloak_light", blocked=False, domain_level="light",
        )


def _crawl(router, downloader, settings=None):
    sent = []
    mw = _make_middleware(local_router=router)
    mw.crawler = SimpleNamespace(
        settings=Settings(settings or {}),
        engine=SimpleNamespace(downloader=downloader),
        signals=SimpleNamespace(send_catch_log=lambda **kw: sent.append(kw)),
    )
    return mw, sent


@pytest.mark.asyncio
async def test_download_delay_spaces_the_local_fetches():
    router = SlowRouter()
    mw, _ = _crawl(router, FakeDownloader(concurrency=8, delay=0.3))
    for i in range(3):
        assert await mw._try_local_route(Request(f"https://a.example/{i}"), SPIDER) is not None
    gaps = [b - a for a, b in zip(router.started, router.started[1:])]
    assert all(g >= 0.28 for g in gaps), gaps


@pytest.mark.asyncio
async def test_per_domain_concurrency_holds_concurrent_requests():
    router = SlowRouter(seconds=0.2)
    mw, _ = _crawl(router, FakeDownloader(concurrency=1))
    await asyncio.gather(*(mw._try_local_route(Request(f"https://a.example/{i}"), SPIDER) for i in range(3)))
    # one at a time: each fetch starts after the previous one ended
    for prev_end, start in zip(router.ended, router.started[1:]):
        assert start >= prev_end - 0.005


@pytest.mark.asyncio
async def test_other_domains_are_not_held_by_a_slow_one():
    router = SlowRouter(seconds=0.2)
    mw, _ = _crawl(router, FakeDownloader(concurrency=1))
    t0 = time.monotonic()
    await asyncio.gather(mw._try_local_route(Request("https://a.example/"), SPIDER),
                         mw._try_local_route(Request("https://b.example/"), SPIDER))
    assert time.monotonic() - t0 < 0.35  # side by side, not 0.4 s in a row


@pytest.mark.asyncio
async def test_pacing_off_sends_back_to_back():
    router = SlowRouter()
    mw, sent = _crawl(router, FakeDownloader(delay=0.5), {"MIMIC_PACING": False})
    t0 = time.monotonic()
    for i in range(3):
        await mw._try_local_route(Request(f"https://a.example/{i}"), SPIDER)
    assert time.monotonic() - t0 < 0.3
    assert sent == []


@pytest.mark.asyncio
async def test_autothrottle_hears_the_sites_latency():
    router = SlowRouter(elapsed_ms=850)
    mw, sent = _crawl(router, FakeDownloader())
    request = Request("https://a.example/")
    response = await mw._try_local_route(request, SPIDER)
    assert request.meta["download_slot"] == "a.example"
    assert request.meta["download_latency"] == pytest.approx(0.85)
    assert len(sent) == 1
    assert sent[0]["signal"] is signals.response_downloaded
    assert sent[0]["response"] is response and sent[0]["request"] is request


@pytest.mark.asyncio
async def test_without_a_downloader_nothing_waits():
    router = SlowRouter()
    mw = _make_middleware(local_router=router)  # no crawler at all
    t0 = time.monotonic()
    for i in range(3):
        assert await mw._try_local_route(Request(f"https://a.example/{i}"), SPIDER) is not None
    assert time.monotonic() - t0 < 0.3
