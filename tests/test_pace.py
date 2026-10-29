"""CALYPRIUM_PACE_PPM: a spider-wide pages-per-minute pace (a token bucket), off unless set."""
from __future__ import annotations

from unittest import mock

import pytest
from scrapy.exceptions import NotConfigured
from scrapy.http import Request

from scrapy_calyprium import _trace
from scrapy_calyprium._config import CalypriumConfig
from scrapy_calyprium.middleware.portfolio import PaceMiddleware, TokenBucket
from scrapy_calyprium.middleware.tracing import FIRST_ATTEMPT, CalypriumTraceMiddleware
from tests._helpers import make_crawler


class Clock:
    def __init__(self, now=100.0):
        self.now = now

    def __call__(self):
        return self.now


# -- the bucket ----------------------------------------------------------------------------------------------


def test_one_token_a_second_queues_takers_in_order():
    clock = Clock()
    bucket = TokenBucket(1.0, burst=1, clock=clock)
    assert [bucket.take() for _ in range(4)] == [0.0, 1.0, 2.0, 3.0]
    clock.now += 2.5  # two and a half tokens came, all owed already
    assert bucket.take() == pytest.approx(1.5)


def test_a_burst_goes_at_once_then_the_rate_holds():
    clock = Clock()
    bucket = TokenBucket(0.5, burst=3, clock=clock)  # 30 pages/min
    assert [bucket.take() for _ in range(5)] == [0.0, 0.0, 0.0, 2.0, 4.0]


def test_idle_time_saves_up_no_more_than_the_burst():
    clock = Clock()
    bucket = TokenBucket(1.0, burst=2, clock=clock)
    bucket.take()
    clock.now += 3600
    assert [bucket.take() for _ in range(3)] == [0.0, 0.0, 1.0]


def test_a_token_taken_on_time_never_waits():
    clock = Clock()
    bucket = TokenBucket(2.0, clock=clock)  # 120 pages/min
    waits = []
    for _ in range(10):
        waits.append(bucket.take())
        clock.now += 0.5
    assert waits == [0.0] * 10


def test_rate_must_be_positive():
    with pytest.raises(ValueError):
        TokenBucket(0)


# -- the middleware ------------------------------------------------------------------------------------------


@pytest.mark.parametrize("settings", [{}, {"CALYPRIUM_PACE_PPM": 0}, {"CALYPRIUM_PACE_PPM": ""},
                                      {"CALYPRIUM_PACE_PPM": None}, {"CALYPRIUM_PACE_PPM": "off"},
                                      {"CALYPRIUM_PACE_PPM": -5}, {"CALYPRIUM_PACE_PPM": float("inf")}])
def test_pace_is_off_unless_set(settings):
    with pytest.raises(NotConfigured):
        PaceMiddleware.from_crawler(make_crawler(settings))


def _pace(ppm=6, burst=None, clock=None):
    settings = {"CALYPRIUM_PACE_PPM": ppm}
    if burst is not None:
        settings["CALYPRIUM_PACE_BURST"] = burst
    mw = PaceMiddleware.from_crawler(make_crawler(settings))
    mw.bucket.clock = clock or Clock()
    mw.bucket._at = mw.bucket.clock()
    mw._sleep = mock.AsyncMock()
    return mw


@pytest.mark.asyncio
async def test_requests_wait_their_turn_spider_wide():
    mw = _pace(ppm=6)  # one every 10 s, every domain together
    for url in ("https://a.example/1", "https://b.example/1", "https://a.example/2"):
        assert await mw.process_request(Request(url), None) is None
    assert [c.args[0] for c in mw._sleep.await_args_list] == [pytest.approx(10.0), pytest.approx(20.0)]
    stats = mw.stats
    assert stats.get_value("calyprium/pace/delayed") == 2
    assert stats.get_value("calyprium/pace/wait_seconds") == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_the_burst_setting():
    mw = _pace(ppm=60, burst=3)
    for _ in range(4):
        await mw.process_request(Request("https://a.example/"), None)
    assert [c.args[0] for c in mw._sleep.await_args_list] == [pytest.approx(1.0)]
    assert mw.bucket.burst == 3


@pytest.mark.asyncio
async def test_the_sdks_own_calls_are_not_paced():
    mw = _pace(ppm=1)
    await mw.process_request(Request("https://a.example/"), None)
    for _ in range(3):
        await mw.process_request(Request("https://prism.example/api", meta={"_internal": True}), None)
    mw._sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_attempt_starts_when_the_pace_lets_it_go():
    mw = _pace(ppm=6)
    tracing = CalypriumTraceMiddleware()
    first, second = Request("https://a.example/1"), Request("https://a.example/2")
    for req in (first, second):
        tracing.process_request(req, None)
    began = second.meta[_trace.ATTEMPT_KEY]["start"]
    with mock.patch("scrapy_calyprium.middleware.portfolio.time.time", return_value=began + 10):
        await mw.process_request(first, None)  # no wait: untouched
        await mw.process_request(second, None)
    assert first.meta[_trace.ATTEMPT_KEY]["start"] < began + 10
    assert second.meta[_trace.ATTEMPT_KEY]["start"] == began + 10
    assert second.meta[FIRST_ATTEMPT] == began + 10  # the pace's wait is the request's queue time


# -- configure() -----------------------------------------------------------------------------------------------


def test_configure_installs_both_around_the_stack():
    mws = CalypriumConfig(api_key="clp_k").resolve().to_scrapy_settings()["DOWNLOADER_MIDDLEWARES"]
    assert mws["scrapy_calyprium.middleware.portfolio.PaceMiddleware"] == 120
    assert mws["scrapy_calyprium.middleware.portfolio.PoolBusyMiddleware"] == 950
    assert (mws["scrapy_calyprium.middleware.veil.VeilProxyMiddleware"]
            < mws["scrapy_calyprium.middleware.portfolio.PaceMiddleware"]
            < mws["scrapy_calyprium.middleware.mimic.MimicBrowserMiddleware"])
