"""The proxy gateway's pool_busy refusal: recognised on every path, requeued without spending a retry, never a
site failure, a block or a failed request.

The gateway answers a CONNECT it cannot give a connection with ``HTTP/1.1 503 Service Unavailable (pool_busy)``
and ``X-Calyprium-Proxy-Error: pool_busy``; a plain-HTTP request through it gets that 503 as its response.
"""
from __future__ import annotations

import asyncio
import random
from types import SimpleNamespace
from unittest import mock

import pytest
from scrapy.core.downloader.handlers.http11 import TunnelError, TunnelingTCP4ClientEndpoint
from scrapy.http import HtmlResponse, Request, Response

from scrapy_calyprium import _pool_busy
from scrapy_calyprium.extensions.run_stats import CalypriumRunStats
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.portfolio import (
    AIMD_DECREASE_INTERVAL_S,
    AIMD_INCREASE_AFTER,
    PoolBusyError,
    PoolBusyMiddleware,
)
from scrapy_calyprium.routing.auto import RouteResult
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import LocalFetchError, LocalFetchResult
from tests._helpers import make_crawler
from tests.test_egress_ip_feedback import FakeSolveClient as ReportingSolveClient
from tests.test_local_routing import FakeFetcher, FakeSolveClient, _blocked_403, _make_router, _ok, _solve_ok
from tests.test_middleware_local_integration import FakeRouter, _make_middleware

URL = "https://shop.example/p/1"
BUSY_CONNECT = (b"HTTP/1.1 503 Service Unavailable (pool_busy)\r\n"
                b"X-Calyprium-Proxy-Error: pool_busy\r\nContent-Length: 0\r\n\r\n")
# What the local fetcher raises for a refused CONNECT (routing/local_fetch.py; measured against a fake gateway).
HTTPCLOAK_BUSY = ("httpcloak fetch failed for https://shop.example/p/1: dial_proxy shop.example: "
                  "proxy CONNECT failed: 503 Service Unavailable (pool_busy)")
CURL_BUSY = ("curl_cffi fetch failed for https://shop.example/p/1: Failed to perform, curl: (7) CONNECT tunnel "
             "failed, response 503. See https://curl.se/libcurl/c/libcurl-errors.html first for more details. "
             "(X-Calyprium-Proxy-Error: pool_busy; Service Unavailable (pool_busy))")
BUSY_HEADERS = {"X-Calyprium-Proxy-Error": "pool_busy", "Content-Type": "text/plain"}
PAGE = (b"<html><head><title>Widget | Shop</title></head><body><nav></nav><main>"
        + b"<a href='/p'>product</a>" * 5 + b"</main></body></html>")


def _tunnel_error(reply: bytes) -> BaseException:
    """The error Scrapy 2.12's own CONNECT code raises for a proxy's reply (http11.processProxyResponse)."""
    endpoint = TunnelingTCP4ClientEndpoint(mock.Mock(), "shop.example", 443, ("gateway", 8080, b"Basic x"),
                                           mock.Mock())
    endpoint._protocol = SimpleNamespace(transport=mock.Mock(), dataReceived=None)
    endpoint._protocolDataReceived = lambda data: None
    failures = []
    endpoint._tunnelReadyDeferred.addErrback(failures.append)
    endpoint.processProxyResponse(reply)
    return failures[0].value


def _busy_fetch(status=503):
    return LocalFetchResult(url=URL, final_url=URL, status_code=status,
                            headers={"x-calyprium-proxy-error": "pool_busy", "content-type": "text/plain"},
                            body=b"pool_busy", backend="curl_cffi")


class FakeDownloader:
    def __init__(self, total_concurrency=16):
        self.total_concurrency = total_concurrency


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


def _mw(settings=None, downloader=None, clock=None, **kwargs):
    crawler = make_crawler({"CONCURRENT_REQUESTS": 16, **(settings or {})})
    crawler.engine = SimpleNamespace(downloader=downloader or FakeDownloader())
    mw = PoolBusyMiddleware.from_crawler(crawler)
    mw._sleep = mock.AsyncMock()
    mw._clock = clock or Clock()
    mw._random = random.Random(7)
    for key, value in kwargs.items():
        setattr(mw, key, value)
    return mw


def _stat(mw, key):
    return mw.crawler.stats.get_value(key)


# -- recognising it ------------------------------------------------------------------------------------------


def test_scrapys_tunnel_error_names_pool_busy():
    error = _tunnel_error(BUSY_CONNECT)
    assert isinstance(error, TunnelError)
    assert "(pool_busy)" in str(error)  # [{'status': 503, 'reason': b'Service Unavailable (pool_busy)'}]
    assert _pool_busy.is_pool_busy_error(error)


def test_another_tunnel_refusal_is_not_pool_busy():
    error = _tunnel_error(b"HTTP/1.1 502 Bad Gateway\r\nX-Calyprium-Proxy-Error: upstream_failed\r\n\r\n")
    assert isinstance(error, TunnelError) and not _pool_busy.is_pool_busy_error(error)
    assert not _pool_busy.is_pool_busy_error(_tunnel_error(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n"))


@pytest.mark.parametrize("error", [
    LocalFetchError(HTTPCLOAK_BUSY, proxy_error="pool_busy"),
    LocalFetchError(HTTPCLOAK_BUSY),  # the text alone
    LocalFetchError(CURL_BUSY, proxy_error="pool_busy"),
    LocalFetchError("curl_cffi fetch failed: curl: (7) CONNECT tunnel failed, response 503", proxy_error="pool_busy"),
    PoolBusyError("whatever"),
    RuntimeError("upstream said pool_busy"),
])
def test_local_fetch_and_other_errors_naming_pool_busy(error):
    assert _pool_busy.is_pool_busy_error(error)


def test_an_explicit_cause_counts():
    try:
        try:
            raise LocalFetchError(HTTPCLOAK_BUSY)
        except LocalFetchError as exc:
            raise RuntimeError("route failed") from exc
    except RuntimeError as outer:
        assert _pool_busy.is_pool_busy_error(outer)


@pytest.mark.parametrize("error", [
    LocalFetchError("httpcloak fetch failed for https://shop.example/pool_busy?q=pool_busy: timed out"),
    LocalFetchError("curl_cffi fetch failed: curl: (7) CONNECT tunnel failed, response 503"),
    TimeoutError("timed out"),
    ConnectionRefusedError("refused"),
    None,
])
def test_errors_that_are_not_pool_busy(error):
    assert not _pool_busy.is_pool_busy_error(error)


def test_the_header_on_a_response():
    req = Request("http://shop.example/p")
    assert _pool_busy.is_pool_busy_response(Response(req.url, status=503, headers=BUSY_HEADERS, request=req))
    assert _pool_busy.is_pool_busy_headers({"x-calyprium-proxy-error": "POOL_BUSY"})
    assert _pool_busy.is_pool_busy_headers({b"X-Calyprium-Proxy-Error": [b"pool_busy"]})
    assert not _pool_busy.is_pool_busy_response(Response(req.url, status=503, request=req))
    assert not _pool_busy.is_pool_busy_headers({"X-Calyprium-Proxy-Error": "refused_domain"})
    assert not _pool_busy.is_pool_busy_headers(None)


def test_pool_busy_error_is_a_transport_error_named_by_the_classifier():
    from scrapy_calyprium import _outcome
    error = PoolBusyError("refused")
    assert isinstance(error, ConnectionError)  # without PoolBusyMiddleware: a transport error's handling
    assert _outcome.of_error(error)["evidence"] == "pool_busy"


# -- requeue ---------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_tunnel_refusal_is_requeued_without_spending_a_retry():
    mw = _mw()
    req = Request(URL, meta={"retry_times": 1, "calyprium_attempt_number": 1, "mimic_transport_retries": 1},
                  priority=3)
    out = await mw.process_exception(req, _tunnel_error(BUSY_CONNECT), None)
    assert isinstance(out, Request) and out.url == URL and out.dont_filter and out.priority == 3
    assert out.meta["calyprium_pool_busy"] == 1
    assert (out.meta["retry_times"], out.meta["calyprium_attempt_number"], out.meta["mimic_transport_retries"]) == (
        1, 1, 1)
    first = mw._sleep.await_args.args[0]
    assert 2.0 <= first <= 4.0
    again = await mw.process_exception(out, LocalFetchError(HTTPCLOAK_BUSY), None)
    assert again.meta["calyprium_pool_busy"] == 2 and again.meta["retry_times"] == 1
    assert 2.0 <= mw._sleep.await_args.args[0] <= 8.0
    assert _stat(mw, "calyprium/pool_busy") == 2
    assert _stat(mw, "calyprium/pool_busy/requeued") == 2
    assert _stat(mw, "calyprium/pool_busy/wait_seconds") == pytest.approx(
        sum(c.args[0] for c in mw._sleep.await_args_list), abs=0.01)


def test_backoff_is_jittered_and_grows_to_the_cap():
    mw = _mw()
    for refusals, ceiling in ((1, 4.0), (2, 8.0), (3, 10.0), (30, 10.0)):
        delays = [mw._delay(refusals) for _ in range(200)]
        assert all(2.0 <= d <= ceiling for d in delays)
        assert max(delays) - min(delays) > 0.5 * (ceiling - 2.0)  # spread, not a fixed delay


@pytest.mark.asyncio
async def test_a_plain_http_refusal_is_requeued():
    mw = _mw()
    req = Request("http://shop.example/p")
    out = await mw.process_response(req, Response(req.url, status=503, headers=BUSY_HEADERS, request=req), None)
    assert isinstance(out, Request) and out.meta["calyprium_pool_busy"] == 1
    assert "retry_times" not in out.meta


@pytest.mark.asyncio
async def test_the_cap_hands_the_refusal_to_the_usual_handling():
    mw = _mw({"CALYPRIUM_POOL_BUSY_MAX_REQUEUES": 3})
    req = Request(URL, meta={"calyprium_pool_busy": 3})
    assert await mw.process_exception(req, _tunnel_error(BUSY_CONNECT), None) is None
    resp = Response(URL, status=503, headers=BUSY_HEADERS, request=req)
    assert await mw.process_response(req, resp, None) is resp
    mw._sleep.assert_not_awaited()
    assert _stat(mw, "calyprium/pool_busy/max_reached") == 2
    assert _stat(mw, "calyprium/pool_busy") == 2
    assert _stat(mw, "calyprium/pool_busy/requeued") is None


@pytest.mark.asyncio
async def test_anything_else_passes_untouched():
    mw = _mw()
    req = Request(URL)
    assert await mw.process_exception(req, TimeoutError("timed out"), None) is None
    page = HtmlResponse(URL, status=503, body=b"<html>busy</html>", request=req)
    assert await mw.process_response(req, page, None) is page
    internal = Request(URL, meta={"_internal": True})
    assert await mw.process_exception(internal, PoolBusyError(), None) is None
    assert _stat(mw, "calyprium/pool_busy") is None


# -- AIMD ------------------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_refusals_back_the_concurrency_off_once_per_interval_down_to_one():
    downloader, clock = FakeDownloader(16), Clock()
    mw = _mw(downloader=downloader, clock=clock)
    req = Request(URL)
    await mw.process_exception(req, PoolBusyError(), None)
    assert downloader.total_concurrency == 12
    await mw.process_exception(req, PoolBusyError(), None)  # the same burst: no second cut
    assert downloader.total_concurrency == 12
    seen = []
    for _ in range(12):
        clock.now += AIMD_DECREASE_INTERVAL_S
        await mw.process_exception(req, PoolBusyError(), None)
        seen.append(downloader.total_concurrency)
    assert seen[:6] == [9, 6, 4, 3, 2, 1] and set(seen[6:]) == {1}
    assert _stat(mw, "calyprium/pool_busy/concurrency") == 1


@pytest.mark.asyncio
async def test_a_run_of_successes_raises_it_back_to_the_configured_value():
    downloader = FakeDownloader(14)
    mw = _mw(downloader=downloader)  # CONCURRENT_REQUESTS 16
    req = Request(URL)
    ok = HtmlResponse(URL, body=b"<html>page</html>", request=req)
    for _ in range(AIMD_INCREASE_AFTER - 1):
        await mw.process_response(req, ok, None)
    assert downloader.total_concurrency == 14
    await mw.process_response(req, ok, None)
    assert downloader.total_concurrency == 15
    for _ in range(AIMD_INCREASE_AFTER * 5):
        await mw.process_response(req, ok, None)
    assert downloader.total_concurrency == 16  # never above CONCURRENT_REQUESTS


@pytest.mark.asyncio
async def test_a_refusal_restarts_the_run_of_successes():
    downloader = FakeDownloader(16)
    mw = _mw(downloader=downloader)
    req = Request(URL)
    await mw.process_exception(req, PoolBusyError(), None)  # 16 -> 12
    ok = HtmlResponse(URL, body=b"<html>page</html>", request=req)
    for _ in range(AIMD_INCREASE_AFTER - 1):
        await mw.process_response(req, ok, None)
    await mw.process_exception(req, PoolBusyError(), None)  # within the interval: no cut, but the run restarts
    await mw.process_response(req, ok, None)
    assert downloader.total_concurrency == 12


@pytest.mark.asyncio
async def test_aimd_can_be_turned_off():
    downloader = FakeDownloader(16)
    mw = _mw({"CALYPRIUM_POOL_AIMD": False}, downloader=downloader)
    out = await mw.process_exception(Request(URL), PoolBusyError(), None)
    assert isinstance(out, Request)  # still requeued
    assert downloader.total_concurrency == 16


@pytest.mark.asyncio
async def test_concurrency_changes_are_logged_rate_limited(caplog):
    downloader, clock = FakeDownloader(64), Clock()
    mw = _mw({"CONCURRENT_REQUESTS": 64}, downloader=downloader, clock=clock)
    caplog.set_level("INFO", logger="scrapy_calyprium.middleware.portfolio")

    def lines():
        return [r.getMessage() for r in caplog.records if "concurrency" in r.getMessage()]

    await mw.process_exception(Request(URL), PoolBusyError(), None)  # 64 -> 48, logged
    clock.now += AIMD_DECREASE_INTERVAL_S
    await mw.process_exception(Request(URL), PoolBusyError(), None)  # 48 -> 36, 5 s after the last log
    assert downloader.total_concurrency == 36
    assert lines() == ["PoolBusyMiddleware: concurrency 64 -> 48 (the proxy pool is busy; CONCURRENT_REQUESTS 64)"]
    clock.now += AIMD_DECREASE_INTERVAL_S
    await mw.process_exception(Request(URL), PoolBusyError(), None)  # 36 -> 27, 10 s after it
    assert lines()[-1] == "PoolBusyMiddleware: concurrency 48 -> 27 (the proxy pool is busy; CONCURRENT_REQUESTS 64)"


# -- the local route -----------------------------------------------------------------------------------------


def _seeded_cache(egress_ip=None):
    cache = DomainCache()
    slot = cache.set_cookies_from_solve(
        "shop.example", cookies=[{"name": "cf_clearance", "value": "abc"}],
        user_agent="UA-1", proxy_session_id="sess-1", egress_ip=egress_ip,
    )
    return cache, slot


@pytest.mark.asyncio
@pytest.mark.parametrize("busy", [LocalFetchError(HTTPCLOAK_BUSY, proxy_error="pool_busy"), _busy_fetch()])
async def test_a_refused_light_fetch_never_solves_nor_changes_exit(busy):
    f, s = FakeFetcher(), FakeSolveClient()
    f.queue(busy)
    router = _make_router(fetcher=f, solve=s)
    router._light_sessions = ["lt-a"]
    result = await router.fetch(URL, domain="shop.example")
    assert result.pool_busy and result.routing_method == "pool_busy"
    assert result.fetch is None and not result.blocked
    assert result.diagnosis.cls == "proxy_failure" and result.diagnosis.evidence == "pool_busy"
    assert s.calls == [] and len(f.calls) == 1  # no solve, no second fetch
    assert router._light_sessions == ["lt-a"]  # the exit is not to blame
    assert router.cache.get("shop.example") is None  # nothing learnt about the domain


@pytest.mark.asyncio
@pytest.mark.parametrize("busy", [LocalFetchError(CURL_BUSY, proxy_error="pool_busy"), _busy_fetch()])
async def test_a_refused_cookie_replay_blames_no_slot_and_no_ip(busy):
    cache, slot = _seeded_cache(egress_ip="203.0.113.10")
    f, s = FakeFetcher(), ReportingSolveClient()
    f.queue(_blocked_403(), busy)  # the site challenges the light fetch; the replay is refused by the gateway
    router = _make_router(fetcher=f, solve=s, cache=cache)
    result = await router.fetch(URL, domain="shop.example")
    await asyncio.sleep(0)  # let any IP report run
    assert result.pool_busy
    assert (slot.fail_count, slot.block_count, slot.in_flight) == (0, 0, 0)
    assert slot.requests_per_minute() == 0  # the refused replay never reached the site
    assert s.solve_calls == [] and s.report_calls == []
    assert cache.get("shop.example").level == "cookies" and slot.is_live


@pytest.mark.asyncio
async def test_a_refused_replay_after_a_solve_keeps_the_new_slot_unblamed():
    f, s = FakeFetcher(), FakeSolveClient()
    f.queue(_blocked_403(), LocalFetchError(HTTPCLOAK_BUSY))
    s.queue(_solve_ok())
    router = _make_router(fetcher=f, solve=s)
    result = await router.fetch(URL, domain="shop.example")
    assert result.pool_busy
    (slot,) = router.cache.get("shop.example").slots
    assert slot.fail_count == 0 and slot.is_live
    assert len(s.calls) == 1  # the solve that ran before the refusal, and no other


@pytest.mark.asyncio
async def test_a_refusal_records_no_span():
    tracer = mock.Mock(run_number=7)
    f = FakeFetcher()
    f.queue(LocalFetchError(HTTPCLOAK_BUSY))
    router = _make_router(fetcher=f)
    router.tracer = tracer
    result = await router.fetch(URL, domain="shop.example")
    assert result.pool_busy and result.trace_id is None
    tracer.record_span.assert_not_called()


@pytest.mark.asyncio
async def test_the_mimic_middleware_raises_pool_busy_instead_of_falling_back():
    router = FakeRouter()
    router.queue(RouteResult(fetch=None, routing_method="pool_busy", blocked=False, domain_level="light",
                             error=HTTPCLOAK_BUSY, pool_busy=True))
    mw = _make_middleware(local_router=router)
    req = Request(URL)
    with pytest.raises(PoolBusyError) as caught:
        await mw.process_request(req, None)
    assert _pool_busy.is_pool_busy_error(caught.value)
    assert mw._local_stats == {"local_success": 0, "local_blocked": 0, "local_solve": 0, "fallback_legacy": 0,
                               "pool_busy": 1}
    assert "calyprium_fallback_diagnosis" not in req.meta  # no fall-back to a plain download
    # PoolBusyMiddleware (950) sees the exception first and requeues it.
    out = await _mw().process_exception(req, caught.value, None)
    assert isinstance(out, Request) and out.meta["calyprium_pool_busy"] == 1


@pytest.mark.asyncio
async def test_without_pool_busy_middleware_it_is_a_transport_error():
    crawler = make_crawler({"MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "k",
                            "DOWNLOADER_MIDDLEWARES": {"scrapy.downloadermiddlewares.retry.RetryMiddleware": None}})
    mw = MimicBrowserMiddleware.from_crawler(crawler)
    out = await mw.process_exception(Request(URL), PoolBusyError(HTTPCLOAK_BUSY), None)
    assert isinstance(out, Request) and out.meta["mimic_transport_retries"] == 1


# -- the run's stats -----------------------------------------------------------------------------------------


def _run_stats():
    crawler = make_crawler({"FORGE_API_URL": "http://forge", "CALYPRIUM_API_KEY": "clp_k",
                            "RECRAWL_SPIDER_SLUG": "shop", "RECRAWL_USER_ID": "u-1", "SPIDER_RUN_NUMBER": 7})
    crawler.spider = SimpleNamespace(name="shop")
    return crawler, CalypriumRunStats.from_crawler(crawler)


@pytest.mark.asyncio
async def test_run_stats_count_no_failure_and_no_new_request():
    crawler, ext = _run_stats()
    mw = _mw()
    mw.crawler = crawler
    mw.stats = crawler.stats
    req = Request(URL)
    ext.request_scheduled(req, None)
    requeue = await mw.process_exception(req, _tunnel_error(BUSY_CONNECT), None)
    ext.request_scheduled(requeue, None)  # the engine schedules the requeue
    ok = HtmlResponse(URL, body=PAGE, request=requeue)
    ext.response_received(ok, requeue, None)
    assert ext._request_count == 1
    assert ext._response_count == 1 and dict(ext._status_counts) == {"200": 1}
    assert dict(ext._class_counts) == {"ok": 1}
    with mock.patch("scrapy_calyprium.extensions.run_stats.httpx.post") as post:
        ext._flush()
    payload = post.call_args.kwargs["json"]
    assert payload["pool_busy_count"] == 1 and payload["request_count"] == 1
    assert payload["failure_class_counts"] == {"ok": 1}
    # A later retry of the requeued request is a request again.
    ext.request_scheduled(requeue.replace(dont_filter=True), None)
    assert ext._request_count == 2
