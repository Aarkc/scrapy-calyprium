"""Retry what is worth retrying: the diagnosis's move decides, not a status list.

The platform disables Scrapy's RetryMiddleware, so before this a blocked page, a rate limit or a
site error was never tried again (only transport errors were, AAR-64), and a spider's RETRY_*
settings did nothing.
"""
from __future__ import annotations

import time
from types import SimpleNamespace
from unittest import mock

import pytest
from scrapy.http import HtmlResponse, Request

from scrapy_calyprium.extensions.request_tracer import CalypriumRequestTracer
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.routing.local_fetch import LocalFetchError, LocalFetchResult
from tests._helpers import make_crawler
from tests.test_local_routing import FakeFetcher, FakeSolveClient, _make_router, _solve_ok

URL = "https://shop.example/p/1"
PAGE = (b"<html><head><title>Widget | Shop</title></head><body><nav></nav><main>"
        + b"<a href='/p'>product</a>" * 5 + b"</main></body></html>")
CF_CHALLENGE = (b"<html><head><title>Just a moment...</title></head><body>"
                b"<script>window._cf_chl_opt={cvId:'3'}</script></body></html>")
CF_HEADERS = {"Server": "cloudflare", "CF-RAY": "8a1b"}
CF_1010 = (b"<html><head><title>Access denied | shop.example | Cloudflare</title></head><body>"
           b"<h1>Sorry, you have been blocked</h1><span>Error code: 1010</span></body></html>")
RUN = {"FORGE_API_URL": "http://forge", "CALYPRIUM_API_KEY": "clp_k",
       "RECRAWL_SPIDER_SLUG": "shop", "RECRAWL_USER_ID": "u-1", "SPIDER_RUN_NUMBER": 7}


def _mw(settings=None, tracer=None, builtin_retry=False):
    values = {"MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "k",
              "CALYPRIUM_RETRY_DELAY": 5.0, **(settings or {})}
    if not builtin_retry:
        values["DOWNLOADER_MIDDLEWARES"] = {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": None}
    crawler = make_crawler(values)
    if tracer is not None:
        crawler.extensions = SimpleNamespace(middlewares=[tracer])
    mw = MimicBrowserMiddleware.from_crawler(crawler)
    mw._sleep = mock.AsyncMock()
    return mw


def _resp(req, status=200, body=PAGE, headers=None):
    return HtmlResponse(req.url, status=status, body=body, headers=headers or {}, request=req)


async def _attempt(mw, req, status=200, body=PAGE, headers=None):
    """One download attempt as Scrapy runs it: process_request, then process_response."""
    assert await mw.process_request(req, None) is None  # plain download (no Mimic route)
    return await mw.process_response(req, _resp(req, status, body, headers), None)


def _stat(mw, key):
    return mw.crawler.stats.get_value(key)


# -- what is retried -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_challenge_is_retried_at_once_carrying_the_prior_class():
    mw = _mw()
    out = await _attempt(mw, Request(URL), 403, CF_CHALLENGE, CF_HEADERS)
    assert isinstance(out, Request) and out.dont_filter and out.priority == -1
    assert out.meta["calyprium_attempt_number"] == 1
    assert out.meta["calyprium_prior_class"] == "challenge"
    mw._sleep.assert_not_awaited()
    assert _stat(mw, "calyprium/retry/challenge") == 1
    assert _stat(mw, "calyprium/retry/count") == 1


@pytest.mark.asyncio
async def test_retries_stop_at_calyprium_retry_times():
    mw = _mw({"CALYPRIUM_RETRY_TIMES": 2})
    req = Request(URL)
    for n in (1, 2):
        req = await _attempt(mw, req, 403, CF_CHALLENGE, CF_HEADERS)
        assert req.meta["calyprium_attempt_number"] == n
    final = await _attempt(mw, req, 403, CF_CHALLENGE, CF_HEADERS)
    assert isinstance(final, HtmlResponse) and final.status == 403
    assert final.meta["calyprium_diagnosis"]["class"] == "challenge"  # the spider sees why
    assert _stat(mw, "calyprium/retry/challenge") == 2
    assert _stat(mw, "calyprium/retry/max_reached") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status,body,cls", [
    (404, PAGE, "not_found"),
    (451, b"<html>Unavailable for legal reasons</html>", "geo_or_consent"),
    (401, b"", "login_wall"),
    (200, PAGE, "ok"),
])
async def test_stop_moves_and_good_pages_are_never_retried(status, body, cls):
    mw = _mw()
    req = Request(URL)
    out = await _attempt(mw, req, status, body)
    assert isinstance(out, HtmlResponse)
    assert req.meta["calyprium_diagnosis"]["class"] == cls
    assert _stat(mw, "calyprium/retry/count") is None


@pytest.mark.asyncio
async def test_fix_ours_is_retried_once():
    mw = _mw({"CALYPRIUM_RETRY_TIMES": 5})
    first = await _attempt(mw, Request(URL), 403, CF_1010, CF_HEADERS)
    assert first.meta["calyprium_prior_class"] == "identity_deny"
    again = await _attempt(mw, first, 403, CF_1010, CF_HEADERS)
    assert isinstance(again, HtmlResponse)
    assert _stat(mw, "calyprium/retry/identity_deny") == 1


@pytest.mark.asyncio
async def test_a_rate_limit_waits_for_retry_after():
    mw = _mw()
    out = await _attempt(mw, Request(URL), 429, b"<html>slow down</html>", {"Retry-After": "7"})
    assert out.meta["calyprium_prior_class"] == "rate_limited"
    mw._sleep.assert_awaited_once_with(7.0)


@pytest.mark.asyncio
async def test_a_rate_limit_without_retry_after_backs_off_doubling():
    mw = _mw({"CALYPRIUM_RETRY_TIMES": 3})
    req = await _attempt(mw, Request(URL), 429, b"<html>slow down</html>")
    req = await _attempt(mw, req, 429, b"<html>slow down</html>")
    assert [c.args[0] for c in mw._sleep.await_args_list] == [5.0, 10.0]


@pytest.mark.asyncio
async def test_delays_are_capped():
    mw = _mw({"CALYPRIUM_RETRY_MAX_DELAY": 30})
    await _attempt(mw, Request(URL), 503, b"busy", {"Retry-After": "3600",
                                                   "Content-Type": "text/plain"})
    mw._sleep.assert_awaited_once_with(30.0)


@pytest.mark.asyncio
async def test_a_site_error_is_retried_later():
    mw = _mw()
    out = await _attempt(mw, Request(URL), 500, b"<html>error</html>")
    assert out.meta["calyprium_prior_class"] == "origin_error"
    mw._sleep.assert_awaited_once_with(5.0)


@pytest.mark.asyncio
async def test_a_proxy_failure_from_mimic_is_retried_elsewhere_at_once():
    mw = _mw({"MIMIC_ALL_REQUESTS": True})
    resp = mock.Mock()
    resp.json.return_value = {"html": "", "status_code": 502,
                              "headers": {"x-mimic-error": "NS_ERROR_PROXY_BAD_GATEWAY"}}
    resp.raise_for_status = mock.Mock()
    mw._client = mock.AsyncMock(is_closed=False)
    mw._client.post.return_value = resp
    req = Request(URL)
    response = await mw.process_request(req, None)
    out = await mw.process_response(req, response, None)
    assert out.meta["calyprium_prior_class"] == "proxy_failure"
    mw._sleep.assert_not_awaited()
    # The retry tells Mimic where its ladder stood.
    await mw.process_request(out, None)
    payload = mw._client.post.call_args.kwargs["json"]
    assert (payload["attempt"], payload["prior_class"]) == (1, "proxy_failure")


@pytest.mark.asyncio
@pytest.mark.parametrize("with_diagnosis", [True, False])
async def test_mimics_pacing_answer_is_waited_out_not_retried_at_once(with_diagnosis):
    """Mimic holds a domain at its pace: a 429 envelope with Retry-After (mimic escalation.pace)."""
    mw = _mw({"MIMIC_ALL_REQUESTS": True})
    data = {"html": "", "status_code": 429, "routing": {"routing_method": "auto"},
            "headers": {"retry-after": "12",
                        "x-mimic-error": "paced: shop.example is held at 0.50 requests/s"}}
    if with_diagnosis:
        data["routing"]["diagnosis"] = {"class": "rate_limited", "status": 429,
                                        "evidence": "platform_pace", "retry_after": 12.0}
    resp = mock.Mock()
    resp.json.return_value = data
    resp.raise_for_status = mock.Mock()
    mw._client = mock.AsyncMock(is_closed=False)
    mw._client.post.return_value = resp
    req = Request(URL)
    response = await mw.process_request(req, None)
    assert req.meta["calyprium_diagnosis"]["class"] == "rate_limited"
    out = await mw.process_response(req, response, None)
    assert out.meta["calyprium_prior_class"] == "rate_limited"
    mw._sleep.assert_awaited_once_with(12.0)


# -- opting out, and leaving Scrapy's own retry alone ------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("settings,meta", [
    ({"CALYPRIUM_RETRY_ENABLED": False}, {}),
    ({}, {"dont_retry": True}),
    ({}, {"max_retry_times": 0}),
    ({"CALYPRIUM_RETRY_TIMES": 0}, {}),
])
async def test_retry_opt_outs(settings, meta):
    mw = _mw(settings)
    out = await _attempt(mw, Request(URL, meta=meta), 403, CF_CHALLENGE, CF_HEADERS)
    assert isinstance(out, HtmlResponse)


@pytest.mark.asyncio
async def test_scrapys_retry_middleware_keeps_its_statuses():
    mw = _mw(builtin_retry=True)
    assert 503 in mw._builtin_retry_codes
    vendor_503 = await _attempt(mw, Request(URL), 503, b"<html></html>", CF_HEADERS)
    assert isinstance(vendor_503, HtmlResponse)  # RetryMiddleware already retried it
    challenge = await _attempt(mw, Request(URL), 403, CF_CHALLENGE, CF_HEADERS)
    assert isinstance(challenge, Request)  # 403 is not its business


# -- the retried attempt is still traced (and priced) once ------------------------------


@pytest.mark.asyncio
async def test_a_retried_attempt_is_traced_once_and_its_retry_names_its_attempt():
    tracer = CalypriumRequestTracer.from_crawler(make_crawler(RUN))
    mw = _mw(tracer=tracer)
    req = Request(URL)
    retry = await _attempt(mw, req, 403, CF_CHALLENGE, CF_HEADERS)
    (first,) = tracer._buffer
    assert (first["status"], first["failure_class"], first["vendor"], first["attempt"]) == (
        "blocked", "challenge", "cloudflare", 0)
    final = await _attempt(mw, retry, 200, PAGE)
    tracer.response_received(final, retry)  # Scrapy fires it for the final response only
    second = tracer._buffer[1]
    assert (second["status"], second["failure_class"], second["attempt"]) == ("success", "ok", 1)
    assert len(tracer._buffer) == 2


@pytest.mark.asyncio
async def test_sleep_waits_without_blocking_the_loop():
    started = time.monotonic()
    await MimicBrowserMiddleware._sleep(0.05)
    assert time.monotonic() - started >= 0.04


# -- the local route: transport errors and rate limits ---------------------------------------


def _local(status, body, headers=None):
    return LocalFetchResult(url=URL, final_url=URL, status_code=status, body=body,
                            headers={"content-type": "text/html", **(headers or {})},
                            backend="httpcloak")


@pytest.mark.asyncio
async def test_a_local_transport_error_is_never_a_success_span():
    tracer = CalypriumRequestTracer.from_crawler(make_crawler(RUN))
    fetcher = FakeFetcher()
    fetcher.queue(LocalFetchError("read timed out"))
    router = _make_router(fetcher=fetcher)
    router.tracer = tracer
    result = await router.fetch(URL, domain="shop.example")
    assert result.routing_method == "transport_error" and result.diagnosis.cls == "proxy_failure"
    (span,) = tracer._buffer
    assert span["status"] == "timeout" and span["status_code"] == 0
    assert span["failure_class"] == "proxy_failure"


@pytest.mark.asyncio
async def test_a_local_rate_limit_backs_off_without_a_solve():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    fetcher.queue(_local(429, b"<html>slow down</html>", {"retry-after": "2"}))
    router = _make_router(fetcher=fetcher, solve=solve)
    result = await router.fetch(URL, domain="shop.example")
    assert result.blocked and result.diagnosis.cls == "rate_limited"
    assert solve.calls == []  # a solve cannot lift a rate limit


@pytest.mark.asyncio
async def test_the_middleware_slows_down_a_local_rate_limit():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    fetcher.queue(_local(429, b"<html>slow down</html>", {"retry-after": "2"}))
    mw = _mw()
    mw._local_enabled = True
    mw._local_router = _make_router(fetcher=fetcher, solve=solve)
    req = Request(URL)
    response = await mw.process_request(req, None)
    assert response.status == 429
    assert req.meta["calyprium_diagnosis"]["class"] == "rate_limited"
    out = await mw.process_response(req, response, None)
    assert out.meta["calyprium_prior_class"] == "rate_limited"
    mw._sleep.assert_awaited_once_with(2.0)


@pytest.mark.asyncio
async def test_a_refused_clearance_is_named():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    fetcher.queue(_local(403, CF_CHALLENGE, {"server": "cloudflare"}),
                  _local(403, CF_CHALLENGE, {"server": "cloudflare"}))
    solve.queue(_solve_ok())
    router = _make_router(fetcher=fetcher, solve=solve)
    result = await router.fetch(URL, domain="shop.example")
    assert result.blocked and result.routing_method == "solve_then_replay"
    d = result.diagnosis
    assert (d.cls, d.evidence) == ("clearance_refused", "replay_refused")


@pytest.mark.asyncio
async def test_a_local_fallthrough_keeps_why():
    fetcher = FakeFetcher()
    fetcher.queue(LocalFetchError("connection reset by peer"))
    mw = _mw()
    mw._local_enabled = True
    mw._local_router = _make_router(fetcher=fetcher)
    req = Request(URL)
    assert await mw.process_request(req, None) is None  # on to a plain download
    assert req.meta["calyprium_fallback_diagnosis"]["class"] == "proxy_failure"
    assert "connection reset" in req.meta["calyprium_fetch_error"]
    assert mw._local_stats["local_success"] == 0
