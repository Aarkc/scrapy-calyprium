"""Keep the cause: every attempt carries the shared classifier's diagnosis.

Before this, a Mimic /api/fetch reply became a response with only its Content-Type, Mimic's
x-mimic-error and the reason a Mimic call failed were dropped, spans called a page blocked by
status code alone, and CalypriumRunStats counted a routing method nothing set.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import httpx
import pytest
from scrapy import Spider
from scrapy.downloadermiddlewares.httpcompression import HttpCompressionMiddleware
from scrapy.http import HtmlResponse, Request
from twisted.internet.error import TCPTimedOutError

from scrapy_calyprium.extensions.request_tracer import CalypriumRequestTracer
from scrapy_calyprium.extensions.run_stats import CalypriumRunStats
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.spectre import SpectreMiddleware
from tests._helpers import make_crawler

URL = "https://shop.example/p/1"
PAGE = ("<html><head><title>Widget | Shop</title></head><body><nav></nav><main>"
        + "<a href='/p'>product</a>" * 5 + "</main></body></html>")
CF_CHALLENGE = ("<html><head><title>Just a moment...</title></head><body>"
                "<script>window._cf_chl_opt={cvId:'3'}</script></body></html>")
AKAMAI_DENY = "<html><head><title>Access Denied</title></head><body>You don't have permission</body></html>"
RUN = {"FORGE_API_URL": "http://forge", "CALYPRIUM_API_KEY": "clp_k",
       "RECRAWL_SPIDER_SLUG": "shop", "RECRAWL_USER_ID": "u-1", "SPIDER_RUN_NUMBER": 7}


def _mw(settings=None, tracer=None):
    crawler = make_crawler({
        "MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "k",
        "DOWNLOADER_MIDDLEWARES": {"scrapy.downloadermiddlewares.retry.RetryMiddleware": None},
        **(settings or {}),
    })
    if tracer is not None:
        crawler.extensions = SimpleNamespace(middlewares=[tracer])
    mw = MimicBrowserMiddleware.from_crawler(crawler)
    mw._sleep = mock.AsyncMock()
    return mw


def _client(data):
    resp = mock.Mock()
    resp.json.return_value = data
    resp.raise_for_status = mock.Mock()
    client = mock.AsyncMock()
    client.post.return_value = resp
    client.is_closed = False
    return client


def _tracer():
    crawler = make_crawler(RUN)
    return CalypriumRequestTracer.from_crawler(crawler)


# -- Mimic paths keep the cause ------------------------------------------------------


@pytest.mark.asyncio
async def test_mimic_headers_reach_the_response_without_framing_or_cookies():
    mw = _mw()
    req = Request(URL)
    client = _client({
        "html": PAGE, "status_code": 200, "final_url": URL,
        "headers": {"content-type": "text/html; charset=utf-8", "content-encoding": "gzip",
                    "content-length": "999", "transfer-encoding": "chunked",
                    "connection": "keep-alive", "set-cookie": "session=SECRET123; Path=/",
                    "server": "cloudflare", "cf-ray": "8a1b2c", "x-cache": "HIT"},
    })
    resp = await mw._fetch_auto(client, req)
    assert resp.headers.get("Server") == b"cloudflare"
    assert resp.headers.get("Cf-Ray") == b"8a1b2c"
    assert resp.headers.get("X-Cache") == b"HIT"
    assert resp.headers.get("Content-Type") == b"text/html; charset=utf-8"
    for name in ("Content-Encoding", "Content-Length", "Transfer-Encoding", "Connection",
                 "Set-Cookie"):
        assert name not in resp.headers, name
    assert "SECRET123" not in repr(resp.headers) and "SECRET123" not in repr(req.meta)
    assert req.meta["calyprium_diagnosis"]["class"] == "ok"
    assert req.meta["calyprium_routing_method"] == "httpcloak"
    # The page Mimic decoded is not decoded a second time downstream.
    out = HttpCompressionMiddleware().process_response(req, resp, Spider("s"))
    assert out.text == PAGE


@pytest.mark.asyncio
async def test_mimics_own_diagnosis_is_kept():
    mw = _mw()
    req = Request(URL)
    client = _client({
        "html": AKAMAI_DENY, "status_code": 403, "headers": {"content-type": "text/html"},
        "routing": {"routing_method": "browser_fallback", "rung": 3, "cost_usd": 0.002,
                    "diagnosis": {"class": "edge_deny", "vendor": "akamai", "status": 403,
                                  "evidence": "akamai_deny"},
                    "attempts": [{"step": "httpcloak", "class": "edge_deny", "ms": 120}]},
    })
    await mw._fetch_auto(client, req)
    d = req.meta["calyprium_diagnosis"]
    assert (d["class"], d["vendor"], d["evidence"]) == ("edge_deny", "akamai", "akamai_deny")
    assert (d["number"], d["side"], d["move"]) == (6, "site", "climb")  # filled from the class
    assert req.meta["calyprium_routing"]["rung"] == 3


@pytest.mark.asyncio
async def test_older_mimic_gets_the_classifier_verdict_with_cookie_names_only():
    mw = _mw()
    req = Request(URL)
    client = _client({
        "html": AKAMAI_DENY, "status_code": 403,
        "headers": {"content-type": "text/html", "set-cookie": "_abck=VALUE9~-1~; Path=/"},
    })
    resp = await mw._fetch_auto(client, req)
    d = req.meta["calyprium_diagnosis"]
    assert (d["class"], d["vendor"]) == ("edge_deny", "akamai")  # vendor from the cookie's name
    assert "VALUE9" not in repr(d) and "Set-Cookie" not in resp.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("error,cls", [
    ("DeadlineExceeded: deadline spent", "capacity"),
    ("NS_ERROR_PROXY_BAD_GATEWAY", "proxy_failure"),
])
async def test_a_degraded_reply_keeps_mimics_error(error, cls):
    mw = _mw()
    req = Request(URL)
    client = _client({"html": "", "status_code": 502, "headers": {"x-mimic-error": error}})
    resp = await mw._fetch_auto(client, req)
    assert req.meta["calyprium_fetch_error"] == error
    assert req.meta["calyprium_diagnosis"]["class"] == cls
    assert resp.headers.get("X-Mimic-Error") == error.encode()


@pytest.mark.asyncio
async def test_a_failed_mimic_call_keeps_why_for_the_fallback_download():
    mw = _mw({"MIMIC_ALL_REQUESTS": True})
    mw._client = _client({})
    mw._client.post.side_effect = httpx.ConnectError("connection refused")
    req = Request(URL)
    assert await mw.process_request(req, None) is None  # continues as a plain download
    assert req.meta["calyprium_fallback_diagnosis"]["class"] == "proxy_failure"
    assert req.meta["calyprium_fetch_error"].startswith("ConnectError")
    assert "calyprium_diagnosis" not in req.meta
    # The plain download's response is diagnosed on its own.
    resp = HtmlResponse(URL, status=200, body=PAGE.encode(), request=req)
    assert await mw.process_response(req, resp, None) is resp
    assert req.meta["calyprium_diagnosis"]["class"] == "ok"
    assert req.meta["calyprium_fallback_diagnosis"]["class"] == "proxy_failure"


@pytest.mark.asyncio
async def test_a_busy_mimic_is_our_capacity():
    mw = _mw({"MIMIC_ALL_REQUESTS": True})
    busy = httpx.Response(503, json={"detail": "Engine camoufox is not available"},
                          headers={"Retry-After": "5"},
                          request=httpx.Request("POST", "http://mimic/api/fetch"))
    client = mock.AsyncMock()
    client.post.return_value = busy
    client.is_closed = False
    mw._client = client
    req = Request(URL)
    assert await mw.process_request(req, None) is None
    d = req.meta["calyprium_fallback_diagnosis"]
    assert (d["class"], d["evidence"], d["retry_after"]) == ("capacity", "mimic_503", 5.0)
    assert req.meta["calyprium_fetch_error"] == "mimic 503: Engine camoufox is not available"


@pytest.mark.asyncio
async def test_attempt_and_prior_class_go_to_mimic_only_on_a_retry():
    mw = _mw()
    client = _client({"html": PAGE, "status_code": 200})
    await mw._fetch_auto(client, Request(URL))
    first = client.post.call_args.kwargs["json"]
    assert "attempt" not in first and "prior_class" not in first
    retry = Request(URL, meta={"calyprium_attempt_number": 2, "calyprium_prior_class": "challenge"})
    await mw._fetch_auto(client, retry)
    payload = client.post.call_args.kwargs["json"]
    assert (payload["attempt"], payload["prior_class"]) == (2, "challenge")


@pytest.mark.asyncio
async def test_a_browser_session_keeps_the_cause():
    mw = _mw()
    mw.session_id = "sess-1"
    req = Request(URL, meta={"mimic": True})
    client = _client({"html": CF_CHALLENGE, "status_code": 403, "browser_engine": "camoufox",
                      "headers": {"content-type": "text/html", "server": "cloudflare"}})
    resp = await mw._fetch_browser(client, req, None)
    assert resp.headers.get("Server") == b"cloudflare"
    d = req.meta["calyprium_diagnosis"]
    assert (d["class"], d["vendor"]) == ("challenge", "cloudflare")
    assert req.meta["calyprium_routing_method"] == "browser_direct"


@pytest.mark.asyncio
async def test_a_transport_error_keeps_its_diagnosis_for_the_errback():
    mw = _mw()
    req = Request(URL)
    retry = await mw.process_exception(req, TCPTimedOutError(), None)
    d = req.meta["calyprium_diagnosis"]
    assert (d["class"], d["evidence"]) == ("proxy_failure", "timeout")
    assert retry is not None
    assert mw.crawler.stats.get_value("calyprium/retry/proxy_failure") == 1


# -- Tracer: the outcome is the diagnosis -----------------------------------------------


@pytest.mark.asyncio
async def test_span_carries_the_diagnosis_rung_and_attempt():
    tracer = _tracer()
    mw = _mw(tracer=tracer)
    req = Request(URL, meta={"calyprium_attempt_number": 1, "calyprium_prior_class": "challenge"})
    await mw.process_request(req, None)
    client = _client({
        "html": CF_CHALLENGE, "status_code": 403, "headers": {"content-type": "text/html"},
        "routing": {"routing_method": "browser_fallback", "engine": "camoufox", "rung": 2,
                    "diagnosis": {"class": "challenge", "vendor": "cloudflare",
                                  "evidence": "cloudflare_challenge", "status": 403}},
    })
    tracer.response_received(await mw._fetch_auto(client, req), req)
    (span,) = tracer._buffer
    assert span["status"] == "blocked"
    assert (span["failure_class"], span["vendor"], span["evidence"]) == (
        "challenge", "cloudflare", "cloudflare_challenge")
    assert (span["rung"], span["attempt"]) == (2, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("routing_blocked,cls,status", [
    (True, "ok", "success"),          # Mimic's old flag no longer overrides the diagnosis
    (False, "not_found", "error"),
    (False, "rate_limited", "blocked"),
    (False, "geo_or_consent", "error"),
])
async def test_span_status_is_the_diagnosis(routing_blocked, cls, status):
    tracer = _tracer()
    mw = _mw(tracer=tracer)
    req = Request(URL)
    client = _client({"html": PAGE, "status_code": 200, "headers": {"content-type": "text/html"},
                      "routing": {"routing_method": "httpcloak", "blocked": routing_blocked,
                                  "diagnosis": {"class": cls}}})
    tracer.response_received(await mw._fetch_auto(client, req), req)
    assert tracer._buffer[0]["status"] == status
    assert tracer._buffer[0]["failure_class"] == cls


def test_a_plain_download_is_diagnosed_by_the_tracer():
    tracer = _tracer()
    req = Request(URL, meta={"proxy": "http://gw:8080"})
    resp = HtmlResponse(URL, status=403, body=CF_CHALLENGE.encode(), request=req,
                        headers={"Server": "cloudflare"})
    tracer.response_received(resp, req)
    span = tracer._buffer[0]
    assert (span["status"], span["failure_class"], span["vendor"]) == (
        "blocked", "challenge", "cloudflare")
    assert span["attempt"] == 0 and "rung" not in span


def test_a_transport_error_span_names_its_class():
    tracer = _tracer()
    req = Request(URL)
    tracer.trace_exception(req, TCPTimedOutError())
    span = tracer._buffer[0]
    assert (span["status"], span["failure_class"], span["evidence"]) == (
        "timeout", "proxy_failure", "timeout")


def test_forge_batch_still_has_every_column_an_older_forge_reads():
    tracer = _tracer()
    req = Request(URL)
    tracer.response_received(HtmlResponse(URL, body=PAGE.encode(), request=req), req)
    span = tracer._buffer[0]
    for column in ("trace_id", "parent_span_id", "timestamp", "duration_ms", "user_id",
                   "spider_slug", "run_number", "url", "domain", "component", "operation",
                   "status", "status_code", "engine", "proxy_session_id", "egress_ip", "slot_id",
                   "routing_method", "response_bytes", "error_message"):
        assert column in span, column


# -- Run stats ------------------------------------------------------------------------


def _stats_ext():
    ext = CalypriumRunStats("http://forge", "", "u-1", "shop", "job", run_number=7)
    ext.stats = make_crawler().stats
    return ext


def test_run_stats_count_each_final_response_by_class():
    ext = _stats_ext()
    for status, body, headers in ((200, PAGE, {}), (403, CF_CHALLENGE, {"Server": "cloudflare"}),
                                  (404, PAGE, {}), (200, PAGE, {})):
        req = Request(URL)
        ext.response_received(HtmlResponse(URL, status=status, body=body.encode(),
                                           headers=headers, request=req), req, None)
    assert dict(ext._class_counts) == {"ok": 2, "challenge": 1, "not_found": 1}
    assert ext.stats.get_value("calyprium/class/ok") == 2
    assert ext.stats.get_value("calyprium/class/challenge") == 1
    with mock.patch("scrapy_calyprium.extensions.run_stats.httpx.post") as post:
        ext._flush()
    assert post.call_args.kwargs["json"]["failure_class_counts"] == {
        "ok": 2, "challenge": 1, "not_found": 1}


def test_run_stats_routing_method_is_set_from_routing():
    ext = _stats_ext()
    cases = [
        {"calyprium_routing": {"routing_method": "browser_fallback"}},
        {"calyprium_routing_method": "httpcloak_cookies", "calyprium_routing": {"routing_method": "x"}},
        {"proxy": "http://gw:8080"},
        {},
    ]
    for meta in cases:
        req = Request(URL, meta=meta)
        ext.response_received(HtmlResponse(URL, body=PAGE.encode(), request=req), req, None)
    assert dict(ext._routing_counts) == {
        "browser_fallback": 1, "httpcloak_cookies": 1, "veil_proxy": 1, "direct": 1}


def test_run_stats_reuse_the_middlewares_diagnosis():
    ext = _stats_ext()
    req = Request(URL, meta={"calyprium_diagnosis": {"class": "captcha", "move": "climb"}})
    ext.response_received(HtmlResponse(URL, body=PAGE.encode(), request=req), req, None)
    assert dict(ext._class_counts) == {"captcha": 1}


# -- Spectre trusts the kept diagnosis -----------------------------------------------------


def test_spectre_rotates_on_the_kept_diagnosis():
    mw = SpectreMiddleware(service_url="http://spectre", api_key="k")
    mw._refresh = mock.Mock()
    req = Request(URL, meta={"calyprium_diagnosis": {"class": "challenge"}})
    mw.process_response(req, HtmlResponse(URL, body=PAGE.encode(), request=req), None)
    assert mw._refresh.call_count == 1
    ok = Request(URL, meta={"calyprium_diagnosis": {"class": "ok"}})
    mw.process_response(ok, HtmlResponse(URL, status=403, body=b"x", request=ok), None)
    assert mw._refresh.call_count == 1
