"""Phase 5 metering: one priced trace span per download attempt.

Before this, CalypriumRequestTracer only saw SpiderAutoRouter.fetch(), so
spiders whose requests went through Mimic's /api/fetch or the plain Veil proxy
path posted no spans and Forge could not price their requests.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import httpx
import pytest
from scrapy import signals
from scrapy.http import HtmlResponse, Request
from twisted.internet.error import TCPTimedOutError

from scrapy_calyprium.extensions import request_tracer as rt
from scrapy_calyprium.extensions.request_tracer import CalypriumRequestTracer
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from tests._helpers import make_crawler

RUN_SETTINGS = {
    "FORGE_API_URL": "http://forge",
    "CALYPRIUM_API_KEY": "clp_k",
    "RECRAWL_SPIDER_SLUG": "shop",
    "RECRAWL_USER_ID": "u-1",
    "SPIDER_RUN_NUMBER": 7,
}


def _tracer(**settings) -> CalypriumRequestTracer:
    crawler = make_crawler({**RUN_SETTINGS, **settings})
    tracer = CalypriumRequestTracer.from_crawler(crawler)
    tracer._crawler = crawler
    return tracer


def _spans(tracer):
    return list(tracer._buffer)


def _response(request, status=200, body=b"<html>ok</html>", flags=None):
    return HtmlResponse(request.url, status=status, body=body, request=request,
                        flags=flags or [])


def _mimic(tracer=None, **settings):
    crawler = make_crawler({
        "MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "clp_k",
        "MIMIC_TRANSPORT_RETRIES": 2, "RETRY_ENABLED": False, **settings,
    })
    if tracer is not None:
        crawler.extensions = SimpleNamespace(middlewares=[tracer])
    return MimicBrowserMiddleware.from_crawler(crawler)


def _mimic_client(data):
    resp = mock.Mock()
    resp.json.return_value = data
    resp.raise_for_status = mock.Mock()
    client = mock.AsyncMock()
    client.post.return_value = resp
    return client


# -- Veil / plain download path ---------------------------------------------------


def test_veil_proxy_response_is_traced_via_signal():
    tracer = _tracer()
    req = Request("https://shop.example/p/1", meta={"proxy": "http://gw:8080",
                                                    "download_latency": 0.25})
    tracer._crawler.signals.send_catch_log(
        signals.response_received, response=_response(req), request=req, spider=None,
    )
    (span,) = _spans(tracer)
    assert span["routing_method"] == "veil_proxy"
    assert span["tier"] == "fast"
    assert span["network"] == "datacenter"
    assert span["status"] == "success"
    assert span["status_code"] == 200
    assert span["response_bytes"] == len(b"<html>ok</html>")
    assert span["duration_ms"] == 250
    assert span["domain"] == "shop.example"
    assert span["run_number"] == 7
    assert span["parent_span_id"] is None  # top-level: Forge prices it


@pytest.mark.parametrize("proxy_type,tier", [
    ("residential", "residential"), ("residential_rotating", "residential"),
    ("datacenter", "fast"), (None, "fast"),
])
def test_veil_tier_follows_proxy_type(proxy_type, tier):
    tracer = _tracer(**({"VEIL_PROXY_TYPE": proxy_type} if proxy_type else {}))
    req = Request("https://a.example", meta={"proxy": "http://gw:8080"})
    tracer.response_received(_response(req), req)
    assert _spans(tracer)[0]["tier"] == tier
    assert _spans(tracer)[0]["network"] == ("residential" if tier == "residential" else "datacenter")


def test_direct_download_is_fast_direct():
    tracer = _tracer()
    req = Request("https://a.example")
    tracer.response_received(_response(req), req)
    span = _spans(tracer)[0]
    assert (span["routing_method"], span["tier"], span["network"]) == ("direct", "fast", "direct")


@pytest.mark.parametrize("status,outcome", [
    (403, "blocked"), (429, "blocked"), (404, "error"), (500, "error"), (301, "success"),
])
def test_outcome_from_status(status, outcome):
    tracer = _tracer()
    req = Request("https://a.example")
    tracer.response_received(_response(req, status=status), req)
    assert _spans(tracer)[0]["status"] == outcome


def test_internal_and_cached_responses_are_not_traced():
    tracer = _tracer()
    internal = Request("https://forge/x", meta={"_internal": True})
    tracer.response_received(_response(internal), internal)
    cached = Request("https://a.example")
    tracer.response_received(_response(cached, flags=["cached"]), cached)
    assert _spans(tracer) == []


def test_disabled_tracer_records_nothing():
    crawler = make_crawler({"FORGE_API_URL": "http://forge"})  # no slug / run number
    tracer = CalypriumRequestTracer.from_crawler(crawler)
    req = Request("https://a.example")
    tracer.response_received(_response(req), req)
    assert _spans(tracer) == []


def test_broken_response_never_raises():
    tracer = _tracer()
    tracer.response_received(object(), object())  # garbage in
    assert _spans(tracer) == []


# -- Mimic /api/fetch and browser sessions -----------------------------------------


@pytest.mark.asyncio
async def test_mimic_fetch_routing_becomes_span_fields():
    tracer = _tracer()
    mw = _mimic(tracer)
    req = Request("https://cf.example/item", meta={"proxy": "http://gw:8080"})
    client = _mimic_client({
        "html": "<html>rendered</html>", "status_code": 200,
        "headers": {"content-type": "text/html"},
        "routing": {
            "routing_method": "browser_fallback", "engine": "camoufox",
            "network": "residential", "bytes": 21, "blocked": False,
            "paid_solve": False, "escalated_from": "httpcloak",
            "escalation_reason": "challenge_cloudflare",
        },
    })
    await mw.process_request(req, None)  # resets per-attempt trace state
    resp = await mw._fetch_auto(client, req)
    tracer.response_received(resp, req)
    (span,) = _spans(tracer)
    assert span["routing_method"] == "browser_fallback"
    assert span["engine"] == "camoufox"
    assert span["tier"] == "browser"
    assert span["network"] == "residential"
    assert span["escalated_from"] == "residential"  # httpcloak on the residential net
    assert span["escalation_reason"] == "challenge_cloudflare"
    assert span["status"] == "success"


@pytest.mark.asyncio
async def test_mimic_explicit_tier_and_block_flag_win():
    tracer = _tracer()
    mw = _mimic(tracer)
    req = Request("https://a.example")
    client = _mimic_client({
        "html": "<html>challenge</html>", "status_code": 200,
        "routing": {"routing_method": "httpcloak", "engine": "httpcloak",
                    "network": "datacenter", "tier": "fast", "blocked": True},
    })
    tracer.response_received(await mw._fetch_auto(client, req), req)
    span = _spans(tracer)[0]
    assert (span["tier"], span["network"], span["status"]) == ("fast", "datacenter", "blocked")
    assert "escalated_from" not in span


@pytest.mark.asyncio
async def test_old_mimic_without_routing_leaves_tier_to_forge():
    tracer = _tracer()
    mw = _mimic(tracer)
    req = Request("https://a.example", meta={"proxy": "http://gw:8080"})
    client = _mimic_client({"html": "<html></html>", "status_code": 200,
                            "browser_engine": "httpcloak"})
    tracer.response_received(await mw._fetch_auto(client, req), req)
    span = _spans(tracer)[0]
    assert span["routing_method"] == "httpcloak"  # not veil_proxy: mimic served it
    assert "tier" not in span and "network" not in span


@pytest.mark.asyncio
async def test_mimic_browser_session_is_browser_tier():
    tracer = _tracer()
    mw = _mimic(tracer)
    mw.session_id = "sess-1"
    req = Request("https://a.example", meta={"mimic": True})
    client = _mimic_client({"html": "<html>js</html>", "status_code": 200,
                            "browser_engine": "nodriver"})
    tracer.response_received(await mw._fetch_browser(client, req, None), req)
    span = _spans(tracer)[0]
    assert (span["routing_method"], span["engine"], span["tier"]) == (
        "browser_direct", "nodriver", "browser")
    assert span["proxy_session_id"] == ""


# -- No double counting with SpiderAutoRouter ----------------------------------------


def _route_result(trace_id):
    from scrapy_calyprium.routing.auto import RouteResult
    from scrapy_calyprium.routing.local_fetch import LocalFetchResult

    fetch = LocalFetchResult(url="https://a.example/", status_code=200, body=b"<html>x</html>",
                             headers={"content-type": "text/html"},
                             final_url="https://a.example/", elapsed_ms=5, backend="httpcloak")
    return RouteResult(fetch=fetch, routing_method="httpcloak_light", blocked=False,
                       domain_level="light", trace_id=trace_id)


@pytest.mark.asyncio
async def test_local_route_span_is_not_recorded_twice():
    tracer = _tracer()
    mw = _mimic(tracer)
    mw._local_enabled = True
    mw._local_router = mock.Mock()
    mw._local_router.fetch = mock.AsyncMock(return_value=_route_result("t-1"))
    req = Request("https://a.example/")
    resp = await mw.process_request(req, None)
    assert req.meta["calyprium_trace_id"] == "t-1"
    tracer.response_received(resp, req)
    assert _spans(tracer) == []  # the router's own span is the only one


@pytest.mark.asyncio
async def test_retried_request_does_not_inherit_the_trace_marker():
    tracer = _tracer()
    mw = _mimic(tracer)
    req = Request("https://a.example/", meta={"calyprium_trace_id": "old",
                                              "calyprium_routing": {"routing_method": "x"}})
    assert await mw.process_request(req, None) is None  # plain download
    assert "calyprium_trace_id" not in req.meta
    req.meta["proxy"] = "http://gw:8080"
    tracer.response_received(_response(req), req)
    assert _spans(tracer)[0]["routing_method"] == "veil_proxy"


@pytest.mark.asyncio
async def test_auto_router_marks_traced_results():
    from scrapy_calyprium.routing.auto import SpiderAutoRouter

    tracer = _tracer()
    router = SpiderAutoRouter.__new__(SpiderAutoRouter)
    router.tracer = tracer
    router.cache = mock.Mock()
    router._fetch_inner = mock.AsyncMock(side_effect=lambda *a, **k: _route_result(None))
    result = await router.fetch("https://a.example/", domain="a.example")
    (span,) = _spans(tracer)
    assert result.trace_id == span["trace_id"]

    router.tracer = None
    assert (await router.fetch("https://a.example/", domain="a.example")).trace_id is None


# -- Transport errors -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_transport_error_attempts_are_traced():
    tracer = _tracer()
    mw = _mimic(tracer)
    req = Request("https://a.example/", meta={"proxy": "http://gw:8080"})
    retry = await mw.process_exception(req, TCPTimedOutError(), None)
    assert retry is not None
    (span,) = _spans(tracer)
    assert span["status"] == "timeout"
    assert span["status_code"] == 0
    assert span["routing_method"] == "veil_proxy"
    assert "TCPTimedOutError" in span["error_message"]

    await mw.process_exception(req, ValueError("not a transport error"), None)
    assert len(_spans(tracer)) == 1


@pytest.mark.asyncio
async def test_mimic_without_tracer_still_retries():
    mw = _mimic()
    assert await mw.process_exception(Request("https://a.example/"), TCPTimedOutError(), None)


# -- Batching / bounded memory / Forge outages -------------------------------------


def test_buffer_is_bounded(monkeypatch):
    monkeypatch.setattr(rt, "MAX_BUFFER", 3)
    tracer = _tracer()
    for i in range(5):
        req = Request(f"https://a.example/{i}")
        tracer.response_received(_response(req), req)
    assert len(_spans(tracer)) == 3
    assert tracer.dropped == 2


def test_forge_unreachable_drops_batch_without_raising():
    tracer = _tracer()
    req = Request("https://a.example/")
    tracer.response_received(_response(req), req)
    with mock.patch.object(rt.httpx, "post", side_effect=httpx.ConnectError("down")) as post:
        tracer._flush()
    post.assert_called_once()
    assert post.call_args.args[0] == "http://forge/jobs/spiders/shop/runs/7/traces"
    assert len(post.call_args.kwargs["json"]["spans"]) == 1
    assert _spans(tracer) == []


def test_full_batch_wakes_flush_thread_without_posting_inline():
    tracer = _tracer()
    with mock.patch.object(rt.httpx, "post") as post:
        for i in range(rt.BATCH_SIZE):
            req = Request(f"https://a.example/{i}")
            tracer.response_received(_response(req), req)
        post.assert_not_called()
    assert tracer._wake.is_set()
