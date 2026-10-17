"""One trace per Scrapy request: the trace middleware gives each request and each download attempt its ids, the
tracer sends the attempt (priced, as before) with them and the request and parse spans to Forge's /spans, and the
SDK's calls to Mimic and Tessera carry `traceparent`."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest import mock

import pytest
from scrapy.http import HtmlResponse, Request

from scrapy_calyprium import _trace
from scrapy_calyprium.extensions.request_tracer import CalypriumRequestTracer
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.tracing import CalypriumTraceMiddleware
from scrapy_calyprium.routing.solve_client import SolveClient
from tests._helpers import make_crawler

RUN_SETTINGS = {
    "FORGE_API_URL": "http://forge", "CALYPRIUM_API_KEY": "clp_k", "RECRAWL_SPIDER_SLUG": "shop",
    "RECRAWL_USER_ID": "u-1", "SPIDER_RUN_NUMBER": 7,
}


def _pair():
    crawler = make_crawler(RUN_SETTINGS)
    tracer = CalypriumRequestTracer.from_crawler(crawler)
    crawler.extensions = SimpleNamespace(middlewares=[tracer])
    return CalypriumTraceMiddleware.from_crawler(crawler), tracer


def _parse(response):
    yield {"name": "a"}
    yield Request("https://shop.example/p/2")
    yield {"name": "b"}


def test_a_request_keeps_its_trace_and_gets_a_span_per_attempt():
    mw, _ = _pair()
    request = next(mw.process_start_requests(iter([Request("https://shop.example/")]), None))
    trace = request.meta[_trace.TRACE_KEY]
    assert len(trace["trace_id"]) == 32 and len(trace["span_id"]) == 16
    mw.process_request(request, None)
    first = _trace.attempt_ids(request)
    retry = request.replace(dont_filter=True)                    # Scrapy copies meta
    mw.process_request(retry, None)
    second = _trace.attempt_ids(retry)
    assert first[0] == second[0] == trace["trace_id"] and first[2] == second[2] == trace["span_id"]
    assert first[1] != second[1] and retry.meta["calyprium_attempts"] == 2
    assert _trace.headers(retry) == {"traceparent": f"00-{trace['trace_id']}-{second[1]}-01"}


def test_parsing_a_page_sends_the_parse_and_request_spans():
    mw, tracer = _pair()
    request = next(mw.process_start_requests(iter([Request("https://shop.example/", callback=_parse)]), None))
    mw.process_request(request, None)
    request.meta["spectre_fingerprint_id"] = "fp_1"
    request.meta["spectre_fingerprint_hash"] = "f" * 64
    response = HtmlResponse(request.url, status=200, body=b"<html></html>", request=request)
    mw.process_spider_input(response, None)
    out = list(mw.process_spider_output(response, _parse(response), None))
    follow = next(x for x in out if isinstance(x, Request))
    trace = request.meta[_trace.TRACE_KEY]
    assert follow.meta[_trace.TRACE_KEY]["parent_trace_id"] == trace["trace_id"]
    assert follow.meta[_trace.TRACE_KEY]["trace_id"] != trace["trace_id"]
    spans = {s["name"]: s for s in tracer._spans}
    parse, root = spans["spider.parse"], spans["spider.request"]
    assert parse["trace_id"] == root["trace_id"] == trace["trace_id"]
    assert parse["parent_span_id"] == root["span_id"] == trace["span_id"] and root["parent_span_id"] == ""
    assert parse["attributes"] == {"spider.callback": "_parse", "spider.items": 2, "spider.requests": 1}
    a = root["attributes"]
    assert (a["url.full"], a["http.response.status_code"], a["spider.attempts"]) == ("https://shop.example/", 200, 1)
    assert a["spider.fingerprint_id"] == "fp_1" and a["spider.queue_wait_ms"] >= 0 and "error" not in root
    assert a["spider.fingerprint_hash"] == "f" * 64


def test_a_callback_that_raises_marks_both_spans_and_still_raises():
    mw, tracer = _pair()
    request = Request("https://shop.example/")
    _trace.request_trace(request)
    response = HtmlResponse(request.url, status=200, body=b"", request=request)

    def broken(_):
        yield {"x": 1}
        raise ValueError("no price")

    with pytest.raises(ValueError):
        list(mw.process_spider_output(response, broken(response), None))
    assert {s["name"]: s["error"] for s in tracer._spans} == {
        "spider.parse": "ValueError: no price", "spider.request": "ValueError: no price"}


def test_async_callbacks_are_traced_too():
    mw, tracer = _pair()
    request = Request("https://shop.example/")
    _trace.request_trace(request)
    response = HtmlResponse(request.url, status=200, body=b"", request=request)

    async def gen():
        yield {"a": 1}
        yield Request("https://shop.example/next")

    async def consume():
        return [x async for x in mw.process_spider_output_async(response, gen(), None)]

    out = asyncio.run(consume())
    assert len(out) == 2 and {s["name"] for s in tracer._spans} == {"spider.parse", "spider.request"}


def test_an_attempt_span_carries_its_trace_and_stays_priced_as_before():
    _, tracer = _pair()
    token = _trace.ATTEMPT.set(("a" * 32, "b" * 16, "c" * 16))
    try:
        tracer.record_span(trace_id="d" * 32, url="https://shop.example/", domain="shop.example", duration_ms=1500)
    finally:
        _trace.ATTEMPT.reset(token)
    span = tracer._buffer[-1]
    assert span["trace_id"] == "d" * 32                         # the priced request id, unchanged
    assert (span["otel_trace_id"], span["otel_span_id"], span["otel_parent_span_id"]) == ("a" * 32, "b" * 16, "c" * 16)
    assert span["otel_start"] < span["timestamp"]
    tracer.record_span(trace_id="e" * 32, url="https://shop.example/x", domain="shop.example")
    assert "otel_trace_id" not in tracer._buffer[-1]            # no attempt: as before


def test_request_spans_go_to_forge_spans_and_stop_on_an_older_forge():
    _, tracer = _pair()
    tracer.record_trace_span("spider.parse", "a" * 32, "b" * 16, "c" * 16, 1.0, 2.0, {"spider.items": 3, "x": None})
    posted = []

    def fake_post(url, json, headers, timeout):
        posted.append((url, json))
        return SimpleNamespace(status_code=404)

    with mock.patch("scrapy_calyprium.extensions.request_tracer.httpx.post", side_effect=fake_post):
        tracer._flush()
        tracer.record_trace_span("spider.parse", "a" * 32, "f" * 16, "c" * 16, 1.0, 2.0)
        tracer._flush()
    assert len(posted) == 1 and posted[0][0] == "http://forge/jobs/spiders/shop/runs/7/spans"
    assert posted[0][1]["spans"][0]["attributes"] == {"spider.items": 3}
    assert tracer._spans == [] and tracer._spans_unsupported


def test_calls_to_mimic_and_tessera_carry_the_attempt():
    crawler = make_crawler({"MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "clp_k"})
    mimic = MimicBrowserMiddleware.from_crawler(crawler)
    solve = SolveClient(service_url="http://mimic", api_key="clp_k")
    assert "traceparent" not in mimic._get_headers() and "traceparent" not in solve._build_headers()
    token = _trace.ATTEMPT.set(("a" * 32, "b" * 16, "c" * 16))
    try:
        assert mimic._get_headers()["traceparent"] == f"00-{'a' * 32}-{'b' * 16}-01"
        assert solve._build_headers()["traceparent"] == f"00-{'a' * 32}-{'b' * 16}-01"
    finally:
        _trace.ATTEMPT.reset(token)


def test_internal_requests_are_not_traced():
    mw, _ = _pair()
    request = Request("https://forge/internal", meta={"_internal": True})
    mw.process_request(request, None)
    assert _trace.ATTEMPT_KEY not in request.meta
