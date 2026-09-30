"""AAR-64: middleware reliability.

1. No User-Agent (and a 10s reactor stall per request) when Spectre is down.
2. An idle browser session held for every run.
3. One failure tore down the shared client and escalated the whole run.
4. Transport errors were never retried (RetryMiddleware is disabled).
5. Blocking network I/O on the reactor thread.
"""
from __future__ import annotations

import asyncio
from unittest import mock

import pytest
from scrapy.http import HtmlResponse, Request
from twisted.internet import defer
from twisted.internet.error import TCPTimedOutError

from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.spectre import SpectreMiddleware
from tests._helpers import make_crawler

FP = {
    "fingerprint": {"id": "fp1", "name": "chrome-win"},
    "headers": {"User-Agent": "Spectre-UA", "Accept-Language": "en-US"},
}


class Deferreds:
    """Stand-in for deferToThread that records calls and lets the test fire them."""

    def __init__(self):
        self.calls = []

    def __call__(self, fn, *args):
        d = defer.Deferred()
        self.calls.append((fn, args, d))
        return d


def _spectre(**kw):
    mw = SpectreMiddleware(service_url="http://spectre", api_key="k", **kw)
    mw._run_blocking = Deferreds()
    mw._now = lambda: 1000.0
    return mw


# -- 1. Spectre ---------------------------------------------------------------


def test_spectre_down_uses_fallback_ua_and_never_blocks():
    mw = _spectre()
    with mock.patch.object(mw, "_resolve_fingerprint") as resolve:
        req = Request("https://example.com/a")
        mw.process_request(req, None)
        resolve.assert_not_called()  # was a synchronous 10s call on the reactor
    assert req.headers[b"User-Agent"] == mw.fallback_user_agent.encode()
    assert len(mw._run_blocking.calls) == 1  # resolve started in the background


def test_spectre_failure_is_cached_with_backoff():
    mw = _spectre()
    mw.process_request(Request("https://example.com/a"), None)
    mw._run_blocking.calls[0][2].errback(RuntimeError("spectre down"))
    for _ in range(5):
        mw.process_request(Request("https://example.com/b"), None)
    assert len(mw._run_blocking.calls) == 1  # backing off, not hammering
    mw._now = lambda: 1000.0 + SpectreMiddleware.MIN_BACKOFF + 1
    mw.process_request(Request("https://example.com/c"), None)
    assert len(mw._run_blocking.calls) == 2
    mw._run_blocking.calls[1][2].errback(RuntimeError("still down"))
    assert mw._backoff == SpectreMiddleware.MIN_BACKOFF * 4  # doubled twice


def test_spectre_fingerprint_applied_once_resolved():
    mw = _spectre()
    mw.process_request(Request("https://example.com/a"), None)
    mw._run_blocking.calls[0][2].callback(FP)
    req = Request("https://example.com/b")
    mw.process_request(req, None)
    assert req.headers[b"User-Agent"] == b"Spectre-UA"
    assert req.meta["spectre_fingerprint_id"] == "fp1"


def test_spectre_block_rotates_in_background():
    mw = _spectre()
    mw._cached_fingerprint = FP
    req = Request("https://example.com/a")
    resp = HtmlResponse(req.url, status=403, body=b"<title>Just a moment...</title>",
                        headers={"Server": "cloudflare", "cf-ray": "x"}, request=req)
    mw.process_response(req, resp, None)
    assert len(mw._run_blocking.calls) == 1   # new identity requested, not inline
    assert mw._cached_fingerprint is FP       # requests keep a real UA meanwhile


# -- 2/3. Mimic: lazy session, no global teardown, per-domain escalation ---------


def _mimic(settings=None):
    crawler = make_crawler({
        "MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "k",
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
        },
        **(settings or {}),
    })
    mw = MimicBrowserMiddleware.from_crawler(crawler)
    client = mock.AsyncMock()
    client.is_closed = False
    mw._client = client
    return mw, client


def _session_response():
    r = mock.Mock()
    r.json.return_value = {"session_id": "sess-1", "worker": "w"}
    r.raise_for_status = mock.Mock()
    return r


@pytest.mark.asyncio
async def test_spider_opened_does_not_create_a_session():
    mw, client = _mimic({"MIMIC_ALL_REQUESTS": True})
    await mw.spider_opened(mock.Mock())
    client.post.assert_not_called()
    assert mw.session_id is None


@pytest.mark.asyncio
async def test_session_created_lazily_once_under_concurrency():
    mw, client = _mimic()

    async def slow_post(*a, **kw):
        await asyncio.sleep(0)
        return _session_response()

    client.post.side_effect = slow_post
    ids = await asyncio.gather(*[mw._ensure_session(None) for _ in range(5)])
    assert ids == ["sess-1"] * 5
    assert client.post.call_count == 1


@pytest.mark.asyncio
async def test_failures_do_not_tear_down_shared_client():
    mw, client = _mimic({"MIMIC_ALL_REQUESTS": True})
    mw.session_id = "sess-1"
    client.post.side_effect = RuntimeError("mimic 502")
    for i in range(3):
        assert await mw.process_request(Request(f"https://example.com/{i}"), None) is None
    client.aclose.assert_not_called()          # other coroutines still use it
    assert mw._client is client
    assert mw.session_id is None               # dead session dropped...
    client.delete.assert_awaited_once()        # ...best-effort
    assert mw.stealth_level == "moderate"      # no run-wide escalation


@pytest.mark.asyncio
async def test_block_escalates_only_that_domain_and_decays():
    mw, _ = _mimic()
    req = Request("https://blocked.example/a", meta={"mimic_browser": True})
    resp = HtmlResponse(req.url, status=403, body=b"<title>Just a moment...</title>",
                        headers={"Server": "cloudflare", "cf-ray": "x"}, request=req)
    with mock.patch("scrapy_calyprium.middleware.mimic.time.monotonic", return_value=100.0):
        await mw.process_response(req, resp, None)
        assert mw._stealth_for("blocked.example") == "maximum"
        assert mw._stealth_for("other.example") == "moderate"
    assert mw.stealth_level == "moderate"
    with mock.patch("scrapy_calyprium.middleware.mimic.time.monotonic",
                    return_value=100.0 + mw.escalation_ttl + 1):
        assert mw._stealth_for("blocked.example") == "moderate"


@pytest.mark.asyncio
async def test_escalated_domain_fetches_with_maximum_stealth():
    mw, _ = _mimic()
    mw._escalated["blocked.example"] = float("inf")
    resp = mock.Mock()
    resp.json.return_value = {"html": "<html></html>"}
    resp.raise_for_status = mock.Mock()
    client = mock.AsyncMock()
    client.post.return_value = resp
    await mw._fetch_auto(client, Request("https://blocked.example/x"))
    assert client.post.call_args.kwargs["json"]["stealth_level"] == "maximum"


# -- 4. transport retry ------------------------------------------------------------


@pytest.mark.asyncio
async def test_transport_errors_retried_up_to_cap():
    mw, _ = _mimic({"MIMIC_TRANSPORT_RETRIES": 2})
    req = Request("https://example.com/a")
    r1 = await mw.process_exception(req, TCPTimedOutError(), None)
    assert isinstance(r1, Request) and r1.dont_filter
    assert r1.meta["mimic_transport_retries"] == 1
    r2 = await mw.process_exception(r1, TCPTimedOutError(), None)
    assert r2.meta["mimic_transport_retries"] == 2
    assert await mw.process_exception(r2, TCPTimedOutError(), None) is None
    assert mw.crawler.stats.get_value("mimic/transport_retry/count") == 2


@pytest.mark.asyncio
async def test_non_transport_errors_not_retried():
    mw, _ = _mimic()
    assert await mw.process_exception(Request("https://e.com"), ValueError("x"), None) is None


def test_no_double_retry_when_retry_middleware_enabled():
    crawler = make_crawler({"MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "k"})
    assert MimicBrowserMiddleware.from_crawler(crawler).transport_retries == 0


# -- 5. blocking I/O off the reactor -----------------------------------------------


def test_tracer_full_buffer_does_not_post_on_caller_thread():
    from scrapy_calyprium.extensions import request_tracer as rt

    tracer = rt.CalypriumRequestTracer("http://forge", "", "u", "s", 1, api_key="k")
    with mock.patch.object(rt.httpx, "post") as post:
        for i in range(rt.BATCH_SIZE):
            tracer.record_span(trace_id=str(i), url="u", domain="d")
        post.assert_not_called()
    assert tracer._wake.is_set()


@pytest.mark.parametrize("cls_name", ["TargetDiscoveryPipeline", "TargetCompletionPipeline"])
def test_targets_flush_runs_off_reactor(cls_name):
    from scrapy_calyprium.pipelines import targets

    cls = getattr(targets, cls_name)
    if cls_name == "TargetDiscoveryPipeline":
        p = cls("http://forge", "k", "u", "t", "s", {"link": "doc"}, {}, batch_size=1)
        item = {"url": "http://a", "link": "http://b"}
    else:
        p = cls("http://forge", "k", "u", "s", batch_size=1)
        item = {"url": "http://a"}
    runner = Deferreds()
    p._run_blocking = runner
    with mock.patch("httpx.post") as post:
        p.process_item(item, None)
        post.assert_not_called()
    assert len(runner.calls) == 1 and runner.calls[0][0] == p._post
