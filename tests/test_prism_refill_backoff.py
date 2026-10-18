"""PrismSitemapSpider: a failed refill backs off, and a domain Prism has not indexed is scanned,
waited for, and given up on with a clear reason.

Found in a fleet check: a 404 from /api/domains/{domain}/urls re-queued the refill at once, every
time: ~2,265 requests in 5 minutes, then the run's timeout with 0 items (267 of 269 Prism-sourced
spiders were in that state). One spider used ti.com while Prism had www.ti.com.
"""
from __future__ import annotations

import json
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, TextResponse
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TCPTimedOutError
from twisted.python.failure import Failure

from scrapy_calyprium.spiders import prism_sitemap
from scrapy_calyprium.spiders.prism_sitemap import PrismSitemapSpider
from tests._helpers import make_spider

PRISM = "http://prism.test"


class S(PrismSitemapSpider):
    name = "s"
    prism_domain = "ti.com"
    prism_path_prefix = "/product/"

    def parse_item(self, response):
        return iter(())


@pytest.fixture(autouse=True)
def _steady_jitter(monkeypatch):
    monkeypatch.setattr(prism_sitemap.random, "uniform", lambda a, b: b)


def _spider(**settings):
    sp = make_spider(S, {"PRISM_URL": PRISM, "CALYPRIUM_API_KEY": "clp_run_key", **settings},
                     batch_size=2)
    (refill,) = list(sp.start_requests())
    return sp, refill


def _fail(request, status=None, exc=None):
    if status is not None:
        failure = Failure(HttpError(TextResponse(request.url, status=status, request=request)))
    else:
        failure = Failure(exc or TCPTimedOutError())
    failure.request = request
    return failure


async def _errback(sp, failure):
    """Run the errback as Scrapy would; returns (requests, the delay it waited)."""
    with mock.patch.object(prism_sitemap._wait, "sleep", mock.AsyncMock()) as sleep:
        out = await sp._handle_prism_error(failure)
    delay = sleep.await_args.args[0] if sleep.await_count else 0.0
    return list(out), delay


def _domain(request):
    return urlparse(request.url).path.split("/")[3]


FULL_PAGE = ["http://ti.com/product/1", "http://ti.com/product/2"]  # batch_size=2


def _page(request, urls):
    body = json.dumps({"urls": urls, "total": len(urls)}).encode()
    return TextResponse(request.url, body=body, request=request)


# -- backoff ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_failed_refill_backs_off_doubling_to_a_minute():
    sp, refill = _spider()
    delays = []
    for _ in range(8):
        (refill,), delay = await _errback(sp, _fail(refill, exc=TCPTimedOutError()))
        assert refill.meta["_internal"] and refill.meta["_prism_offset"] == 0
        delays.append(delay)
    assert delays == [2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0]
    assert sp.crawler.stats.get_value("prism/refill_failures") == 8


@pytest.mark.asyncio
async def test_the_backoff_is_jittered_down_never_zero(monkeypatch):
    monkeypatch.setattr(prism_sitemap.random, "uniform", lambda a, b: a)
    sp, refill = _spider()
    _, delay = await _errback(sp, _fail(refill, status=500))
    assert delay == pytest.approx(1.5)


@pytest.mark.asyncio
async def test_a_good_page_resets_the_backoff():
    sp, refill = _spider()
    for _ in range(3):
        (refill,), _ = await _errback(sp, _fail(refill, status=503))
    list(sp._handle_prism_page(_page(refill, FULL_PAGE)))
    assert sp._prism_refill_failures == 0 and not sp._prism_exhausted
    _, delay = await _errback(sp, _fail(sp._make_refill_request(), status=503))
    assert delay == 2.0


# -- a domain Prism has not indexed ------------------------------------------------------


@pytest.mark.asyncio
async def test_not_indexed_tries_the_other_www_form_first():
    sp, refill = _spider()
    (probe,), delay = await _errback(sp, _fail(refill, status=404))
    assert delay == 0.0
    assert _domain(probe) == "www.ti.com" and probe.meta["_prism_alt"] == "www.ti.com"
    assert parse_qs(urlparse(probe.url).query)["path_prefix"] == ["/product/"]
    assert sp.crawler.stats.get_value("prism/not_indexed") == 1


@pytest.mark.asyncio
async def test_the_other_www_form_is_adopted_when_prism_has_it():
    sp, refill = _spider()
    (probe,), _ = await _errback(sp, _fail(refill, status=404))
    out = list(sp._handle_prism_page(_page(probe, ["https://www.ti.com/product/1"])))
    assert [r.url for r in out] == ["https://www.ti.com/product/1"]
    assert _domain(sp._make_refill_request()) == "www.ti.com"
    assert sp._prism_wait_deadline is None


@pytest.mark.asyncio
async def test_then_prism_is_asked_to_scan_once_with_the_runs_key():
    sp, refill = _spider()
    (probe,), _ = await _errback(sp, _fail(refill, status=404))
    (scan, poll), delay = await _errback(sp, _fail(probe, status=404))
    assert scan.method == "POST"
    assert scan.url == f"{PRISM}/api/sitemaps/fetch?domain=ti.com"
    assert json.loads(scan.body) == {"scan_type": "full", "force": False}
    assert scan.headers[b"Authorization"] == b"Bearer clp_run_key"
    assert scan.meta["_internal"] and scan.meta["handle_httpstatus_all"]
    assert _domain(poll) == "ti.com" and "_prism_alt" not in poll.meta
    assert delay > 0
    (poll2,), delay2 = await _errback(sp, _fail(poll, status=404))  # no second scan
    assert _domain(poll2) == "ti.com" and delay2 > delay


@pytest.mark.asyncio
@pytest.mark.parametrize("status,stat", [
    (200, "prism/scan_requested"), (202, "prism/scan_requested"), (409, "prism/scan_requested"),
    (401, "prism/scan_refused"),
])
async def test_the_scan_answer_is_logged_never_fatal(status, stat):
    sp, refill = _spider()
    scan = sp._make_scan_request("ti.com")
    assert sp._handle_prism_scan(TextResponse(scan.url, status=status, request=scan)) == []
    assert sp.crawler.stats.get_value(stat) == 1
    assert sp._handle_prism_scan_error(_fail(scan)) == []


@pytest.mark.asyncio
async def test_polling_gives_up_after_the_wait_and_closes_the_spider():
    sp, refill = _spider(PRISM_WAIT_FOR_SCAN_S=900)
    (probe,), _ = await _errback(sp, _fail(refill, status=404))
    (_, poll), _ = await _errback(sp, _fail(probe, status=404))
    sp._prism_wait_deadline -= 901  # the wait is over
    with pytest.raises(CloseSpider) as closed:
        await _errback(sp, _fail(poll, status=404))
    assert closed.value.reason == "prism_no_urls"
    assert sp._prism_exhausted
    assert sp.crawler.stats.get_value("prism/no_urls") == 1
    assert await sp._handle_prism_error(_fail(poll, status=404)) == []  # nothing more


@pytest.mark.asyncio
async def test_a_run_with_urls_only_stops_the_refill():
    sp, refill = _spider()
    list(sp._handle_prism_page(_page(refill, FULL_PAGE)))
    later = sp._make_refill_request()
    (scan, poll), _ = await _errback(sp, _fail(later, status=404))  # no www probe mid-run
    sp._prism_wait_deadline -= 10_000
    assert await sp._handle_prism_error(_fail(poll, status=404)) == []
    assert sp._prism_exhausted


@pytest.mark.asyncio
async def test_with_no_wait_the_scan_still_goes_out_before_giving_up():
    sp, refill = _spider(PRISM_WAIT_FOR_SCAN_S=0)
    (probe,), _ = await _errback(sp, _fail(refill, status=404))
    (scan, poll), _ = await _errback(sp, _fail(probe, status=404))
    assert scan.method == "POST"
    with pytest.raises(CloseSpider):
        await _errback(sp, _fail(poll, status=404))


def test_the_wait_ends_before_the_runs_timeout():
    sp, _ = _spider(PRISM_WAIT_FOR_SCAN_S=900, CLOSESPIDER_TIMEOUT=600)
    assert sp._prism_wait_for_scan_s() == 570
    sp2, _ = _spider()
    assert sp2._prism_wait_for_scan_s() == 900


@pytest.mark.parametrize("domain,alt", [
    ("ti.com", "www.ti.com"), ("www.ti.com", "ti.com"), ("WWW.Example.co.uk", "example.co.uk"),
    ("localhost", None),
])
def test_other_www(domain, alt):
    assert prism_sitemap._other_www(domain) == alt
