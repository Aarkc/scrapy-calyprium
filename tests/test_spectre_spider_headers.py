"""A header the spider sets on its Request wins over the device fingerprint's.

SpectreMiddleware replaced every fingerprint header, Referer included: heremaps' search key
answers only a request that names wego.here.com as its Referer, and got https://www.google.com/
(2026-10-22; the spider switched Spectre off). The browser identity (User-Agent, client hints)
stays the fingerprint's, and Scrapy's automatic Referer (the parent page) is still replaced, as
before.
"""
from __future__ import annotations

from scrapy import Spider
from scrapy.http import HtmlResponse, Request
from scrapy.spidermiddlewares.referer import RefererMiddleware

from scrapy_calyprium._headers import SCRAPY_REFERER, scrapy_referer
from scrapy_calyprium.middleware.spectre import FINGERPRINT_HEADERS, SpectreMiddleware
from scrapy_calyprium.middleware.tracing import CalypriumTraceMiddleware
from tests._helpers import make_crawler

GOOGLE = b"https://www.google.com/"
HOME = "https://wego.here.com/"
FP = {
    "fingerprint": {"id": "fp1", "name": "chrome-153"},
    "headers": {
        "User-Agent": "Spectre-UA",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="153"',
        "Referer": "https://www.google.com/",
    },
}
FP2 = {
    "fingerprint": {"id": "fp2", "name": "chrome-155"},
    "headers": {**FP["headers"], "User-Agent": "Spectre-UA-2", "Accept-Language": "de-DE,de;q=0.9"},
}


def _spectre(fp=FP):
    mw = SpectreMiddleware(service_url="http://spectre", api_key="k")
    mw._cached_fingerprint = fp
    return mw


def _followed(request, parent_url):
    """``request`` as a callback of ``parent_url`` yields it: Scrapy's RefererMiddleware, then
    CalypriumTraceMiddleware (spider output runs from the spider outwards)."""
    response = HtmlResponse(parent_url, body=b"<html></html>", request=Request(parent_url))
    referer = RefererMiddleware(make_crawler().settings)
    out = list(referer.process_spider_output(response, [request], Spider("s")))
    return list(CalypriumTraceMiddleware().process_spider_output(response, out, Spider("s")))[0]


def test_spider_referer_wins_over_the_fingerprint():
    req = Request("https://discover.search.hereapi.com/v1/discover?q=x", headers={"Referer": HOME})
    _spectre().process_request(req, None)
    assert req.headers[b"Referer"] == HOME.encode()
    assert req.headers[b"User-Agent"] == b"Spectre-UA"


def test_spider_referer_wins_on_a_followed_request():
    """heremaps yields its search from the script's callback, naming the site's home page."""
    req = _followed(
        Request("https://discover.search.hereapi.com/v1/discover?q=x", headers={"Referer": HOME}),
        "https://wego.here.com/static/app.js")
    _spectre().process_request(req, None)
    assert req.headers[b"Referer"] == HOME.encode()


def test_spider_referer_survives_a_retry():
    mw = _spectre()
    req = Request("https://example.com/api", headers={"Referer": "https://example.com/page"})
    mw.process_request(req, None)
    retry = req.replace(dont_filter=True)  # the headers and meta of the first pass
    mw.process_request(retry, None)
    assert retry.headers[b"Referer"] == b"https://example.com/page"


def test_scrapy_automatic_referer_is_still_replaced():
    req = _followed(Request("https://example.com/product/1"), "https://example.com/category")
    assert req.headers[b"Referer"] == b"https://example.com/category"  # Scrapy set it
    assert scrapy_referer(req)
    _spectre().process_request(req, None)
    assert req.headers[b"Referer"] == GOOGLE


def test_spider_accept_wins_but_not_the_browser_identity():
    req = Request("https://example.com/api", headers={
        "Accept": "application/json", "User-Agent": "MyBot/1.0", "sec-ch-ua": '"Mine";v="1"',
        "X-Api-Key": "k1"})
    _spectre().process_request(req, None)
    assert req.headers[b"Accept"] == b"application/json"
    assert req.headers[b"X-Api-Key"] == b"k1"
    assert req.headers[b"User-Agent"] == b"Spectre-UA"
    assert req.headers[b"sec-ch-ua"] == b'"Chromium";v="153"'


def test_a_retry_takes_the_current_fingerprint_not_the_last():
    """A retry carries the last pass's fingerprint values: they are the fingerprint's, by value."""
    mw = _spectre()
    req = Request("https://example.com/a")
    mw.process_request(req, None)
    mw._cached_fingerprint = FP2  # rotated after a block
    retry = req.replace(dont_filter=True)
    mw.process_request(retry, None)
    assert retry.headers[b"Accept-Language"] == b"de-DE,de;q=0.9"
    assert retry.headers[b"User-Agent"] == b"Spectre-UA-2"


def test_copied_meta_does_not_decide_what_the_spider_set():
    mw = _spectre()
    first = Request("https://example.com/a")
    mw.process_request(first, None)
    # a spider passing meta on (meta=response.meta) to a request with its own Referer
    second = Request("https://example.com/b", headers={"Referer": "https://example.com/a"},
                     meta=dict(first.meta))
    mw.process_request(second, None)
    assert second.headers[b"Referer"] == b"https://example.com/a"


def test_fingerprint_headers_are_named_for_the_local_fetch():
    req = Request("https://example.com/api", headers={"Referer": HOME, "Accept": "application/json"})
    _spectre().process_request(req, None)
    marked = set(req.meta[FINGERPRINT_HEADERS])
    assert {"user-agent", "accept-language", "sec-ch-ua"} <= marked
    assert "referer" not in marked and "accept" not in marked


def test_trace_middleware_keeps_the_referer_scrapy_gave():
    req = _followed(Request("https://example.com/b"), "https://example.com/a#top")
    assert req.meta[SCRAPY_REFERER] == "https://example.com/a"  # fragment stripped, as Scrapy does
    assert scrapy_referer(req)
    # one copied from another request is replaced by what Scrapy gives this one
    again = _followed(Request("https://example.com/c", meta=dict(req.meta)), "https://example.com/b")
    assert again.meta[SCRAPY_REFERER] == "https://example.com/b"
    start = list(CalypriumTraceMiddleware().process_start_requests(
        [Request("https://example.com/", meta={SCRAPY_REFERER: "https://x/"})], Spider("s")))[0]
    assert SCRAPY_REFERER not in start.meta


def test_referrer_policy_origin_is_scrapys_too():
    """A page's Referrer-Policy makes Scrapy send only the origin: that is still Scrapy's."""
    parent = "https://example.com/category?page=2"
    response = HtmlResponse(parent, body=b"", request=Request(parent),
                            headers={"Referrer-Policy": "strict-origin-when-cross-origin"})
    req = Request("https://api.example.net/search")
    out = list(RefererMiddleware(make_crawler().settings).process_spider_output(
        response, [req], Spider("s")))
    req = list(CalypriumTraceMiddleware().process_spider_output(response, out, Spider("s")))[0]
    assert req.headers[b"Referer"] == b"https://example.com/"
    _spectre().process_request(req, None)
    assert req.headers[b"Referer"] == GOOGLE


def test_no_referer_is_scrapys_when_its_middleware_is_off():
    crawler = make_crawler({"REFERER_ENABLED": False})
    response = HtmlResponse("https://example.com/a", body=b"", request=Request("https://example.com/a"))
    req = Request("https://example.com/b", headers={"Referer": "https://example.com/a"})
    req = list(CalypriumTraceMiddleware(crawler).process_spider_output(
        response, [req], Spider("s")))[0]
    assert not scrapy_referer(req)
    _spectre().process_request(req, None)
    assert req.headers[b"Referer"] == b"https://example.com/a"
