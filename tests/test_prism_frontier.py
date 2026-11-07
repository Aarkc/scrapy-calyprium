"""A PrismSitemapSpider whose queue is its frontier (scrapy_calyprium.frontier): Prism's list only feeds discovery.
A page is done once its URLs are handed to the frontier (the checkpoint moves past it at once), Forge's freshness
filter is not asked (the frontier knows what was fetched and when), the walk stops after its share of pages for
the run (the next run carries on from the checkpoint), and progress is the frontier's to report."""
from __future__ import annotations

from unittest import mock

from tests._helpers import make_spider
from tests.test_prism_checkpoint_offset import S, URLS, _complete, _handle, _qs

FRONTIER = {"SCHEDULER": "scrapy_calyprium.frontier.FrontierScheduler",
            "CALYPRIUM_FRONTIER_URL": "http://frontier.test", "PRISM_FRONTIER_WALK_PAGES": 2,
            "RECRAWL_TRACKING_ENABLED": True}


def _spider(start_offset=0, settings=FRONTIER):
    sp = make_spider(S, settings, batch_size=5)
    sp._prism_next_offset = start_offset
    (refill,) = list(sp.start_requests())
    return sp, refill


def test_a_page_handed_to_the_frontier_is_done():
    sp, refill = _spider()
    with mock.patch.object(sp, "_filter_fresh_urls") as fresh:
        reqs, _ = _handle(sp, refill, URLS)
    fresh.assert_not_called()  # the frontier knows what was fetched
    assert [r.url for r in reqs] == URLS and all(r.meta["_prism_page"] == 0 for r in reqs)
    assert sp._prism_checkpoint_offset == 5 and not sp._prism_open_pages
    assert sp.crawler.stats.get_value("calyprium/progress/done") is None  # the scheduler reports progress


def test_the_walk_stops_after_its_pages_and_the_next_run_carries_on():
    sp, refill = _spider()
    reqs, _ = _handle(sp, refill, URLS)
    assert not sp._prism_exhausted
    more = _complete(sp, reqs[0])  # a fetched page's callback asks for the next Prism page
    (nxt,) = [r for r in more if r.meta.get("_internal")]
    assert _qs(nxt)["offset"] == "5" and _qs(nxt)["limit"] == "5"
    _handle(sp, nxt, [f"http://example.com/q/{i}" for i in range(5)])
    assert sp._prism_exhausted and sp._prism_checkpoint_offset == 10  # two pages: this run's share
    sp2, refill2 = _spider(start_offset=10)
    assert _qs(refill2)["offset"] == "10"


def test_without_the_frontier_nothing_changes():
    sp, refill = _spider(settings={"PRISM_FRONTIER_WALK_PAGES": 2})
    reqs, _ = _handle(sp, refill, URLS)
    assert sp._prism_checkpoint_offset == 0 and sp._prism_open_pages == {0: 5}


def test_a_run_from_another_source_does_not_refill_for_a_leased_prism_request():
    """Found in a fleet check (maxbotix_parts, 11-07): a run started from its sitemap:// source leased a request a
    prism:// run had left with _parse_and_maybe_refill, and the refill raised AttributeError on the unset source."""
    from scrapy.http import HtmlResponse, Request

    sp = make_spider(S, FRONTIER, batch_size=5, url_source="http://example.com/sitemap.xml")
    list(sp.start_requests())
    sp._prism_next_offset = 145  # the checkpoint resumes the offset whatever the source
    request = Request("http://example.com/p/2", callback=sp._parse_and_maybe_refill)
    out = list(sp._parse_and_maybe_refill(HtmlResponse(request.url, body=b"<html></html>", request=request)))
    assert out == [] and not sp._refill_in_flight
