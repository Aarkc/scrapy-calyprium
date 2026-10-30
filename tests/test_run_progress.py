"""A run's progress (scrapy_calyprium.progress): set by a spider, sent by CalypriumRunStats, and reported by
PrismSitemapSpider from its place in Prism's URL list."""
from __future__ import annotations

import json
from unittest.mock import patch

from scrapy.http import TextResponse

from scrapy_calyprium import progress
from scrapy_calyprium.extensions.run_stats import CalypriumRunStats
from scrapy_calyprium.spiders.prism_sitemap import PrismSitemapSpider
from tests._helpers import make_crawler, make_spider


class S(PrismSitemapSpider):
    name = "s"
    prism_domain = "example.com"

    def parse_item(self, response):
        return iter(())


def _spider(start_offset=0, settings=None):
    sp = make_spider(S, settings or {}, batch_size=5)
    sp._prism_next_offset = start_offset
    (refill,) = list(sp.start_requests())
    return sp, refill


def _handle(sp, refill, urls, total=20):
    body = json.dumps({"urls": urls, "total": total}).encode()
    out = list(sp._handle_prism_page(TextResponse(url=refill.url, body=body, request=refill)))
    return [r for r in out if not r.meta.get("_internal")], [r for r in out if r.meta.get("_internal")]


def _complete(sp, reqs):
    for r in reqs:
        list(sp._parse_and_maybe_refill(TextResponse(url=r.url, body=b"", request=r)))


def _progress(sp):
    return progress.read(sp.crawler.stats)


URLS = [f"http://example.com/p/{i}" for i in range(5)]


def test_report_sets_the_three_stats_and_read_gives_them_back():
    crawler = make_crawler()
    assert progress.read(crawler.stats) == {"progress_done": None, "progress_left": None, "progress_unit": None}
    progress.report(crawler.stats, done=120, left=-3, unit="companies")
    assert progress.read(crawler.stats) == {"progress_done": 120, "progress_left": 0, "progress_unit": "companies"}


def test_a_spider_reports_through_its_crawler():
    sp, _ = _spider()
    progress.report(sp, done=4, left=6)
    assert _progress(sp) == {"progress_done": 4, "progress_left": 6, "progress_unit": "pages"}


def test_the_stats_report_carries_progress():
    ext = CalypriumRunStats("http://forge", "secret", "u", "s", "job", run_number=3)
    ext.stats = make_crawler().stats
    with patch("scrapy_calyprium.extensions.run_stats.httpx.post") as post:
        ext._flush()
    assert post.call_args[1]["json"]["progress_done"] is None
    progress.report(ext.stats, done=10, left=90, unit="urls")
    with patch("scrapy_calyprium.extensions.run_stats.httpx.post") as post:
        ext._flush()
    sent = post.call_args[1]["json"]
    assert (sent["progress_done"], sent["progress_left"], sent["progress_unit"]) == (10, 90, "urls")


def test_prism_progress_is_the_completed_place_in_the_list():
    sp, refill = _spider()
    assert _progress(sp)["progress_done"] is None  # no page yet: the list's length is unknown
    reqs, _ = _handle(sp, refill, URLS)
    assert _progress(sp) == {"progress_done": 0, "progress_left": 20, "progress_unit": "urls"}
    _complete(sp, reqs)
    assert _progress(sp)["progress_done"] == 5 and _progress(sp)["progress_left"] == 15


def test_a_resumed_run_counts_from_where_it_began_and_wraps_back_to_it():
    sp, refill = _spider(start_offset=15)
    reqs, _ = _handle(sp, refill, URLS)  # 15..20: the end of the list
    _complete(sp, reqs)
    # The pass: 5 to the end, then 0..15 after the wrap.
    assert _progress(sp)["progress_done"] == 5 and _progress(sp)["progress_left"] == 15
    _, (wrap,) = _handle(sp, sp._make_refill_request(), [])  # past the end: back to 0
    assert sp._prism_wrapped
    assert _progress(sp)["progress_done"] == 5 and _progress(sp)["progress_left"] == 15
    reqs, _ = _handle(sp, wrap, URLS)
    _complete(sp, reqs)
    assert _progress(sp)["progress_done"] == 10 and _progress(sp)["progress_left"] == 10


def test_without_wrapping_the_pass_ends_at_the_end_of_the_list():
    sp, refill = _spider(start_offset=15, settings={"PRISM_WRAP_ON_EXHAUST": False})
    reqs, _ = _handle(sp, refill, URLS)
    _complete(sp, reqs)
    assert _progress(sp)["progress_done"] == 5 and _progress(sp)["progress_left"] == 0
