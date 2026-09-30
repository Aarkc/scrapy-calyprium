"""AAR-65: checkpoint the lowest fully-completed Prism offset, and wrap at the end.

Before: the offset advanced when a page was *fetched*, so a kill (e.g. the
3-hour deadline) skipped every queued-but-uncrawled URL; a max_urls stop still
advanced by the full page; a fully-fresh page skipped 50k unexamined URLs; and
once a run exhausted the corpus every later run started at the end and crawled
0 URLs.
"""
from __future__ import annotations

import json
from unittest import mock
from urllib.parse import parse_qs, urlparse

from scrapy.http import Request, TextResponse

from scrapy_calyprium.extensions.prism_checkpoint import PrismOffsetCheckpoint
from scrapy_calyprium.spiders.prism_sitemap import PrismSitemapSpider
from tests._helpers import make_spider


class S(PrismSitemapSpider):
    name = "s"
    prism_domain = "example.com"

    def parse_item(self, response):
        return iter(())


def _spider(start_offset=0, batch_size=5, max_urls=0):
    sp = make_spider(S, {}, batch_size=batch_size, max_urls=max_urls)
    sp._prism_next_offset = start_offset  # as PrismOffsetCheckpoint would
    (refill,) = list(sp.start_requests())
    return sp, refill


def _page(refill_request, urls):
    body = json.dumps({"urls": urls, "total": 999}).encode()
    return TextResponse(url=refill_request.url, body=body, request=refill_request)


def _handle(sp, refill_request, urls):
    out = list(sp._handle_prism_page(_page(refill_request, urls)))
    reqs = [r for r in out if not r.meta.get("_internal")]
    refills = [r for r in out if r.meta.get("_internal")]
    return reqs, refills


def _complete(sp, req):
    resp = TextResponse(url=req.url, body=b"", request=req)
    return list(sp._parse_and_maybe_refill(resp))


def _fail(sp, req):
    failure = mock.Mock()
    failure.request = req
    return list(sp._prism_errback(failure))


def _qs(request):
    return {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}


URLS = [f"http://example.com/p/{i}" for i in range(5)]


def test_checkpoint_waits_for_queued_urls_to_complete():
    sp, refill = _spider()
    reqs, _ = _handle(sp, refill, URLS)
    assert sp._prism_next_offset == 5        # fetch frontier moved on...
    assert sp._prism_checkpoint_offset == 0  # ...but nothing is complete yet
    for r in reqs[:4]:
        _complete(sp, r)
    assert sp._prism_checkpoint_offset == 0  # one URL still in flight
    _fail(sp, reqs[4])                       # a failed URL still completes
    assert sp._prism_checkpoint_offset == 5


def test_checkpoint_is_lowest_incomplete_page():
    sp, refill = _spider()
    page1, _ = _handle(sp, refill, URLS)
    refill2 = sp._make_refill_request()
    page2, _ = _handle(sp, refill2, [u + "b" for u in URLS])
    for r in page2:
        _complete(sp, r)
    _complete(sp, page1[0])
    assert sp._prism_checkpoint_offset == 0
    for r in page1[1:]:
        _complete(sp, r)
    assert sp._prism_checkpoint_offset == 10


def test_dupe_filtered_request_completes_its_page():
    sp, refill = _spider()
    reqs, _ = _handle(sp, refill, URLS)
    for r in reqs[:4]:
        _complete(sp, r)
    sp._on_request_dropped(reqs[4])
    assert sp._prism_checkpoint_offset == 5


def test_extension_saves_completed_offset_not_frontier():
    sp, refill = _spider()
    _handle(sp, refill, URLS)
    ext = PrismOffsetCheckpoint("http://forge", "", "u", "s", api_key="clp_k")
    ext._spider = sp
    assert ext._current_offset() == 0


def test_max_urls_checkpoints_only_what_was_yielded():
    sp, refill = _spider(max_urls=3)
    reqs, _ = _handle(sp, refill, URLS)
    assert len(reqs) == 3
    assert sp._prism_exhausted
    assert sp._prism_next_offset == 3  # was 5: the 2 unyielded URLs were skipped
    for r in reqs:
        _complete(sp, r)
    assert sp._prism_checkpoint_offset == 3


def test_fully_fresh_page_widens_instead_of_skipping():
    sp, refill = _spider()
    with mock.patch.object(sp, "_filter_fresh_urls", return_value=[]):
        reqs, refills = _handle(sp, refill, URLS)
    assert reqs == []
    (nxt,) = refills
    assert _qs(nxt)["offset"] == "5"   # was 50000: 49,995 URLs never examined
    assert _qs(nxt)["limit"] == "10"
    assert sp._prism_checkpoint_offset == 5


def test_exhausted_resume_wraps_to_zero_and_stops_at_start():
    sp, refill = _spider(start_offset=3)
    reqs, refills = _handle(sp, refill, [])  # corpus ends before offset 3
    assert reqs == [] and not sp._prism_exhausted
    (wrap,) = refills
    assert _qs(wrap)["offset"] == "0"
    reqs, _ = _handle(sp, wrap, URLS)
    assert [r.url for r in reqs] == URLS[:3]  # stops where this run began
    assert sp._prism_exhausted
    for r in reqs:
        _complete(sp, r)
    assert sp._prism_checkpoint_offset == 3


def test_short_last_page_wraps_when_resumed():
    sp, refill = _spider(start_offset=2)
    reqs, _ = _handle(sp, refill, URLS[:2])  # short page = end of corpus
    assert not sp._prism_exhausted
    assert sp._prism_next_offset == 0 and sp._prism_stop_at == 2
    for r in reqs:
        _complete(sp, r)
    assert sp._prism_checkpoint_offset == 0


def test_no_wrap_when_run_started_at_zero():
    sp, refill = _spider()
    _handle(sp, refill, URLS[:2])
    assert sp._prism_exhausted


def test_extension_resets_before_saving_a_lower_offset():
    ext = PrismOffsetCheckpoint("http://forge", "", "u", "s", api_key="clp_k")
    ext._last_saved = 1000
    calls = []
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.delete.side_effect = lambda url, headers: calls.append(("DELETE", url)) or mock.Mock(status_code=204)
    client.post.side_effect = lambda url, headers, json: calls.append(("POST", json["offset"])) or mock.Mock(status_code=200)
    with mock.patch("scrapy_calyprium.extensions.prism_checkpoint.httpx.Client", return_value=client):
        assert ext._persist("s", 40)
        assert ext._persist("s", 80)
    assert calls == [
        ("DELETE", "http://forge/spiders/s/checkpoint"),
        ("POST", 40),
        ("POST", 80),
    ]
    assert ext._last_saved == 80
