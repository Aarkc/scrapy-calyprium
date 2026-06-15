"""Regression tests for the targets:// source's resilience to transient
Forge errors.

Bug: ``_fetch_targets_batch`` set ``_targets_exhausted = True`` on *any*
exception, so a single transient blip while fetching the next batch
permanently stopped refilling and ended a multi-day catch-up early.
"""
from __future__ import annotations

from unittest import mock

from scrapy_calyprium.spiders.prism_sitemap import (
    PrismSitemapSpider,
    _TARGETS_MAX_FETCH_FAILURES,
    _REFILL_THRESHOLD,
)


def _make_targets_spider():
    """A spider wired up for the targets:// path without running __init__."""
    spider = PrismSitemapSpider.__new__(PrismSitemapSpider)
    spider.batch_size = 5000
    spider._targets_forge_url = "http://forge"
    spider._targets_spider_slug = "slug"
    spider._targets_api_key = "secret"
    spider._targets_user_id = "user"
    spider._targets_type = "product"
    spider._targets_offset = 10_000
    spider._targets_exhausted = False
    spider._targets_fetch_failures = 0
    return spider


def test_transient_failure_does_not_exhaust():
    spider = _make_targets_spider()
    with mock.patch("requests.get", side_effect=ConnectionError("boom")):
        for n in range(_TARGETS_MAX_FETCH_FAILURES - 1):
            urls = spider._fetch_targets_batch()
            assert urls == []
            assert spider._targets_exhausted is False
            assert spider._targets_fetch_failures == n + 1


def test_sustained_failures_eventually_exhaust():
    spider = _make_targets_spider()
    with mock.patch("requests.get", side_effect=ConnectionError("boom")):
        for _ in range(_TARGETS_MAX_FETCH_FAILURES):
            spider._fetch_targets_batch()
    assert spider._targets_exhausted is True


def test_success_resets_failure_run():
    spider = _make_targets_spider()
    # A few failures, then a success — the failure run must reset so a later
    # blip doesn't compound toward exhaustion.
    with mock.patch("requests.get", side_effect=ConnectionError("boom")):
        spider._fetch_targets_batch()
        spider._fetch_targets_batch()
    assert spider._targets_fetch_failures == 2

    ok = mock.Mock()
    ok.raise_for_status = mock.Mock()
    ok.json = mock.Mock(return_value={
        "urls": ["http://x/%d" % i for i in range(5000)],
        "total_pending": 1_000_000,
    })
    with mock.patch("requests.get", return_value=ok):
        urls = spider._fetch_targets_batch()
    assert len(urls) == 5000
    assert spider._targets_fetch_failures == 0
    assert spider._targets_exhausted is False


def test_empty_response_still_exhausts():
    """A genuine empty result (no pending left) must still exhaust normally."""
    spider = _make_targets_spider()
    ok = mock.Mock()
    ok.raise_for_status = mock.Mock()
    ok.json = mock.Mock(return_value={"urls": [], "total_pending": 0})
    with mock.patch("requests.get", return_value=ok):
        urls = spider._fetch_targets_batch()
    assert urls == []
    assert spider._targets_exhausted is True


# ---------------------------------------------------------------------------
# Refill must count failed requests (errback), not just successes — otherwise
# accumulated errors pin _pending_count high and refill stops forever (a run
# died at ~25k of 8.5M after 5,837 errback'd requests).
# ---------------------------------------------------------------------------


def _make_refill_spider(yielded, responded):
    s = _make_targets_spider()
    s._urls_yielded = yielded
    s._urls_responded = responded
    s._refill_in_flight = False
    return s


def test_errback_counts_failed_request():
    s = _make_refill_spider(yielded=5000, responded=0)
    s._targets_exhausted = True  # isolate the counter from refill
    list(s._targets_errback(None))
    assert s._urls_responded == 1


def test_errback_drives_refill_when_queue_drains():
    # pending = yielded - responded, starting just above the refill threshold
    s = _make_refill_spider(yielded=_REFILL_THRESHOLD + 2, responded=0)
    batch = [f"http://x/{i}" for i in range(5000)]
    out = []
    with mock.patch.object(s, "_fetch_targets_batch", return_value=batch):
        for _ in range(3):  # three failed requests drain the queue below threshold
            out += list(s._targets_errback(None))
    assert len(out) == 5000  # refill fired from the errback path
    assert s._urls_responded == 3
    assert out[0].callback == s._parse_and_maybe_refill_targets
    assert out[0].errback == s._targets_errback


def test_refill_noop_while_queue_full_but_still_counts():
    s = _make_refill_spider(yielded=10_000, responded=0)  # pending >> threshold
    with mock.patch.object(s, "_fetch_targets_batch", return_value=["x"]) as m:
        out = list(s._targets_errback(None))
    assert out == []          # no refill while the queue is full
    assert m.call_count == 0
    assert s._urls_responded == 1  # ...but the failure is still counted
