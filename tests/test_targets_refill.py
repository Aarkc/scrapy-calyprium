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
