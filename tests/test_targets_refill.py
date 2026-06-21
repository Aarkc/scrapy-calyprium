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
    spider._targets_cursor = 10_000
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
    s.crawler = _FakeCrawler(_FakeScheduler(10_000))
    with mock.patch.object(s, "_fetch_targets_batch", return_value=["x"]) as m:
        out = list(s._targets_errback(None))
    assert out == []          # no refill while the queue is full
    assert m.call_count == 0
    assert s._urls_responded == 1  # ...but the failure is still counted


# ---------------------------------------------------------------------------
# The real fix: _pending_count reads the engine's scheduler depth, which can't
# leak. Failures that bypass BOTH callback and errback (httpcloak "cookie replay
# infra error" drops) inflated the old yielded-minus-responded estimate without
# bound, pinning it above _REFILL_THRESHOLD so refill never fired again — every
# run stalled after the ~200k startup burst with millions of targets pending.
# ---------------------------------------------------------------------------


class _FakeScheduler:
    def __init__(self, depth):
        self._depth = depth

    def __len__(self):
        return self._depth


class _FakeCrawler:
    def __init__(self, scheduler):
        self.engine = mock.Mock()
        self.engine.slot.scheduler = scheduler


def test_pending_count_prefers_scheduler_depth():
    # Bookkeeping would read 200k (leaked); the real queue holds 42.
    s = _make_refill_spider(yielded=200_000, responded=0)
    s.crawler = _FakeCrawler(_FakeScheduler(42))
    assert s._pending_count == 42


def test_pending_count_falls_back_without_engine():
    # No crawler/engine reachable -> fall back to the bookkeeping estimate.
    s = _make_refill_spider(yielded=5000, responded=2000)
    assert s._pending_count == 3000


def _mk_resp(urls, next_cursor=None, total=None):
    r = mock.Mock()
    r.raise_for_status = mock.Mock()
    body = {"urls": list(urls)}
    if next_cursor is not None:
        body["next_cursor"] = next_cursor
    if total is not None:
        body["total_pending"] = total
    r.json = mock.Mock(return_value=body)
    return r


def test_keyset_sends_after_id_cursor():
    # The fetch must page by `after_id` (keyset), not `offset` — offset over a
    # shrinking pending set skips rows and ends the run early at ~one window.
    s = _make_targets_spider()
    s._targets_cursor = 12345
    captured = {}
    def fake_get(url, params=None, **kw):
        captured.update(params or {})
        return _mk_resp([f"http://x/{i}" for i in range(5000)], next_cursor=99999)
    with mock.patch("requests.get", side_effect=fake_get):
        urls = s._fetch_targets_batch()
    assert captured.get("after_id") == 12345   # cursor sent
    assert "offset" not in captured             # NOT offset pagination
    assert len(urls) == 5000


def test_keyset_advances_cursor_to_next_cursor():
    s = _make_targets_spider()
    s._targets_cursor = 0
    with mock.patch("requests.get", return_value=_mk_resp(["http://x/%d" % i for i in range(5000)], next_cursor=88888)):
        s._fetch_targets_batch()
    assert s._targets_cursor == 88888           # advanced to server cursor, not +len
    assert s._targets_exhausted is False


def test_keyset_short_batch_exhausts():
    s = _make_targets_spider()
    with mock.patch("requests.get", return_value=_mk_resp(["http://x/1"], next_cursor=5)):
        urls = s._fetch_targets_batch()
    assert len(urls) == 1
    assert s._targets_exhausted is True         # short page => end of backlog


def test_keyset_fallback_when_server_omits_next_cursor():
    # Legacy server (no next_cursor) must not pin the cursor and re-fetch forever.
    s = _make_targets_spider()
    s._targets_cursor = 100
    with mock.patch("requests.get", return_value=_mk_resp(["http://x/%d" % i for i in range(5000)])):
        s._fetch_targets_batch()
    assert s._targets_cursor == 5100            # fell back to += len(urls)


def test_refill_resumes_despite_leaked_bookkeeping():
    # Production bug: 15,653 requests failed without acking, so
    # yielded-minus-responded is pinned at 15,653 (>> threshold). With the
    # scheduler actually drained, refill MUST still fire.
    s = _make_refill_spider(yielded=215_653, responded=200_000)  # estimate=15,653
    s.crawler = _FakeCrawler(_FakeScheduler(10))  # real queue near-empty
    assert s._pending_count == 10
    batch = [f"http://x/{i}" for i in range(5000)]
    with mock.patch.object(s, "_fetch_targets_batch", return_value=batch):
        out = list(s._maybe_refill_targets())
    assert len(out) == 5000  # refill fired despite the leaked estimate
