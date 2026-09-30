"""AAR-59: S3BatchPipeline must not wedge on odd items or silently lose batches.

Before: ``json.dumps`` ran outside the try after ``_batch_id`` was bumped, so a
single ``datetime.date`` made every later item (and the close flush) raise; and
an upload error cleared the buffer with no stat, while RecrawlTrackingPipeline
still marked those URLs fresh.
"""
from __future__ import annotations

import datetime
import decimal
import json
import os
from unittest import mock

import pytest
import scrapy
from scrapy.exceptions import DropItem
from twisted.internet import defer

from scrapy_calyprium.pipelines import s3_batch
from scrapy_calyprium.pipelines.recrawl import RecrawlTrackingPipeline
from scrapy_calyprium.pipelines.s3_batch import S3BatchPipeline
from tests._helpers import make_crawler


class Part(scrapy.Item):
    sku = scrapy.Field()
    price = scrapy.Field()


class FakeS3:
    """put_object fails ``fail_times`` times (like a 500 during a forge deploy)."""

    def __init__(self, fail_times=0):
        self.fail_times = fail_times
        self.calls = 0
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentLength, ContentType):
        self.calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("An error occurred (500) when calling PutObject")
        self.objects[Key] = Body.read()

    def lines(self):
        out = []
        for key in sorted(self.objects):
            out += [json.loads(l) for l in self.objects[key].decode().splitlines()]
        return out


def _sync(fn, *args, **kwargs):
    return defer.maybeDeferred(fn, *args, **kwargs)


def make_pipeline(tmp_path, fake, crawler=None, batch_size=3, attempts=3):
    crawler = crawler or make_crawler({})
    p = S3BatchPipeline(
        endpoint_url="http://s3", access_key="k", secret_key="k",
        region_name="us-east-1", bucket="b", batch_size=batch_size,
        path_template="{user_id}/{spider}/runs/{run_number}/batch_{batch_id}.jl",
        user_id="u", spider_name="s", run_number="1",
        upload_attempts=attempts, retry_backoff=1.0,
        spill_dir=str(tmp_path / "spill"),
    )
    p.crawler = crawler
    p._client = fake
    p._sleep = mock.Mock()
    p._run_blocking = _sync
    return p


def _close(p, spider=None):
    d = p.close_spider(spider)
    results = []
    d.addBoth(results.append)
    assert results and results[0] is None, results
    return p


def _items():
    return [
        {"url": "http://x/1", "n": 1},
        {"url": "http://x/2", "d": datetime.date(2026, 9, 30)},
        {"url": "http://x/3", "dt": datetime.datetime(2026, 9, 30, 12, 0)},
        {"url": "http://x/4", "price": decimal.Decimal("1.10")},
        {"url": "http://x/5", "part": Part(sku="A1", price=decimal.Decimal("2")),
         "tags": {"a"}},
        {"url": "http://x/6", "raw": b"bytes\xff"},
    ]


def test_all_six_odd_items_are_stored(tmp_path):
    fake = FakeS3()
    p = make_pipeline(tmp_path, fake)
    for item in _items():
        p.process_item(item, None)
    _close(p)
    lines = fake.lines()
    assert len(lines) == 6
    assert lines[1]["d"] == "2026-09-30"
    assert lines[3]["price"] == "1.10"
    assert lines[4]["part"] == {"sku": "A1", "price": "2"}
    assert lines[4]["tags"] == ["a"]
    assert p.crawler.stats.get_value("s3batch/items_written") == 6
    assert p.crawler.stats.get_value("s3batch/lost_items") is None


def test_unserializable_item_is_dropped_and_counted_without_wedging(tmp_path):
    fake = FakeS3()
    p = make_pipeline(tmp_path, fake)
    p.process_item({"url": "http://x/1"}, None)
    with pytest.raises(DropItem):
        p.process_item({"url": "http://x/bad", "obj": object()}, None)
    p.process_item({"url": "http://x/2"}, None)
    p.process_item({"url": "http://x/3"}, None)
    p.process_item({"url": "http://x/4"}, None)
    _close(p)
    assert [l["url"] for l in fake.lines()] == [f"http://x/{i}" for i in (1, 2, 3, 4)]
    assert p.crawler.stats.get_value("s3batch/dropped_items") == 1


def test_process_item_returns_deferred_with_item_when_flushing(tmp_path):
    p = make_pipeline(tmp_path, FakeS3(), batch_size=1)
    item = {"url": "http://x/1"}
    out = []
    p.process_item(item, None).addCallback(out.append)
    assert out == [item]


def test_transient_500s_are_retried_nothing_lost(tmp_path):
    fake = FakeS3(fail_times=2)  # ~ a few seconds of 500s, within the retry budget
    p = make_pipeline(tmp_path, fake, attempts=3)
    for item in _items():
        p.process_item(item, None)
    _close(p)
    assert len(fake.lines()) == 6
    assert p._sleep.call_args_list == [mock.call(1.0), mock.call(2.0)]  # backoff
    assert p.crawler.stats.get_value("s3batch/lost_items") is None


def test_outage_spills_to_disk_and_recovers_later(tmp_path):
    # First batch exhausts its retries (3 attempts), then S3 comes back.
    fake = FakeS3(fail_times=3)
    p = make_pipeline(tmp_path, fake, attempts=3)
    for item in _items()[:3]:
        p.process_item(item, None)
    assert len(p._spilled) == 1 and os.path.exists(p._spilled[0][0])
    spill_path = p._spilled[0][0]
    for item in _items()[3:]:
        p.process_item(item, None)  # success drains the spill too
    _close(p)
    assert len(fake.lines()) == 6
    assert not p._spilled and not os.path.exists(spill_path)
    assert p.crawler.stats.get_value("s3batch/upload_failures") == 1
    assert p.crawler.stats.get_value("s3batch/lost_items") is None


def test_permanent_failure_counts_lost_items_and_fails_the_run(tmp_path):
    fake = FakeS3(fail_times=10_000)
    p = make_pipeline(tmp_path, fake, attempts=2)
    for item in _items()[:4]:
        p.process_item(item, None)
    with mock.patch.object(s3_batch, "_exit_nonzero_at_exit") as exit_hook:
        _close(p)
    assert p.crawler.stats.get_value("s3batch/lost_items") == 4
    exit_hook.assert_called_once()


def test_fail_on_loss_can_be_disabled(tmp_path):
    p = make_pipeline(tmp_path, FakeS3(fail_times=10_000), attempts=1)
    p.fail_on_loss = False
    p.process_item({"url": "http://x/1"}, None)
    with mock.patch.object(s3_batch, "_exit_nonzero_at_exit") as exit_hook:
        _close(p)
    exit_hook.assert_not_called()
    assert p.crawler.stats.get_value("s3batch/lost_items") == 1


# -- freshness only after persistence ---------------------------------------


def _pair(tmp_path, fake):
    crawler = make_crawler({
        "RECRAWL_TRACKING_ENABLED": True,
        "CALYPRIUM_API_KEY": "clp_k",
        "RECRAWL_SPIDER_SLUG": "s",
        "RECRAWL_BATCH_SIZE": 1000,
        "ITEM_PIPELINES": {
            "scrapy_calyprium.pipelines.s3_batch.S3BatchPipeline": 100,
            "scrapy_calyprium.pipelines.recrawl.RecrawlTrackingPipeline": 300,
        },
    })
    s3 = make_pipeline(tmp_path, fake, crawler=crawler, attempts=1)
    recrawl = RecrawlTrackingPipeline.from_crawler(crawler)
    recrawl._run_blocking = _sync
    reported = []
    recrawl._post = lambda batch: reported.extend(r["url"] for r in batch)
    return s3, recrawl, reported


def _run_items(s3, recrawl, items):
    for item in items:
        s3.process_item(item, None)
        recrawl.process_item(item, None)


def test_urls_marked_fresh_only_after_their_batch_is_stored(tmp_path):
    fake = FakeS3()
    s3, recrawl, reported = _pair(tmp_path, fake)
    assert recrawl.wait_for_persistence
    _run_items(s3, recrawl, _items()[:2])
    recrawl.close_spider(None)          # pipelines close in parallel...
    assert reported == []               # ...nothing stored yet -> nothing fresh
    _close(s3)
    assert reported == ["http://x/1", "http://x/2"]


def test_lost_batch_is_never_marked_fresh(tmp_path):
    fake = FakeS3(fail_times=10_000)
    s3, recrawl, reported = _pair(tmp_path, fake)
    _run_items(s3, recrawl, _items())
    with mock.patch.object(s3_batch, "_exit_nonzero_at_exit"):
        recrawl.close_spider(None)
        _close(s3)
    assert reported == []
    assert s3.crawler.stats.get_value("s3batch/lost_items") == 6


def test_recrawl_without_s3batch_reports_as_items_pass(tmp_path):
    crawler = make_crawler({
        "RECRAWL_TRACKING_ENABLED": True, "CALYPRIUM_API_KEY": "clp_k",
        "ITEM_PIPELINES": {"scrapy_calyprium.pipelines.recrawl.RecrawlTrackingPipeline": 300},
    })
    recrawl = RecrawlTrackingPipeline.from_crawler(crawler)
    recrawl._run_blocking = _sync
    reported = []
    recrawl._post = lambda batch: reported.extend(r["url"] for r in batch)
    recrawl.process_item({"url": "http://x/1"}, None)
    recrawl.close_spider(None)
    assert not recrawl.wait_for_persistence
    assert reported == ["http://x/1"]
