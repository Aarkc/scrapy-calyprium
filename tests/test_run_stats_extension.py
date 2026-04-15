"""Tests for CalypriumRunStats Scrapy extension.

Covers: signal counting, HTTP payload shape, no-op when run_number is
missing, and idempotent cumulative reporting.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scrapy_calyprium.extensions.run_stats import CalypriumRunStats


def _make_ext(run_number=42, slug="test-spider"):
    return CalypriumRunStats(
        forge_url="http://forge",
        service_secret="secret",
        user_id="user-1",
        spider_slug=slug,
        scrapyd_job_id="job-abc",
        run_number=run_number,
        interval=30.0,
    )


class _FakeResponse:
    def __init__(self, status, body=b""):
        self.status = status
        self.body = body
        self.headers = {}


class _FakeRequest:
    def __init__(self, meta=None):
        self.meta = meta or {}


class TestSignalCounting:
    def test_request_scheduled_increments_count(self):
        ext = _make_ext()
        for _ in range(5):
            ext.request_scheduled(_FakeRequest(), None)
        assert ext._request_count == 5

    def test_response_received_tracks_status_codes(self):
        ext = _make_ext()
        for status in [200, 200, 200, 403, 403, 503]:
            ext.response_received(
                _FakeResponse(status, b"x" * 100), _FakeRequest(), None,
            )
        assert ext._response_count == 6
        assert dict(ext._status_counts) == {"200": 3, "403": 2, "503": 1}
        assert ext._bytes_downloaded == 600

    def test_response_received_tracks_routing_method(self):
        ext = _make_ext()
        ext.response_received(
            _FakeResponse(200),
            _FakeRequest(meta={"calyprium_routing_method": "httpcloak_light"}),
            None,
        )
        ext.response_received(
            _FakeResponse(200),
            _FakeRequest(meta={"calyprium_routing_method": "solve_then_replay"}),
            None,
        )
        ext.response_received(
            _FakeResponse(200),
            _FakeRequest(meta={"calyprium_routing_method": "httpcloak_light"}),
            None,
        )
        assert dict(ext._routing_counts) == {
            "httpcloak_light": 2,
            "solve_then_replay": 1,
        }

    def test_item_scraped_increments_count(self):
        ext = _make_ext()
        for _ in range(3):
            ext.item_scraped({}, None, None)
        assert ext._item_count == 3


class TestFlush:
    def test_flush_posts_cumulative_payload(self):
        ext = _make_ext(run_number=7, slug="my-spider")
        # Simulate some activity
        ext._request_count = 100
        ext._response_count = 95
        ext._item_count = 40
        ext._bytes_downloaded = 1_500_000
        ext._status_counts.update({"200": 50, "403": 45})
        ext._routing_counts.update({"httpcloak_light": 80, "solve_then_replay": 15})

        with patch("scrapy_calyprium.extensions.run_stats.httpx.post") as mock_post:
            ext._flush()
        assert mock_post.called
        url = mock_post.call_args[0][0]
        assert url == "http://forge/jobs/spiders/my-spider/runs/7/stats"
        payload = mock_post.call_args[1]["json"]
        assert payload["request_count"] == 100
        assert payload["response_count"] == 95
        assert payload["item_count"] == 40
        assert payload["bytes_downloaded"] == 1_500_000
        assert payload["status_code_counts"] == {"200": 50, "403": 45}
        assert payload["routing_method_counts"] == {
            "httpcloak_light": 80, "solve_then_replay": 15,
        }
        headers = mock_post.call_args[1]["headers"]
        assert headers["X-Service-Secret"] == "secret"
        assert headers["X-User-Id"] == "user-1"

    def test_flush_noop_without_run_number(self):
        ext = _make_ext(run_number=None)
        with patch("scrapy_calyprium.extensions.run_stats.httpx.post") as mock_post:
            ext._flush()
        assert not mock_post.called

    def test_flush_swallows_http_errors(self):
        ext = _make_ext()
        with patch(
            "scrapy_calyprium.extensions.run_stats.httpx.post",
            side_effect=RuntimeError("network down"),
        ):
            # Must not raise — a failed report should never crash the spider
            ext._flush()


class TestFromCrawler:
    def test_disabled_when_run_number_missing(self):
        crawler = MagicMock()
        crawler.settings.get.return_value = ""
        crawler.settings.getfloat.return_value = 30.0
        with patch.dict("os.environ", {}, clear=False):
            ext = CalypriumRunStats.from_crawler(crawler)
        assert ext.run_number is None
        # Signals should NOT be connected when disabled
        crawler.signals.connect.assert_not_called()

    def test_enabled_reads_run_number_from_settings(self):
        crawler = MagicMock()
        settings = {}

        def _get(key, default=None):
            return settings.get(key, default)

        def _getfloat(key, default=0.0):
            v = settings.get(key)
            return float(v) if v is not None else default

        crawler.settings.get.side_effect = _get
        crawler.settings.getfloat.side_effect = _getfloat

        settings["SPIDER_RUN_NUMBER"] = "99"
        settings["RECRAWL_SPIDER_SLUG"] = "x"
        settings["RECRAWL_USER_ID"] = "u1"
        settings["FORGE_API_URL"] = "http://f"
        settings["FORGE_SERVICE_SECRET"] = "s"

        ext = CalypriumRunStats.from_crawler(crawler)
        assert ext.run_number == 99
        assert ext.spider_slug == "x"
        assert ext.user_id == "u1"
