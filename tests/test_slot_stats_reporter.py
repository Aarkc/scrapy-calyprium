"""Tests for the SlotStatsReporter background task."""
from __future__ import annotations

import pytest

from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.slot_stats import SlotStatsReporter


@pytest.fixture
def cache_with_traffic():
    cache = DomainCache()
    slot_a = cache.set_cookies_from_solve(
        "example.com",
        cookies=[{"name": "cf_clearance", "value": "x"}],
        user_agent="UA",
        proxy_session_id="sess-A",
    )
    slot_b = cache.set_cookies_from_solve(
        "example.com",
        cookies=[{"name": "cf_clearance", "value": "y"}],
        user_agent="UA",
        proxy_session_id="sess-B",
    )
    # Simulate some activity
    for _ in range(10):
        cache.record_request("example.com", slot_a.slot_id)
    for _ in range(5):
        cache.record_request("example.com", slot_b.slot_id)
    cache.record_slot_success("example.com", slot_a.slot_id)
    cache.record_slot_success("example.com", slot_a.slot_id)
    return cache


def test_build_batch_includes_all_slots(cache_with_traffic):
    reporter = SlotStatsReporter(
        cache=cache_with_traffic,
        service_url="http://mimic.test",
        spider="digikey_fast",
    )
    batch = reporter._build_batch()
    assert batch["spider"] == "digikey_fast"
    assert len(batch["entries"]) == 2

    by_session = {e["proxy_session_id"]: e for e in batch["entries"]}
    assert "sess-A" in by_session
    assert "sess-B" in by_session
    assert by_session["sess-A"]["domain"] == "example.com"
    assert by_session["sess-A"]["rpm"] == 10
    assert by_session["sess-A"]["successes"] == 2
    assert by_session["sess-B"]["rpm"] == 5
    assert by_session["sess-B"]["successes"] == 0


def test_build_batch_computes_deltas(cache_with_traffic):
    reporter = SlotStatsReporter(
        cache=cache_with_traffic,
        service_url="http://mimic.test",
    )
    # First report — initial counts
    first = reporter._build_batch()
    sess_a = next(e for e in first["entries"] if e["proxy_session_id"] == "sess-A")
    assert sess_a["successes"] == 2

    # No new activity — deltas should be 0
    second = reporter._build_batch()
    sess_a = next(e for e in second["entries"] if e["proxy_session_id"] == "sess-A")
    assert sess_a["successes"] == 0

    # Add more successes
    for _ in range(3):
        cache_with_traffic.record_slot_success("example.com", "ignored")  # noop
    slot_a = cache_with_traffic.get("example.com").slots[0]
    slot_a.success_count += 5

    third = reporter._build_batch()
    sess_a = next(e for e in third["entries"] if e["proxy_session_id"] == "sess-A")
    assert sess_a["successes"] == 5


def test_build_batch_skips_non_cookie_domains():
    cache = DomainCache()
    cache.set_light("example.com")  # not a cookies entry
    reporter = SlotStatsReporter(
        cache=cache,
        service_url="http://mimic.test",
    )
    batch = reporter._build_batch()
    assert batch["entries"] == []


def test_build_batch_includes_learned_cap():
    cache = DomainCache()
    slot = cache.set_cookies_from_solve(
        "example.com",
        cookies=[{"name": "c", "value": "x"}],
        user_agent="UA",
        proxy_session_id="sess",
    )
    entry = cache.get("example.com")
    entry.learned_rpm_cap = 47.5

    reporter = SlotStatsReporter(
        cache=cache,
        service_url="http://mimic.test",
    )
    batch = reporter._build_batch()
    assert batch["entries"][0]["learned_rpm_cap"] == 47.5


def test_headers_include_service_secret_and_user():
    reporter = SlotStatsReporter(
        cache=DomainCache(),
        service_url="http://mimic.test",
        api_key="caly_key",
        service_secret="topsecret",
        user_id="user-123",
    )
    h = reporter._headers()
    assert h["X-API-Key"] == "caly_key"
    assert h["Authorization"] == "Bearer caly_key"
    assert h["X-Service-Secret"] == "topsecret"
    assert h["X-User-Id"] == "user-123"
    assert h["X-Service-Name"] == "scrapy-calyprium"
