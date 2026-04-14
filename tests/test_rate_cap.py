"""AAR-17 follow-up: tests for the spider-side per-slot rate cap learner.

Verifies that DomainCache + DomainEntry track per-slot RPM in a rolling
window, learn a safe per-slot RPM cap from observed blocks, and that the
cap recovers gradually when traffic stays clean.

Mirrors the server-side mimic.routing.domain_cache tests so the two
implementations stay in sync.
"""
from __future__ import annotations

import time

import pytest

from scrapy_calyprium.routing.domain_cache import (
    DomainCache,
    DomainEntry,
    CookieSlot,
)


# ---------------------------------------------------------------------------
# Per-slot RPM window
# ---------------------------------------------------------------------------


class TestSlotRPM:
    def test_zero_for_new_slot(self):
        slot = CookieSlot(
            slot_id="s1", cookies=[], user_agent="UA",
            proxy_session_id="p1",
        )
        assert slot.requests_per_minute() == 0

    def test_records_recent_requests(self):
        slot = CookieSlot(
            slot_id="s1", cookies=[], user_agent="UA",
            proxy_session_id="p1",
        )
        for _ in range(20):
            slot.record_request()
        assert slot.requests_per_minute() == 20

    def test_window_expires_old_requests(self):
        slot = CookieSlot(
            slot_id="s1", cookies=[], user_agent="UA",
            proxy_session_id="p1",
        )
        # Inject 15 timestamps from 90s ago (outside window)
        old = time.time() - 90
        slot._request_times = [old + i * 0.1 for i in range(15)]
        # And 5 from now
        for _ in range(5):
            slot.record_request()
        assert slot.requests_per_minute() == 5


# ---------------------------------------------------------------------------
# Adaptive cap learning
# ---------------------------------------------------------------------------


class TestRateCapLearning:
    def _seeded_cache(self):
        cache = DomainCache()
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "cf_clearance", "value": "x"}],
            user_agent="UA",
            proxy_session_id="sess-1",
        )
        return cache

    def test_block_at_high_rpm_learns_cap(self):
        cache = self._seeded_cache()
        entry = cache.get("example.com")
        slot = entry.slots[0]

        # Simulate the slot having done 100 RPM
        for _ in range(100):
            slot.record_request()
        assert slot.requests_per_minute() == 100

        cache.record_slot_failure("example.com", slot.slot_id, status_code=403)

        # Cap should be 70% of 100 = 70 RPM. After MAX_SLOT_FAILURES=1
        # the slot is dead so cache.get() returns None; inspect the
        # internal entry to verify the cap was still learned.
        entry = cache._entries.get("example.com")
        assert entry is not None
        assert entry.learned_rpm_cap is not None
        assert entry.learned_rpm_cap == pytest.approx(70.0, abs=0.1)

    def test_block_below_5_rpm_does_not_learn(self):
        cache = self._seeded_cache()
        entry = cache.get("example.com")
        slot = entry.slots[0]
        for _ in range(3):
            slot.record_request()
        cache.record_slot_failure("example.com", slot.slot_id, status_code=403)
        entry = cache._entries.get("example.com")
        assert entry.learned_rpm_cap is None

    def test_infra_failure_does_not_update_cap(self):
        cache = self._seeded_cache()
        entry = cache.get("example.com")
        slot = entry.slots[0]
        for _ in range(50):
            slot.record_request()
        cache.record_slot_failure("example.com", slot.slot_id, status_code=None)
        entry = cache._entries.get("example.com")
        assert entry.learned_rpm_cap is None  # AAR-14: infra doesn't blame domain

    def test_cap_uses_median_of_block_rpms(self):
        cache = self._seeded_cache()
        entry = cache.get("example.com")
        slot = entry.slots[0]

        # Simulate three blocks at different rates
        for rate in [50, 100, 80]:
            slot._request_times = [time.time() - i * 0.5 for i in range(rate)]
            cache.record_slot_failure(
                "example.com", slot.slot_id, status_code=403,
            )
            slot.fail_count = 0  # reset so it stays live
            slot.block_count = 0

        entry = cache.get("example.com")
        # Median of [50, 80, 100] is 80; cap = 80 * 0.7 = 56
        assert entry.learned_rpm_cap == pytest.approx(56.0, abs=0.1)


# ---------------------------------------------------------------------------
# next_slot honors the cap
# ---------------------------------------------------------------------------


class TestNextSlotRespectsRateCap:
    def test_prefers_under_cap_slots(self):
        cache = DomainCache()
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "c", "value": "1"}],
            user_agent="UA",
            proxy_session_id="A",
        )
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "c", "value": "2"}],
            user_agent="UA",
            proxy_session_id="B",
        )
        entry = cache.get("example.com")
        # Set a learned cap and load slot A above it
        entry.learned_rpm_cap = 30.0
        slot_a, slot_b = entry.slots[0], entry.slots[1]
        for _ in range(50):
            slot_a.record_request()  # over cap
        for _ in range(5):
            slot_b.record_request()  # under cap

        chosen = entry.next_slot()
        assert chosen.proxy_session_id == "B"

    def test_falls_back_to_least_loaded_when_all_over_cap(self):
        cache = DomainCache()
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "c", "value": "1"}],
            user_agent="UA",
            proxy_session_id="A",
        )
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "c", "value": "2"}],
            user_agent="UA",
            proxy_session_id="B",
        )
        entry = cache.get("example.com")
        entry.learned_rpm_cap = 10.0
        for _ in range(50):
            entry.slots[0].record_request()
        for _ in range(30):
            entry.slots[1].record_request()

        chosen = entry.next_slot()
        # Both over cap; least-loaded is slot B
        assert chosen.proxy_session_id == "B"


# ---------------------------------------------------------------------------
# DomainCache.record_request public API
# ---------------------------------------------------------------------------


class TestRecordRequest:
    def test_increments_slot_rpm(self):
        cache = DomainCache()
        slot = cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "c", "value": "x"}],
            user_agent="UA",
            proxy_session_id="A",
        )
        for _ in range(10):
            cache.record_request("example.com", slot.slot_id)
        assert slot.requests_per_minute() == 10

    def test_unknown_slot_is_noop(self):
        cache = DomainCache()
        cache.record_request("nope.com", "ghost-slot")  # should not raise


# ---------------------------------------------------------------------------
# Cap recovery (maybe_raise_cap)
# ---------------------------------------------------------------------------


class TestSilentFailureFeedback:
    """Spider-side feedback channel for silent block detection."""

    def _make_router(self):
        from scrapy_calyprium.routing.auto import SpiderAutoRouter
        from scrapy_calyprium.routing.local_fetch import LocalFetcher
        from scrapy_calyprium.routing.solve_client import SolveClient

        cache = DomainCache()
        slot = cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "cf_clearance", "value": "x"}],
            user_agent="UA",
            proxy_session_id="sess-A",
        )
        # Stand up a router with stub fetcher/solve client we won't actually use
        class _StubFetcher:
            backend = "stub"
            async def fetch(self, *a, **kw):
                raise NotImplementedError
        router = SpiderAutoRouter.__new__(SpiderAutoRouter)
        router.fetcher = _StubFetcher()
        router.cache = cache
        router.solve_client = None
        router.proxy_url = None
        router._solve_locks = {}
        return router, cache, slot

    def test_silent_failure_increments_slot_failure(self):
        router, cache, slot = self._make_router()
        # Simulate the slot having seen real traffic
        for _ in range(50):
            slot.record_request()
        assert slot.fail_count == 0

        router.report_silent_failure("example.com", slot.slot_id, reason="no_data")

        # Slot should have a failure recorded AND the rate cap should have learned
        assert slot.fail_count == 1
        entry = cache._entries.get("example.com")
        assert entry.learned_rpm_cap is not None
        # 50 RPM at block time -> cap = 50 * 0.7 = 35
        assert entry.learned_rpm_cap == pytest.approx(35.0, abs=0.1)

    def test_silent_failure_with_unknown_slot_is_noop(self):
        router, cache, slot = self._make_router()
        router.report_silent_failure("example.com", "ghost-slot", reason="x")
        # No exception, no slot mutated
        assert slot.fail_count == 0

    def test_silent_failure_with_no_slot_id_is_noop(self):
        router, cache, slot = self._make_router()
        router.report_silent_failure("example.com", None, reason="x")
        assert slot.fail_count == 0


class TestCapRecovery:
    def test_no_raise_within_block_cooldown(self):
        cache = DomainCache()
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "c", "value": "x"}],
            user_agent="UA",
            proxy_session_id="A",
        )
        entry = cache.get("example.com")
        entry.learned_rpm_cap = 50.0
        entry._last_block_time = time.time() - 60  # only 60s ago
        original = entry.learned_rpm_cap
        entry.maybe_raise_cap()
        assert entry.learned_rpm_cap == original

    def test_raises_cap_after_5min_clean(self):
        cache = DomainCache()
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "c", "value": "x"}],
            user_agent="UA",
            proxy_session_id="A",
        )
        entry = cache.get("example.com")
        entry.learned_rpm_cap = 50.0
        entry._last_block_time = time.time() - 600  # 10 min ago
        entry._last_cap_raise_time = 0  # never raised
        entry.maybe_raise_cap()
        assert entry.learned_rpm_cap == pytest.approx(55.0, abs=0.1)
