"""Tests for the target-throughput proactive refill + configurable cookie TTL.

The steady-state cookie-pool refill used to fire one solve at a time, so after
any dip the pool decayed to a low-throughput equilibrium it could never climb
out of. It now tops the in-flight solves up to solve_parallel_solves toward
target_pool_size, so the pool returns to (and holds) the target.
"""
from __future__ import annotations

import time

from scrapy_calyprium.routing import domain_cache as dc
from scrapy_calyprium.routing.auto import SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import DomainCache, CookieSlot


def _router(target, budget):
    dc.configure(max_slots=1000)  # don't let the per-domain cap clip the pool
    r = SpiderAutoRouter.__new__(SpiderAutoRouter)
    r.cache = DomainCache()
    r.target_pool_size = target
    r.solve_parallel_solves = budget
    r.cold_start_burst = 30
    r.refill_interval = 1.0
    r._refill_in_flight = {}
    r._last_refill_check = {}
    r._cold_start_done = {"example.com": True}  # force the steady-state path
    return r


def _seed(cache, n):
    for i in range(n):
        cache.set_cookies_from_solve(
            "example.com",
            cookies=[{"name": "cf_clearance", "value": str(i)}],
            user_agent="UA", proxy_session_id=f"s{i}",
        )


def _count_fired(router, monkeypatch):
    fired = []

    def fake_create_task(coro):
        coro.close()  # don't actually run the solve
        fired.append(1)
        return object()

    monkeypatch.setattr("asyncio.create_task", fake_create_task)
    router._ensure_refill_task("example.com")
    return len(fired)


def test_steady_state_refill_uses_parallel_budget(monkeypatch):
    r = _router(target=10, budget=6)
    _seed(r.cache, 3)  # live=3, need 7 more, budget 6
    assert _count_fired(r, monkeypatch) == 6
    assert r._refill_in_flight["example.com"] == 6


def test_refill_does_not_overshoot_target(monkeypatch):
    r = _router(target=10, budget=6)
    _seed(r.cache, 8)  # live=8, need only 2 even though budget is 6
    assert _count_fired(r, monkeypatch) == 2


def test_no_refill_when_pool_at_target(monkeypatch):
    r = _router(target=10, budget=6)
    _seed(r.cache, 10)
    assert _count_fired(r, monkeypatch) == 0


def test_refill_respects_in_flight(monkeypatch):
    r = _router(target=10, budget=6)
    _seed(r.cache, 3)
    r._refill_in_flight["example.com"] = 4   # 4 already solving
    # budget 6 - 4 in-flight = at most 2 more
    assert _count_fired(r, monkeypatch) == 2


def test_configure_cookie_ttl_extends_expiry():
    slot = CookieSlot(slot_id="s", cookies=[], user_agent="UA", proxy_session_id="p")
    slot.created_at = time.time() - 2000  # 2000s old
    try:
        dc.configure(cookie_ttl=1800)   # 30 min -> expired
        assert slot.is_expired is True
        dc.configure(cookie_ttl=3600)   # 60 min -> still valid
        assert slot.is_expired is False
    finally:
        dc.configure(cookie_ttl=1800)   # restore default
