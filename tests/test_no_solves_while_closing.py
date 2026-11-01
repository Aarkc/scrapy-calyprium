"""Once the crawl is closing (CloseSpider's time limits, an idle close, the runner's stop), no solve starts.

A close waits for the requests in flight, and a request kept solving: partselect's 30-minute first run began
closing at 22:47 and was still open at 23:17 on 270 s solve timeouts (2026-11-01). Scrapy sends spider_closed
only once those requests end, so the router asks the engine (MimicBrowserMiddleware._crawl_closing) before each solve.
"""
import asyncio
from types import SimpleNamespace

import pytest

from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.routing.auto import SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.solve_client import SolveError


class _Solve:
    def __init__(self):
        self.calls = 0

    async def solve(self, **kwargs):
        self.calls += 1
        raise AssertionError("no solve may start while the crawl is closing")


def _router(solve):
    return SpiderAutoRouter(fetcher=SimpleNamespace(), cache=DomainCache(), solve_client=solve,
                            proxy_url="http://u:p@gw:8080", target_pool_size=1, refill_interval=0.0,
                            solve_parallel_solves=1, cold_start_burst=1, light_probe_interval=3600)


def test_no_solve_starts_once_the_crawl_is_closing():
    solve = _Solve()
    router = _router(solve)
    router.closing = lambda: True
    assert asyncio.run(router._try_one_solve("www.shop.example", "https://www.shop.example/p/1")) is None
    with pytest.raises(SolveError):
        asyncio.run(router._solve(domain="www.shop.example"))
    router._ensure_refill_task("www.shop.example")   # no refill either
    assert solve.calls == 0


def test_solves_run_while_the_crawl_is_open():
    class Ok(_Solve):
        async def solve(self, **kwargs):
            self.calls += 1
            return SimpleNamespace(success=False)

    solve = Ok()
    router = _router(solve)
    assert router.closing() is False                  # the default: open
    assert asyncio.run(router._try_one_solve("www.shop.example", "https://www.shop.example/p/1")) is None
    assert solve.calls == 1


def test_the_middleware_reads_the_engines_closing_slot():
    mw = MimicBrowserMiddleware.__new__(MimicBrowserMiddleware)
    mw.crawler = SimpleNamespace(engine=SimpleNamespace(slot=SimpleNamespace(closing=None)))
    assert mw._crawl_closing() is False
    mw.crawler.engine.slot.closing = object()          # the Deferred Slot.close() sets
    assert mw._crawl_closing() is True
    mw.crawler = SimpleNamespace(engine=None)         # before the engine runs
    assert mw._crawl_closing() is False
