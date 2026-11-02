"""What a crawl has going at once (scrapy_calyprium.concurrency): sampled every second by CalypriumRunStats' reporter,
reported to Forge with each window and logged every minute, so a run's concurrency can be read over time."""
import asyncio
from types import SimpleNamespace

from scrapy.settings import Settings
from scrapy.statscollectors import MemoryStatsCollector

from scrapy_calyprium.concurrency import SOLVES_IN_FLIGHT, ConcurrencySampler, describe
from scrapy_calyprium.extensions.run_stats import CalypriumRunStats
from scrapy_calyprium.routing.auto import SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import DomainCache


class _Settings(dict):
    def getint(self, key, default=0):
        return int(self.get(key, default))


class _Scheduler:
    def __init__(self, n):
        self.n = n

    def __len__(self):
        if self.n is None:
            raise RuntimeError("dictionary changed size during iteration")
        return self.n


def _crawler(active=(), queued=0, solves=0, limit=200):
    stats = SimpleNamespace(get_value=lambda key: solves if key == SOLVES_IN_FLIGHT else None)
    engine = SimpleNamespace(downloader=SimpleNamespace(active=set(active)),
                             slot=SimpleNamespace(scheduler=_Scheduler(queued)))
    return SimpleNamespace(engine=engine, stats=stats, settings=_Settings(CONCURRENT_REQUESTS=limit))


def test_a_window_has_the_latest_counts_the_average_and_the_peak():
    crawler = _crawler()
    s = ConcurrencySampler(crawler)
    s.watch("report")
    for n, q in ((4, 900), (8, 880), (6, 870)):
        crawler.engine.downloader.active = set(range(n))
        crawler.engine.slot.scheduler.n = q
        s.sample()
    w = s.window("report")
    assert w == {"in_flight": 6, "in_flight_avg": 6.0, "in_flight_max": 8, "queued": 870, "solves_in_flight": 0,
                 "solves_in_flight_max": 0, "concurrency_limit": 200}
    # the next window starts empty: with no new sample it repeats the latest counts
    assert s.window("report")["in_flight_max"] == 6


def test_each_name_keeps_its_own_window():
    crawler = _crawler(active=range(10))
    s = ConcurrencySampler(crawler)
    s.watch("report")
    s.watch("log")
    s.sample()
    s.window("report")                              # the 30 s report
    crawler.engine.downloader.active = set(range(2))
    s.sample()
    assert s.window("log")["in_flight_max"] == 10    # the minute's log still saw the peak
    assert s.window("report")["in_flight_max"] == 2


def test_a_queue_that_changes_while_counted_leaves_queued_unknown():
    s = ConcurrencySampler(_crawler(active=range(3), queued=None))
    s.watch("w")
    s.sample()
    w = s.window("w")
    assert w["in_flight"] == 3 and w["queued"] is None


def test_nothing_is_reported_before_the_engine_runs():
    s = ConcurrencySampler(SimpleNamespace(engine=None, stats=None, settings=_Settings()))
    s.sample()
    assert s.window("report") == {}


def test_the_router_counts_its_solves_in_progress():
    stats = MemoryStatsCollector(SimpleNamespace(settings=Settings()))
    seen = []

    class Solve:
        async def solve(self, **kwargs):
            seen.append(stats.get_value(SOLVES_IN_FLIGHT))
            return SimpleNamespace(success=False)

    router = SpiderAutoRouter(fetcher=SimpleNamespace(), cache=DomainCache(), solve_client=Solve(),
                              proxy_url="http://u:p@gw:8080", target_pool_size=1, refill_interval=0.0,
                              solve_parallel_solves=1, cold_start_burst=1, light_probe_interval=3600)
    router.stats = stats
    asyncio.run(router._solve(domain="www.shop.example"))
    assert seen == [1]                               # one in progress while it ran
    assert stats.get_value(SOLVES_IN_FLIGHT) == 0     # none after


def test_the_report_carries_the_window_and_the_log_line_reads_it():
    ext = CalypriumRunStats("http://forge", "", "u", "shop", "job", run_number=3, interval=30)
    crawler = _crawler(active=range(5), queued=1200, solves=2, limit=16)
    ext.sampler = ConcurrencySampler(crawler)
    ext.sampler.watch("report")
    ext.sampler.sample()
    posted = []
    ext.auth = SimpleNamespace(call=lambda f: posted.append(f))
    ext._flush()
    assert posted                                    # the POST went (its payload is checked below)
    w = ConcurrencySampler(crawler)
    w.sample()
    line = describe(w.window("log"), 60)
    assert line == ("Concurrency over the last 60 s: 5 requests in flight (average 5, peak 5 of 16 allowed), "
                    "1,200 queued, 2 solves in progress (peak 2)")


def test_the_payload_has_the_concurrency_fields(monkeypatch):
    ext = CalypriumRunStats("http://forge", "", "u", "shop", "job", run_number=3, interval=30)
    ext.sampler = ConcurrencySampler(_crawler(active=range(7), queued=40, solves=1, limit=32))
    ext.sampler.watch("report")
    ext.sampler.sample()
    sent = {}

    def post(url, json=None, headers=None, timeout=None):
        sent.update(json)
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr("scrapy_calyprium.extensions.run_stats.httpx.post", post)
    ext.auth = SimpleNamespace(call=lambda f: f({}))
    ext._flush()
    assert {k: sent[k] for k in ("in_flight", "in_flight_avg", "in_flight_max", "queued", "solves_in_flight",
                                 "solves_in_flight_max", "concurrency_limit")} == {
        "in_flight": 7, "in_flight_avg": 7.0, "in_flight_max": 7, "queued": 40, "solves_in_flight": 1,
        "solves_in_flight_max": 1, "concurrency_limit": 32}
