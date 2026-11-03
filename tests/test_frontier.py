"""FrontierScheduler (scrapy_calyprium/frontier.py) against an in-memory frontier: requests go to the frontier as
they are found and come back rebuilt; what is meant to be fetched anyway stays in the run; fetched pages are
acknowledged with when they are due again; a closing run releases what it never started; it stops asking once
nothing is due; a failing frontier is retried. A real crawl against the real service is checked in calyprium."""
from __future__ import annotations

import json

import pytest
import scrapy
from scrapy import FormRequest, Request
from scrapy.http import HtmlResponse
from twisted.internet import defer

from scrapy_calyprium import frontier
from scrapy_calyprium.frontier import FP_KEY, FrontierScheduler
from tests._helpers import make_crawler

SETTINGS = {"SCHEDULER": "scrapy_calyprium.frontier.FrontierScheduler",
            "CALYPRIUM_FRONTIER_URL": "http://frontier.test", "CALYPRIUM_FRONTIER_CRAWL": "alice:7",
            "CALYPRIUM_FRONTIER_TOKEN": "tok", "CALYPRIUM_FRONTIER_LEASE": 10}


class Shop(scrapy.Spider):
    name = "shop"

    def parse(self, response):
        pass

    def parse_item(self, response):
        pass


class FakeFrontier:
    """The service's run operations, in memory (one crawl)."""

    def __init__(self):
        self.entries = {}  # fp -> item, in discovery order
        self.leased, self.done, self.released = set(), {}, []
        self.fail_lease = 0
        self.fail_send = 0

    def discovered(self, items):
        if self.fail_send:
            self.fail_send -= 1
            raise RuntimeError("frontier down")
        added = 0
        for i in items:
            if i["fp"] not in self.entries:
                self.entries[i["fp"]] = i
                added += 1
        return {"added": added, "known": len(items) - added}

    def lease(self, max_items, lease_s):
        if self.fail_lease:
            self.fail_lease -= 1
            raise RuntimeError("frontier down")
        due = [i for fp, i in self.entries.items() if fp not in self.leased and fp not in self.done]
        out = due[:max_items]
        self.leased.update(i["fp"] for i in out)
        return {"items": [dict(i) for i in out], "more": len(due) > max_items, "due_now": len(due) - len(out)}

    def ack(self, done, release):
        if self.fail_send:
            self.fail_send -= 1
            raise RuntimeError("frontier down")
        for d in done:
            self.done[d["fp"]] = d
        self.released += release
        for fp in release:
            self.leased.discard(fp)
        return {}

    def close(self):
        pass


def _sync(fn, *a, **kw):
    try:
        return defer.succeed(fn(*a, **kw))
    except Exception as exc:  # noqa: BLE001
        return defer.fail(exc)


@pytest.fixture
def sched(monkeypatch):
    from scrapy.utils.request import RequestFingerprinter

    monkeypatch.setattr(frontier, "deferToThread", _sync)
    crawler = make_crawler(SETTINGS)
    crawler.request_fingerprinter = RequestFingerprinter()
    s = FrontierScheduler.from_crawler(crawler)
    s.client = FakeFrontier()
    s.spider = Shop()
    s.spider.crawler = crawler
    return s


def _response(request, status=200):
    return HtmlResponse(request.url, status=status, body=b"<html></html>", request=request)


def test_without_a_frontier_it_is_scrapys_scheduler():
    from scrapy.core.scheduler import Scheduler
    from scrapy.utils.test import get_crawler

    crawler = get_crawler(settings_dict={"CALYPRIUM_FRONTIER_URL": ""})
    assert isinstance(FrontierScheduler.from_crawler(crawler), Scheduler)


def test_requests_go_to_the_frontier_and_come_back_rebuilt(sched):
    spider = sched.spider
    get = Request("https://shop.example/p/1", callback=spider.parse_item, priority=3,
                  meta={"depth": 2, "mimic": True}, headers={"Accept-Language": "en-US"})
    post = FormRequest("https://shop.example/search", formdata={"q": "lamp"}, callback=spider.parse)
    assert sched.enqueue_request(get) and sched.enqueue_request(post)
    sched._send()
    sent = list(sched.client.entries.values())
    assert [i["url"] for i in sent] == [get.url, post.url] and sent[0]["priority"] == 3
    assert len(sent[0]["fp"]) == 32 and json.loads(sent[0]["req"])["callback"] == "parse_item"

    got = [sched.next_request(), sched.next_request()]
    assert got[0].url == get.url and got[0].callback == spider.parse_item and got[0].priority == 3
    assert got[0].meta["depth"] == 2 and got[0].meta["mimic"] is True and got[0].meta[FP_KEY] == sent[0]["fp"]
    assert got[0].headers.get("Accept-Language") == b"en-US"
    assert got[1].method == "POST" and got[1].body == b"q=lamp" and got[1].callback == spider.parse
    assert type(got[1]) is FormRequest
    assert sched.stats.get_value("frontier/leased") == 2


def test_what_is_meant_to_be_fetched_anyway_or_cannot_be_kept_stays_in_the_run(sched):
    start = Request("https://shop.example/", dont_filter=True)
    lam = Request("https://shop.example/a", callback=lambda r: None)
    odd = Request("https://shop.example/b", meta={"obj": object()})
    for r in (start, lam, odd):
        assert sched.enqueue_request(r)
    assert sched.client.entries == {} and not sched.found
    assert [sched.next_request() for _ in range(3)] == [start, lam, odd]
    assert sched.stats.get_value("frontier/local") == 1 and sched.stats.get_value("frontier/unserializable") == 2


def test_a_page_found_twice_in_a_run_goes_once(sched):
    assert sched.enqueue_request(Request("https://shop.example/p/1"))
    assert not sched.enqueue_request(Request("https://shop.example/p/1"))
    assert sched.stats.get_value("frontier/dupes_in_run") == 1 and len(sched.found) == 1


def test_fetched_pages_are_due_again_sooner_without_items(sched):
    for i in (1, 2):
        sched.enqueue_request(Request(f"https://shop.example/p/{i}", callback=sched.spider.parse_item))
    sched._send()
    a, b = sched.next_request(), sched.next_request()
    sched._response_received(_response(a), a, sched.spider)
    sched._response_received(_response(b, status=404), b, sched.spider)
    sched._item_scraped({"x": 1}, _response(a), sched.spider)
    sched._send()
    done = sched.client.done
    assert done[a.meta[FP_KEY]] == {"fp": a.meta[FP_KEY], "status": 200, "refetch_s": 7 * 86400}
    assert done[b.meta[FP_KEY]] == {"fp": b.meta[FP_KEY], "status": 404, "refetch_s": 86400}
    assert sched.fetched == 2
    assert sched.stats.get_value("calyprium/progress/done") == 2


def test_a_request_can_say_when_it_is_due_again(sched):
    week = 7 * 86400
    sched.enqueue_request(Request("https://shop.example/sitemap_1.xml.gz", callback=sched.spider.parse,
                                  meta={"frontier_refetch_s": week}))
    sched.enqueue_request(Request("https://shop.example/p/1", callback=sched.spider.parse_item,
                                  meta={"frontier_refetch_s": "soon"}))
    sched._send()
    a, b = sched.next_request(), sched.next_request()
    assert a.meta["frontier_refetch_s"] == week  # kept through the frontier
    sched._response_received(_response(a), a, sched.spider)
    sched._response_received(_response(b), b, sched.spider)
    sched._item_scraped({"x": 1}, _response(b), sched.spider)
    sched._send()
    assert sched.client.done[a.meta[FP_KEY]]["refetch_s"] == week
    assert sched.client.done[b.meta[FP_KEY]]["refetch_s"] == 7 * 86400  # not a number: the default


def test_a_closing_run_releases_what_it_never_started_and_sends_what_is_left(sched):
    for i in range(5):
        sched.enqueue_request(Request(f"https://shop.example/p/{i}"))
    sched._send()
    started = sched.next_request()
    sched.enqueue_request(Request("https://shop.example/p/new"))
    sched._response_received(_response(started), started, sched.spider)
    sched.close("shutdown")
    client = sched.client
    assert len(client.released) == 4 and started.meta[FP_KEY] in client.done
    assert "https://shop.example/p/new" in {i["url"] for i in client.entries.values()}


def test_the_run_stops_asking_once_nothing_is_due(sched):
    assert sched.has_pending_requests()  # it may have work waiting from an earlier run
    assert sched.next_request() is None
    assert not sched.has_pending_requests()
    sched.enqueue_request(Request("https://shop.example/p/9"))
    assert sched.has_pending_requests()
    sched._send()
    assert sched.next_request().url == "https://shop.example/p/9"


def test_a_failing_frontier_is_asked_again_and_keeps_what_it_could_not_send(sched):
    sched.client.fail_lease = 1
    assert sched.next_request() is None
    assert sched.stats.get_value("frontier/errors/lease") == 1 and sched.has_pending_requests()
    sched.lease_not_before = 0
    sched.client.fail_send = 1
    sched.enqueue_request(Request("https://shop.example/p/1"))
    sched._send()
    assert sched.stats.get_value("frontier/errors/send") == 1 and len(sched.found) == 1
    sched._send()
    assert len(sched.client.entries) == 1 and not sched.found
    assert sched.next_request().url == "https://shop.example/p/1"


def test_a_catalogues_worth_found_at_once_goes_out_in_batches(sched, monkeypatch):
    """A sitemap index's worth found faster than it is sent: the backlog is taken from the front (a list sliced
    each send copied all of it and kept a real run's CPU busy), more sends go out while it is large, and it all
    reaches the frontier."""
    import time

    sent_sizes = []
    real = sched.client.discovered
    monkeypatch.setattr(sched.client, "discovered", lambda items: sent_sizes.append(len(items)) or real(items))
    sched.sending = 99  # sends held back while the spider finds its pages
    t = time.monotonic()
    for i in range(120_000):
        sched.enqueue_request(Request(f"https://shop.example/p/{i}"))
    assert len(sched.found) == 120_000 and sched._max_sending() == frontier.MAX_SENDING_BACKLOG
    sched.sending = 0
    sched._tick()
    while sched.found:
        sched._send()
    assert len(sched.client.entries) == 120_000 and max(sent_sizes) == frontier.MAX_SEND
    assert time.monotonic() - t < 60
