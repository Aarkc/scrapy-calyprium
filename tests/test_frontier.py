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
from scrapy.settings import Settings
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
        self.leased, self.done, self.failed, self.released = set(), {}, {}, []
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
        due.sort(key=lambda i: -(i.get("priority") or 0))  # highest priority first, then oldest (the service's bands)
        out = due[:max_items]
        self.leased.update(i["fp"] for i in out)
        return {"items": [dict(i) for i in out], "more": len(due) > max_items, "due_now": len(due) - len(out)}

    def ack(self, acks, release):
        if self.fail_send:
            self.fail_send -= 1
            raise RuntimeError("frontier down")
        for d in acks:
            (self.failed if "retry_s" in d else self.done)[d["fp"]] = d
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
    assert [i["url"] for i in sent] == [get.url, post.url] and sent[0]["priority"] == 3 + 2  # raised by its depth
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


def test_what_a_page_yields_comes_before_the_pages_found_alongside_it(sched):
    spider = sched.spider
    for i in range(1, 4):  # a sitemap's pages, found at once
        sched.enqueue_request(Request(f"https://shop.example/p/{i}", callback=spider.parse, meta={"depth": 1}))
    sched._send()
    page = sched.next_request()
    # the page's JSON, then a spider ordering breadth-first (DEPTH_PRIORITY=1 took the depth off its priority)
    sched.enqueue_request(Request("https://shop.example/api/1", callback=spider.parse_item, meta={"depth": 2}))
    sched.enqueue_request(Request("https://shop.example/q/9", callback=spider.parse, priority=-2,
                                  meta={"depth": 2}))
    sched._send()
    sched.leased.clear()  # what is left of the first lease goes back (a later lease)
    sched.client.leased.clear()
    sched.client.done[page.meta[FP_KEY]] = {}
    order = [sched.next_request().url for _ in range(4)]
    assert order == ["https://shop.example/api/1", "https://shop.example/p/2", "https://shop.example/p/3",
                     "https://shop.example/q/9"]
    api = sched.client.entries[next(fp for fp, i in sched.client.entries.items() if i["url"].endswith("/api/1"))]
    assert json.loads(api["req"]).get("priority") is None  # the stored request keeps its own priority


def test_a_page_found_twice_in_a_run_goes_once(sched):
    assert sched.enqueue_request(Request("https://shop.example/p/1"))
    assert not sched.enqueue_request(Request("https://shop.example/p/1"))
    assert sched.stats.get_value("frontier/dupes_in_run") == 1 and len(sched.found) == 1


def test_fetched_pages_are_due_again_sooner_without_items(sched):
    for i in (1, 2, 3):
        sched.enqueue_request(Request(f"https://shop.example/p/{i}", callback=sched.spider.parse_item))
    sched._send()
    a, b, c = sched.next_request(), sched.next_request(), sched.next_request()
    sched._response_received(_response(a), a, sched.spider)
    sched._response_received(_response(b), b, sched.spider)
    sched._response_received(_response(c, status=404), c, sched.spider)
    sched._item_scraped({"x": 1}, _response(a), sched.spider)
    sched._send()
    done = sched.client.done
    assert done[a.meta[FP_KEY]] == {"fp": a.meta[FP_KEY], "status": 200, "refetch_s": 7 * 86400}
    assert done[b.meta[FP_KEY]] == {"fp": b.meta[FP_KEY], "status": 200, "refetch_s": 86400}  # no items: a listing
    # not there: after the long wait, not every day
    assert done[c.meta[FP_KEY]] == {"fp": c.meta[FP_KEY], "status": 404, "refetch_s": 7 * 86400}
    assert sched.fetched == 3
    assert sched.stats.get_value("calyprium/progress/done") == 3


def test_an_item_from_an_errback_leaves_the_page_unanswered(sched):
    from twisted.python.failure import Failure

    sched.enqueue_request(Request("https://shop.example/p/1", callback=sched.spider.parse_item))
    sched._send()
    leased = sched.next_request()
    # the download failed and the errback yielded an item: item_scraped gets the Failure in place of a response
    failure = Failure(RuntimeError("Could not open CONNECT tunnel"))
    failure.request = leased
    sched._item_scraped({"url": leased.url, "error": "tunnel"}, failure, sched.spider)
    sched._send()
    assert sched.client.done == {} and sched.client.failed == {}  # as for a failed download: its lease brings it back
    assert sched.fetched == 0


def test_a_refused_page_is_tried_again_soon_not_when_it_would_be_due(sched):
    week = 7 * 86400
    reqs = [Request("https://shop.example/sitemap_1.xml.gz", callback=sched.spider.parse,
                    meta={"frontier_refetch_s": week}),
            Request("https://shop.example/p/1", callback=sched.spider.parse_item),
            Request("https://shop.example/p/2", callback=sched.spider.parse_item),
            Request("https://shop.example/p/3", callback=sched.spider.parse_item)]
    for r in reqs:
        sched.enqueue_request(r)
    sched._send()
    sm, p1, p2, p3 = (sched.next_request() for _ in range(4))
    sched._response_received(_response(sm, status=502), sm, sched.spider)
    sched._response_received(_response(p1, status=403), p1, sched.spider)
    sched._response_received(_response(p2, status=429), p2, sched.spider)
    sched._item_scraped({"x": 1}, _response(p2, status=429), sched.spider)  # it produced items all the same
    sched._response_received(_response(p3, status=404), p3, sched.spider)
    sched._send()
    client = sched.client
    assert client.failed == {sm.meta[FP_KEY]: {"fp": sm.meta[FP_KEY], "status": 502, "retry_s": 3600},
                             p1.meta[FP_KEY]: {"fp": p1.meta[FP_KEY], "status": 403, "retry_s": 3600}}
    assert client.done[p2.meta[FP_KEY]]["refetch_s"] == week
    assert client.done[p3.meta[FP_KEY]] == {"fp": p3.meta[FP_KEY], "status": 404, "refetch_s": 7 * 86400}  # gone
    assert sched.stats.get_value("frontier/refused") == 3


def test_a_page_that_redirects_is_followed_and_acknowledged_with_where_it_led(sched):
    sched.enqueue_request(Request("https://shop.example/company/old", callback=sched.spider.parse_item))
    sched._send()
    leased = sched.next_request()
    # RedirectMiddleware keeps the 301 from the engine: the request for its Location comes back to the scheduler
    moved = leased.replace(url="https://shop.example/company/new", meta={**leased.meta, "redirect_urls": [leased.url]})
    assert sched.enqueue_request(moved) is True
    assert sched.next_request() is moved
    sched._response_received(_response(moved), moved, sched.spider)
    sched._item_scraped({"x": 1}, _response(moved), sched.spider)
    sched._send()
    fp = leased.meta[FP_KEY]
    assert sched.client.done == {fp: {"fp": fp, "status": 200, "refetch_s": 7 * 86400}}
    assert sched.stats.get_value("frontier/followed") == 1 and sched.fetched == 1
    # what the page's callback yields with the page's meta is a page of its own: the page was answered
    child = Request("https://shop.example/company/other", callback=sched.spider.parse_item, meta=dict(moved.meta))
    sched.enqueue_request(child)
    assert sched.found[-1]["url"] == child.url and sched.stats.get_value("frontier/followed") == 1


def test_a_page_a_middleware_asks_for_again_is_fetched_not_dropped_as_a_repeat(sched):
    sched.enqueue_request(Request("https://shop.example/p/1", callback=sched.spider.parse_item))
    sched._send()
    leased = sched.next_request()
    again = leased.replace(priority=-20)  # not dont_filter: through the frontier, a repeat in this run
    assert sched.enqueue_request(again) is True and sched.next_request() is again
    assert not sched.stats.get_value("frontier/dupes_in_run")


def test_a_refusal_gives_way_to_what_a_later_try_got(sched):
    sched.enqueue_request(Request("https://shop.example/c/1", callback=sched.spider.parse))
    sched._send()
    leased = sched.next_request()
    sched._response_received(_response(leased, status=429), leased, sched.spider)
    retry = leased.replace(dont_filter=True)  # the spider's own try again (a listing: no items)
    sched.enqueue_request(retry)
    assert sched.next_request() is retry
    sched._response_received(_response(retry), retry, sched.spider)
    sched._send()
    fp = leased.meta[FP_KEY]
    assert sched.client.done == {fp: {"fp": fp, "status": 200, "refetch_s": 86400}} and not sched.client.failed
    assert sched.fetched == 1 and sched.stats.get_value("frontier/refused") == 1


def test_the_client_sends_failures_apart():
    sent = {}
    client = frontier.FrontierClient("http://frontier.test", "alice:7", "tok")
    client._post = lambda op, body: sent.update({op: body}) or {}
    client.ack([{"fp": "a", "status": 200, "refetch_s": 60}, {"fp": "b", "status": 503, "retry_s": 3600}], ["c"])
    assert sent["ack"] == {"done": [{"fp": "a", "status": 200, "refetch_s": 60}],
                           "failed": [{"fp": "b", "retry_s": 3600}], "release": ["c"]}
    client.close()


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
    page = sched.next_request()
    assert page.url == "https://shop.example/p/1"
    sched._response_received(_response(page, status=503), page, sched.spider)
    sched.client.fail_send = 1
    sched._send()
    assert sched.acks == {page.meta[FP_KEY]: (503, 3600, True)}  # kept as a failure for the next send
    sched._send()
    assert sched.client.failed[page.meta[FP_KEY]]["retry_s"] == 3600 and not sched.acks


def test_a_frontier_restarting_is_waited_for_and_one_gone_for_good_is_not(sched, monkeypatch):
    """Ten failures in a row gave up after ~6 minutes, about as long as the frontier takes to open after a hard
    restart: a restart closed every run waiting on it (11-04)."""
    clock = [1000.0]
    monkeypatch.setattr(frontier.time, "monotonic", lambda: clock[0])
    sched.client.fail_lease = 10**6
    for _ in range(15):  # 15 failures, a minute apart: still asking
        sched.lease_not_before = 0
        sched._lease()
        clock[0] += 60
    assert sched.remote_more and sched.has_pending_requests()
    sched.client.fail_lease = 0
    sched.client.entries["a"] = {"fp": "a" * 32, "url": "https://shop.example/p/1", "req": None}
    sched.lease_not_before = 0
    assert sched.next_request().url == "https://shop.example/p/1"  # it came back: the clock starts over
    sched.client.fail_lease = 10**6
    for _ in range(21):
        sched.lease_not_before = 0
        sched._lease()
        clock[0] += 60
    assert not sched.remote_more and sched.stats.get_value("frontier/unreachable") == 1


def test_no_download_starts_while_what_was_found_piles_up(sched, monkeypatch):
    monkeypatch.setattr(frontier, "PAUSE_AT", 3)
    start = Request("https://shop.example/", dont_filter=True)
    sched.enqueue_request(start)
    sched.client.fail_send = 1  # the frontier falls behind: the first send fails
    sched.flush_at = 100
    for i in range(5):
        sched.enqueue_request(Request(f"https://shop.example/p/{i}"))
    assert sched.next_request() is None and len(sched.found) == 5
    assert sched.paused and sched.stats.get_value("frontier/paused") == 1
    assert sched.has_pending_requests()
    assert sched.next_request() is start  # the next send went out: downloads start again
    assert not sched.found and not sched.paused and len(sched.client.entries) == 5


def test_pages_found_are_progress_for_the_idle_close(sched):
    from types import SimpleNamespace

    from scrapy.extensions.closespider import CloseSpider

    sched.crawler.settings = Settings({**SETTINGS, "CLOSESPIDER_TIMEOUT_NO_ITEM": 1800})
    idle = CloseSpider(sched.crawler)
    sched.crawler.extensions = SimpleNamespace(middlewares=[object(), idle])
    sched.enqueue_request(Request("https://shop.example/p/1"))
    sched._send()  # new to the frontier
    assert idle.items_in_period == 1
    sched.sent.clear()  # a later run finds it again
    sched.enqueue_request(Request("https://shop.example/p/1"))
    sched._send()  # known already: nothing added, not progress
    assert idle.items_in_period == 1


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
