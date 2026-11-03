"""A Scrapy scheduler whose queue is the spider's frontier, kept outside the run (calyprium's frontier service).

Every request the spider yields goes to its frontier as it is discovered; the run fetches what the frontier leases
it; each fetched page is acknowledged with when it is due again. The frontier keeps each page once per spider with
when it was fetched, so a run that stops (its time limit, a block) loses nothing, the next run carries on from what
is due, and a new pass re-fetches pages only once they are due again. No spider code changes:

    SCHEDULER = "scrapy_calyprium.frontier.FrontierScheduler"
    CALYPRIUM_FRONTIER_URL = "http://calyprium-frontier:8000"
    CALYPRIUM_FRONTIER_CRAWL = "<user id>:<spider id>"     # forge sets these three for a run
    CALYPRIUM_FRONTIER_TOKEN = "<the run's token>"

- A request marked ``dont_filter`` (start URLs, by Scrapy's default ``start_requests``, and retries) is fetched in
  this run without going through the frontier: it is meant to be fetched whatever was fetched before. So is one that
  can't be kept (its callback isn't a method of the spider, its meta isn't JSON).
- A page that produced items is due again CALYPRIUM_FRONTIER_REFETCH_S later (default 7 days); one that produced
  none (a listing, a sitemap: where new pages are found) CALYPRIUM_FRONTIER_LISTING_REFETCH_S later (1 day). A
  request's ``meta["frontier_refetch_s"]`` is its own wait instead: a catalogue's sitemaps, a thousand files of
  50,000 pages each that change slowly, are not read again every day.
- The frontier hands out the highest priority first, then the oldest due. The priority it orders by is the
  request's own raised by its depth, so what a page yields (its detail page, its JSON) is fetched before the pages
  found alongside that page, as Scrapy's last-in-first-out queue did: otherwise a page's follow-up waits behind the
  whole sitemap the page came from. A spider that set DEPTH_PRIORITY for breadth-first order already has its depth
  taken off its priority, so it keeps that order. The stored request keeps its own priority.
- A page that was refused or not answered (a 5xx, 403, 407, 408 or 429, unless it produced items) is acknowledged as
  failed: due again CALYPRIUM_FRONTIER_RETRY_S later (1 hour), not after the refetch wait as if it had been read.
  A request that fails for good is not acknowledged: its lease runs out and the frontier hands it out again. Either
  way each try counts towards the frontier's attempts limit, after which it waits a week. Requests leased but never handed to the downloader are released when the run closes.
- Leases come CALYPRIUM_FRONTIER_LEASE at a time (default 200), lasting CALYPRIUM_FRONTIER_LEASE_S (1800); what is
  discovered and acknowledged is sent every 2 s or CALYPRIUM_FRONTIER_FLUSH entries (200).
- The run's progress (scrapy_calyprium.progress) is the pages it fetched and those the frontier says are due.
- Without the three settings it is Scrapy's own scheduler.

Calls to the frontier run in threads; their results are applied on the reactor.
"""
from __future__ import annotations

import base64
import json
import logging
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx
from scrapy import Request, signals
from twisted.internet import defer, task
from twisted.internet.threads import deferToThread

from . import progress

logger = logging.getLogger(__name__)

FP_KEY = "_frontier_fp"
REFETCH_KEY = "frontier_refetch_s"  # a request's own wait until it is due again, in seconds
REFUSED = frozenset((403, 407, 408, 429))  # with every 5xx: not read, so tried again soon
MAX_SEND = 5000  # discovered URLs in one send (the service takes 10,000)
MAX_SENDING = 2  # sends in flight: they share the reactor's thread pool with DNS
MAX_SENDING_BACKLOG = 4  # ... unless what was found piles up (a sitemap of tens of thousands at a time)
BACKLOG = 20_000
SENT_MAX = 2_000_000  # fingerprints remembered in a run (~80 MB); past it the frontier alone drops repeats
_DEFAULTS = (("method", "GET"), ("headers", {}), ("body", b""), ("cookies", {}), ("encoding", "utf-8"),
             ("priority", 0), ("dont_filter", False), ("flags", []), ("cb_kwargs", {}), ("meta", {}),
             ("errback", None), ("callback", None))


class FrontierClient:
    """The frontier service's run operations (blocking; the scheduler calls them in threads)."""

    def __init__(self, url: str, crawl: str, token: str, timeout: float = 30.0):
        from urllib.parse import quote

        self.base = f"{url.rstrip('/')}/v1/crawls/{quote(crawl, safe='')}"
        self.http = httpx.Client(timeout=timeout, headers={"Authorization": f"Bearer {token}"})

    def _post(self, op: str, body: dict) -> dict:
        r = self.http.post(f"{self.base}/{op}", json=body)
        r.raise_for_status()
        return r.json()

    def discovered(self, items: List[dict]) -> dict:
        return self._post("discovered", {"items": items})

    def lease(self, max_items: int, lease_s: int) -> dict:
        return self._post("lease", {"max": max_items, "lease_s": lease_s})

    def ack(self, acks: List[dict], release: List[str]) -> dict:
        done = [a for a in acks if "retry_s" not in a]
        failed = [{"fp": a["fp"], "retry_s": a["retry_s"]} for a in acks if "retry_s" in a]
        return self._post("ack", {"done": done, "failed": failed, "release": release})

    def close(self) -> None:
        self.http.close()


class FrontierScheduler:
    """See the module docstring."""

    def __init__(self, crawler, client: FrontierClient, *, refetch_s: int, listing_refetch_s: int,
                 lease_size: int, lease_s: int, flush_at: int, retry_s: int = 3600):
        self.crawler = crawler
        self.stats = crawler.stats
        self.client = client
        self.refetch_s, self.listing_refetch_s, self.retry_s = refetch_s, listing_refetch_s, retry_s
        self.lease_size, self.lease_s, self.flush_at = lease_size, lease_s, flush_at
        self.spider = None
        self.local: Deque[Request] = deque()        # fetched in this run, not through the frontier
        self.leased: Deque[Tuple[str, Request]] = deque()  # leased, not handed to the downloader yet
        self.sent: set = set()                      # fingerprints sent or leased this run (8 bytes each)
        self.found: Deque[dict] = deque()           # discovered, not sent yet (taken from the front)
        self.acks: Dict[str, Tuple[int, int, bool]] = {}  # fp -> (status, wait, refused), not sent yet
        self.releases: List[str] = []
        self.remote_more = True                     # the frontier may have something due
        self.leasing = False
        self.sending = 0                            # sends in flight
        self.fetched = 0
        self.due_now = 0
        self.lease_failures = 0
        self.lease_not_before = 0.0
        self.loop: Optional[task.LoopingCall] = None
        self._names: Dict[Any, str] = {}  # a callback's function -> the spider attribute it is under

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        url, crawl, token = (s.get("CALYPRIUM_FRONTIER_URL"), s.get("CALYPRIUM_FRONTIER_CRAWL"),
                             s.get("CALYPRIUM_FRONTIER_TOKEN"))
        if not (url and crawl and token):
            from scrapy.core.scheduler import Scheduler

            logger.warning("FrontierScheduler: no frontier configured; using Scrapy's scheduler")
            return Scheduler.from_crawler(crawler)
        sched = cls(crawler, FrontierClient(url, crawl, token),
                    refetch_s=s.getint("CALYPRIUM_FRONTIER_REFETCH_S", 7 * 86400),
                    listing_refetch_s=s.getint("CALYPRIUM_FRONTIER_LISTING_REFETCH_S", 86400),
                    lease_size=s.getint("CALYPRIUM_FRONTIER_LEASE", 200),
                    lease_s=s.getint("CALYPRIUM_FRONTIER_LEASE_S", 1800),
                    flush_at=s.getint("CALYPRIUM_FRONTIER_FLUSH", 200),
                    retry_s=s.getint("CALYPRIUM_FRONTIER_RETRY_S", 3600))
        crawler.signals.connect(sched._response_received, signal=signals.response_received)
        crawler.signals.connect(sched._item_scraped, signal=signals.item_scraped)
        return sched

    # ── Scrapy's scheduler interface ────────────────────────────────────────────────────────────────────

    def open(self, spider):
        self.spider = spider
        self.loop = task.LoopingCall(self._tick)
        self.loop.start(2.0, now=False)
        logger.info("FrontierScheduler: the frontier is %s", self.client.base)

    def has_pending_requests(self) -> bool:
        return bool(self.local or self.leased or self.found or self.acks or self.releases or self.leasing
                    or self.sending or self.remote_more)

    def enqueue_request(self, request: Request) -> bool:
        if request.dont_filter:
            self.local.append(request)
            self.stats.inc_value("frontier/local")
            return True
        fp = self.crawler.request_fingerprinter.fingerprint(request)
        key = fp[:8]
        if key in self.sent:
            self.stats.inc_value("frontier/dupes_in_run")
            return False
        req = self._serialize(request)
        if req is None:
            self.local.append(request)
            self.stats.inc_value("frontier/unserializable")
            return True
        if len(self.sent) < SENT_MAX:
            self.sent.add(key)
        self.found.append({"fp": fp[:16].hex(), "url": request.url, "req": req, "priority": self._order(request)})
        self.stats.inc_value("frontier/discovered")
        if len(self.found) >= self.flush_at and self.sending < self._max_sending():
            self._send()  # else the next tick sends it, with what is found meanwhile
        return True

    def next_request(self) -> Optional[Request]:
        if self.local:
            return self.local.popleft()
        if len(self.leased) < max(1, self.lease_size // 2):
            if not self.leased and self.found and not self.sending:
                self._send()  # nothing else to fetch: what was found may be all there is
            self._lease()
        if self.leased:
            _, request = self.leased.popleft()
            return request
        return None

    def close(self, reason: str):
        if self.loop is not None and self.loop.running:
            self.loop.stop()
        self.releases.extend(fp for fp, _ in self.leased)
        self.leased.clear()
        found, acks, releases = list(self.found), [], []
        self.found.clear()
        _, acks, releases = self._take()
        logger.info("FrontierScheduler: closing (%s): %d fetched; sending %d found, %d done, %d released",
                    reason, self.fetched, len(found), len(acks), len(releases))
        d = deferToThread(self._close_blocking, found, acks, releases)

        def _done(_):
            self.client.close()
        d.addBoth(_done)
        return d

    def __len__(self) -> int:
        return len(self.local) + len(self.leased)

    # ── the frontier ────────────────────────────────────────────────────────────────────────────────────

    def _method_name(self, fn) -> Optional[str]:
        """The name a bound method of the spider is found under, or None (then Scrapy's own lookup decides)."""
        func = getattr(fn, "__func__", None)
        if func is None or getattr(fn, "__self__", None) is not self.spider:
            return None
        name = self._names.get(func)
        if name is None:
            name = func.__name__
            if getattr(self.spider, name, None) != fn:
                return None
            self._names[func] = name
        return name

    def _fields(self, request: Request) -> Optional[dict]:
        """A plain Request's fields that differ from the defaults, without Request.to_dict: its search of the
        spider's members for each callback's name cost ~100 us a request, minutes of a run's CPU for a
        catalogue's millions. None when only to_dict can say (a subclass, a callback found another way)."""
        if type(request) is not Request:
            return None
        d: Dict[str, Any] = {}
        for key, fn in (("callback", request.callback), ("errback", request.errback)):
            if fn is not None:
                name = self._method_name(fn)
                if name is None:
                    return None
                d[key] = name
        if request.method != "GET":
            d["method"] = request.method
        if request.headers:
            d["headers"] = dict(request.headers)
        if request.body:
            d["body"] = request.body
        for key in ("cookies", "meta", "flags", "cb_kwargs"):
            value = getattr(request, key)
            if value:
                d[key] = value
        if request.encoding != "utf-8":
            d["encoding"] = request.encoding
        if request.priority:
            d["priority"] = request.priority
        return d

    def _serialize(self, request: Request) -> Optional[str]:
        d = self._fields(request)
        if d is None:
            try:
                d = request.to_dict(spider=self.spider)
            except ValueError:  # a callback that isn't a method of the spider
                return None
            d.pop("url", None)
            for key, default in _DEFAULTS:
                if key in d and d[key] == default:
                    del d[key]
        if "headers" in d:
            d["headers"] = {k.decode("latin-1"): [v.decode("latin-1") for v in vs] for k, vs in d["headers"].items()}
        if "body" in d:
            d["body"] = base64.b64encode(d["body"]).decode()
        try:
            return json.dumps(d, separators=(",", ":"))
        except (TypeError, ValueError):  # meta that isn't JSON
            return None

    def _request(self, item: dict) -> Optional[Request]:
        from scrapy.utils.request import request_from_dict

        try:
            d = json.loads(item["req"]) if item.get("req") else {}
            d["url"] = item["url"]
            if "body" in d:
                d["body"] = base64.b64decode(d["body"])
            d["meta"] = {**(d.get("meta") or {}), FP_KEY: item["fp"]}
            return request_from_dict(d, spider=self.spider)
        except Exception as exc:  # noqa: BLE001 -- a request that no longer fits the spider's code
            logger.warning("FrontierScheduler: could not rebuild %s: %s", item.get("url"), exc)
            self.stats.inc_value("frontier/unrebuildable")
            self.releases.append(item["fp"])  # back to the frontier (it gives it up after its attempts)
            return None

    def _lease(self) -> None:
        if self.leasing or not self.remote_more or time.monotonic() < self.lease_not_before:
            return
        self.leasing = True
        sending_before = self.sending or bool(self.found)
        d = deferToThread(self.client.lease, self.lease_size, self.lease_s)

        def _got(out):
            self.leasing = False
            self.lease_failures = 0
            items = out.get("items") or []
            self.due_now = int(out.get("due_now") or 0)
            for item in items:
                if len(self.sent) < SENT_MAX:
                    self.sent.add(bytes.fromhex(item["fp"])[:8])
                request = self._request(item)
                if request is not None:
                    self.leased.append((item["fp"], request))
            self.stats.inc_value("frontier/leased", len(items))
            if not out.get("more") and not items and not sending_before and not self.found and not self.sending:
                self.remote_more = False  # nothing due and nothing on its way there
            elif out.get("more"):
                self.remote_more = True
            self._report()
            self._wake()

        def _failed(failure):
            self.leasing = False
            self.lease_failures += 1
            self.lease_not_before = time.monotonic() + min(60.0, 2.0 ** self.lease_failures)
            self.stats.inc_value("frontier/errors/lease")
            logger.warning("FrontierScheduler: lease failed (%s); trying again", failure.value)
            if self.lease_failures >= 10 and not self.local and not self.leased:
                logger.error("FrontierScheduler: the frontier is unreachable; the run stops asking it")
                self.remote_more = False
        d.addCallbacks(_got, _failed)

    def _max_sending(self) -> int:
        return MAX_SENDING_BACKLOG if len(self.found) > BACKLOG else MAX_SENDING

    def _take(self):
        # From the front of a deque: a list sliced each send copied the whole backlog every time, and with
        # millions found (a catalogue's sitemaps) that kept the run's CPU busy and its downloads waiting.
        found = [self.found.popleft() for _ in range(min(MAX_SEND, len(self.found)))]
        acks = [{"fp": fp, "status": st, "retry_s" if refused else "refetch_s": wait}
                for fp, (st, wait, refused) in self.acks.items()]
        self.acks = {}
        releases, self.releases = self.releases, []
        return found, acks, releases

    def _close_blocking(self, found, acks, releases) -> None:
        for k in range(0, max(1, len(found)), MAX_SEND):
            self._send_blocking(found[k:k + MAX_SEND], acks if k == 0 else [], releases if k == 0 else [])

    def _send_blocking(self, found, acks, releases) -> dict:
        out = {"added": 0}
        if found:
            out.update(self.client.discovered(found))
        if acks or releases:
            self.client.ack(acks, releases)
        return out

    def _send(self) -> None:
        found, acks, releases = self._take()
        if not (found or acks or releases):
            return
        self.sending += 1
        d = deferToThread(self._send_blocking, found, acks, releases)

        def _sent(out):
            self.sending -= 1
            self.stats.inc_value("frontier/added", int(out.get("added") or 0))
            self.stats.inc_value("frontier/acked", len(acks))
            if found:
                self.remote_more = True  # what was found is due now
            self._wake()

        def _failed(failure):
            self.sending -= 1
            self.stats.inc_value("frontier/errors/send")
            logger.warning("FrontierScheduler: sending to the frontier failed (%s); kept for the next send",
                           failure.value)
            self.found.extendleft(reversed(found))
            for a in acks:
                self.acks.setdefault(a["fp"], (a["status"], a.get("refetch_s", a.get("retry_s")), "retry_s" in a))
            self.releases = releases + self.releases
        d.addCallbacks(_sent, _failed)

    def _tick(self) -> None:
        while self.sending < self._max_sending():
            before = self.sending
            self._send()
            if self.sending == before:  # nothing to send
                break
        if not self.leased and self.remote_more:
            self._lease()

    def _wake(self) -> None:
        """Tell the engine there may be requests (it would otherwise wait for its next heartbeat)."""
        engine = getattr(self.crawler, "engine", None)
        slot = getattr(engine, "_slot", None) or getattr(engine, "slot", None)
        try:
            slot.nextcall.schedule()
        except Exception:  # noqa: BLE001 -- the engine's internals; the heartbeat still comes
            pass

    def _report(self) -> None:
        progress.report(self.stats, done=self.fetched, left=self.due_now + len(self.leased), unit="pages")

    @staticmethod
    def _order(request) -> int:
        try:
            depth = int(request.meta.get("depth") or 0)  # set by Scrapy's DepthMiddleware
        except (TypeError, ValueError):
            depth = 0
        return request.priority + max(0, depth)

    # ── what was fetched ────────────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _own_wait(request) -> Optional[int]:
        try:
            wait = int(request.meta.get(REFETCH_KEY) or 0)
        except (TypeError, ValueError):
            return None
        return wait if wait > 0 else None

    def _response_received(self, response, request, spider):
        fp = request.meta.get(FP_KEY)
        if fp and fp not in self.acks:
            if response.status >= 500 or response.status in REFUSED:
                self.acks[fp] = (response.status, self.retry_s, True)
                self.stats.inc_value("frontier/refused")
            else:
                self.acks[fp] = (response.status, self._own_wait(request) or self.listing_refetch_s, False)
            self.fetched += 1
            self._report()
            if len(self.acks) >= self.flush_at and self.sending < MAX_SENDING:
                self._send()

    def _item_scraped(self, item, response, spider):
        request = getattr(response, "request", None)
        fp = request.meta.get(FP_KEY) if request is not None else None
        if fp:  # a page with items: due again after the longer wait (again, if its first ack has gone)
            self.acks[fp] = (response.status, self._own_wait(request) or self.refetch_s, False)
