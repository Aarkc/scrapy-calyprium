"""
PrismSitemapSpider — Scrapy spider that reads URLs from Prism's sitemap database.

Instead of fetching sitemaps at crawl time, this spider reads pre-discovered
URLs from Prism's URL query API. URLs were collected by Prism's background
sitemap scanner and stored in object storage, queryable via DuckDB.

URLs are fetched lazily — the next batch is only requested once the crawler
has consumed most of the current batch. This keeps memory bounded regardless
of total URL count (millions of URLs are fine).

Usage::

    from scrapy_calyprium.spiders import PrismSitemapSpider

    class ProductSpider(PrismSitemapSpider):
        name = "products"
        prism_domain = "www.example.com"
        prism_path_prefix = "/products/"

        def parse_item(self, response):
            yield {"title": response.css("h1::text").get()}

Spider arguments (passed via ``-a`` or Scrapyd settings):
    url_source: Override URL source (default: ``prism://{prism_domain}``)
    prism_url: Override Prism API base URL
    batch_size: URLs per API page (default: 5000)
    max_urls: Stop after N URLs (default: 0 = unlimited)

Checkpointing (``prism://``, AAR-65): ``_prism_checkpoint_offset`` is the start
of the lowest Prism page that still has URLs in flight (or the fetch frontier
when none are), i.e. everything below it is fully crawled. The
``PrismOffsetCheckpoint`` extension persists that, never the fetch frontier, so
a killed run doesn't skip queued-but-uncrawled URLs. When a run that resumed
at offset R reaches the end of the corpus it wraps to 0 and continues up to R
(``PRISM_WRAP_ON_EXHAUST``, default True), so an exhausted checkpoint never
pins later runs at 0 URLs.

Refill failures (``prism://``) back off, 2 s doubling to 60 s with jitter,
never an immediate loop. A domain Prism has not indexed (404) gets the other
``www.`` form tried once, then one scan request per run
(``POST {prism_url}/api/sitemaps/fetch``, with the run's credentials), and the
URL listing is polled with that backoff for up to ``PRISM_WAIT_FOR_SCAN_S``
(default 900 s). Still nothing: the refill stops and, if the run has no URLs,
the spider closes with finish reason ``prism_no_urls`` (stat ``prism/no_urls``).

Link discovery (``prism://``, ``PRISM_DISCOVERY_ENABLED``, default True): when the spider's
``prism_path_prefix`` / ``prism_pattern`` matches none of Prism's URLs for its domain, the site's
sitemap does not list those pages (2026-10-18 fleet check: hirose, nkk, fairview, eaton...). The
run then finds them from the site's own links, best first: the pages above the prefix, the Prism
URLs nearest the filter and the home page, then same-site links ranked by how much of the prefix
they share (``PRISM_DISCOVERY_MAX_PAGES`` pages, default 300, at most ``PRISM_DISCOVERY_MAX_DEPTH``
links deep, default 4). Every link that matches the filter goes to ``parse_item``. Sign-in,
account, cart and checkout links are never followed. No matching page anywhere: the spider closes
with finish reason ``prism_no_matching_urls``. Stats ``prism/discovery_*``.

Prism URLs outside the spider's ``allowed_domains`` are dropped before they are requested (stat
``prism/urls_offsite``). When the listing is all off the domain (the site moved:
andersonpower.com's sitemap lists idealelectricalinc.com) the spider closes with finish reason
``prism_urls_offsite``. A URL on a placeholder host (``http://default/de``: a CMS whose host name
was never set) is read as the Prism domain's.
"""

import heapq
import json
import logging
import random
import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlsplit, urlunsplit

import scrapy
from scrapy import signals
from scrapy.exceptions import CloseSpider, DontCloseSpider
from scrapy.http import TextResponse
from scrapy.linkextractors import LinkExtractor

from scrapy_calyprium import _wait
from scrapy_calyprium._forge import ForgeAuth

logger = logging.getLogger(__name__)

# A failed Prism page request is retried after 2 s, doubling to 60 s (with jitter).
_PRISM_BACKOFF_BASE_S = 2.0
_PRISM_BACKOFF_MAX_S = 60.0
# How long a run waits for Prism to index a domain it does not know (PRISM_WAIT_FOR_SCAN_S).
_PRISM_WAIT_FOR_SCAN_S = 900.0
# The finish reason (and stat) of a run that gave up on Prism ever having URLs for its domain.
PRISM_NO_URLS = "prism_no_urls"
# The finish reason of a run whose every Prism URL is outside its allowed_domains (the site moved).
PRISM_URLS_OFFSITE = "prism_urls_offsite"
# The finish reason of a run whose filter matched no Prism URL and no page reachable by links.
PRISM_NO_MATCHING_URLS = "prism_no_matching_urls"
# Give up on an all-offsite listing after this many URLs without one on the domain.
_OFFSITE_GIVE_UP = 20000

# Link discovery (see the module docstring).
_DISCOVERY_MAX_PAGES = 300
_DISCOVERY_MAX_DEPTH = 4
_DISCOVERY_CONCURRENCY = 8
_DISCOVERY_FRONTIER_MAX = 50000
_DISCOVERY_SEED_LISTING = 2000  # unfiltered Prism URLs read to pick seeds from
_DISCOVERY_SEEDS = 30
_SEED_BONUS = 100
# Never followed: sign-in, account, cart and checkout pages (public content only), and sections
# that rarely lead into a catalogue.
_DISCOVERY_DENY = re.compile(
    r"/(log-?in|log-?out|sign-?in|sign-?up|sign-?out|register|registration|accounts?|"
    r"my-?account|profile|cart|basket|checkout|wishlist|password|auth|sso|oauth|"
    r"blogs?|news|press|careers?|jobs|events?|investors?|author|tags?)(/|$|[?.#;])",
    re.I,
)
_CATALOGUE_WORDS = ("product", "catalog", "categor", "series", "family", "parts", "shop",
                    "collection")


def _on_domains(url: str, domains: List[str]) -> bool:
    """``url``'s host is one of ``domains`` or under one, port aside (as Scrapy's offsite
    filter decides; ``scrapy.utils.url.url_is_from_any_domain`` compares the port too)."""
    try:
        host = (urlsplit(url).hostname or "").lower()
    except ValueError:
        return False
    return bool(host) and any(host == d or host.endswith("." + d)
                              for d in (x.lower().strip(".") for x in domains if x))


def _placeholder_host_fixed(url: str, domain: str) -> str:
    """``http://default/de`` -> ``https://<domain>/de``: a sitemap written by a CMS whose host
    name was never set lists its pages on a single-label host (us.mersen.com, 2026-10-18)."""
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
    except ValueError:
        return url
    if not host or "." in host or ":" in host or not domain:
        return url
    return urlunsplit(("https", domain, parts.path or "/", parts.query, ""))


def _http_status(failure) -> Optional[int]:
    """The HTTP status of a failed request (HttpErrorMiddleware's HttpError), else None."""
    response = getattr(getattr(failure, "value", None), "response", None)
    status = getattr(response, "status", None)
    return status if isinstance(status, int) else None


def _other_www(domain: str) -> Optional[str]:
    """``www.x.com`` <-> ``x.com`` (None for a bare single-label host)."""
    domain = (domain or "").strip().lower()
    if domain.startswith("www."):
        return domain[4:] or None
    return f"www.{domain}" if "." in domain else None


def _with_domain(parsed, domain: str):
    """The ``prism://`` source with another domain (kept in netloc, or path for prism:x)."""
    return parsed._replace(netloc=domain) if parsed.netloc else parsed._replace(path=domain)

# Only fetch the next batch when pending requests drop below this
_REFILL_THRESHOLD = 1000

# A transient targets-fetch error (Forge restart, network blip, timeout) must
# not permanently end the source — that would kill a multi-day catch-up after a
# single hiccup. Tolerate this many *consecutive* failures (retrying on each
# refill) before giving up.
_TARGETS_MAX_FETCH_FAILURES = 5

# A fully-fresh Prism page (every URL filtered as recently crawled) doubles the
# next page's size up to this cap, so a long fresh prefix is walked in few
# hops. Every URL is still examined; the old fixed 50k *skip* jumped over
# URLs that were never checked, and the checkpoint then made that permanent.
_FRESH_PAGE_MAX = 50000


class PrismSitemapSpider(scrapy.Spider):
    """Spider that reads start URLs from Prism's sitemap URL database.

    Subclass this instead of ``scrapy.Spider`` or ``SitemapSpider`` when
    your target domain's sitemaps have already been indexed by Prism.

    Set ``prism_domain`` and optionally ``prism_path_prefix`` or
    ``prism_pattern`` on your subclass to configure which URLs to fetch.
    """

    #: Domain to read URLs for (e.g., "www.example.com"). Required.
    prism_domain: str = ""

    #: URL path prefix filter (e.g., "/products/detail/"). Optional.
    prism_path_prefix: Optional[str] = None

    #: Regex pattern filter on full URL. Optional.
    prism_pattern: Optional[str] = None

    def __init__(
        self,
        url_source: str = None,
        prism_url: str = None,
        batch_size: int = 5000,
        max_urls: int = 0,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.batch_size = int(batch_size)
        self.max_urls = int(max_urls)
        self._prism_url_override = prism_url
        self._urls_yielded = 0
        self._urls_responded = 0
        self._prism_exhausted = False
        self._refill_in_flight = False  # True while a Prism fetch is pending
        self._prism_parsed = None  # stored for refill
        self._prism_next_offset = 0
        # AAR-65 checkpoint bookkeeping (prism:// only).
        self._prism_open_pages: Dict[int, int] = {}  # page start -> in-flight
        self._prism_checkpoint_offset: Optional[int] = None
        self._prism_cycle_start = 0
        self._prism_stop_at: Optional[int] = None
        self._prism_wrapped = False
        self._prism_page_limit = min(self.batch_size, 100000)
        # Refill failures (prism://): consecutive failures drive the backoff; a domain Prism
        # does not know gets its other www form tried once and one scan request per run,
        # then is polled until the wait deadline (monotonic).
        self._prism_refill_failures = 0
        self._prism_alt_tried = False
        self._prism_scan_requested = False
        self._prism_wait_deadline: Optional[float] = None
        # Prism URLs dropped as off the spider's domains, by host (PRISM_URLS_OFFSITE).
        self._offsite_hosts: Dict[str, int] = {}
        self._offsite_count = 0
        self._onsite_seen = False
        # Link discovery: a best-first frontier of (-score, depth, seq, url).
        self._discovery_on = False
        self._discovery_frontier: List[Tuple[int, int, int, str]] = []
        self._discovery_seen: set = set()
        self._discovery_seq = 0
        self._discovery_pages = 0
        self._discovery_inflight = 0
        self._discovery_found = 0
        self._filter_rx = None  # compiled prism pattern; False when it does not compile

        # Build url_source from class attributes if not provided. A spider with neither a
        # url_source nor a prism_domain reads its first allowed domain's URLs from Prism (it is
        # scanned on demand when Prism has none): before, it logged "No url_source and no
        # prism_domain set" and closed without a request (2026-10-18 fleet check, 9 spiders).
        if not url_source and not self.prism_domain:
            fallback = next((d for d in (getattr(self, "allowed_domains", None) or []) if d), None)
            if fallback:
                logger.info("No url_source or prism_domain: reading %s's URLs from Prism", fallback)
                self.prism_domain = fallback
        if url_source:
            self.url_source = url_source
        elif self.prism_domain:
            # Encoded: the source is parsed back with urlparse/parse_qs, where a raw pattern's
            # "+" read as a space and its "#" or "?" cut the query (before 2026-10-18 dwyer's
            # /p/[^/?#]+$ reached Prism as /p/[^/? and panduit's .+ as ". ": 0 URLs each).
            params = {k: v for k, v in (("path_prefix", self.prism_path_prefix),
                                        ("pattern", self.prism_pattern)) if v}
            qs = "?" + urlencode(params) if params else ""
            self.url_source = f"prism://{self.prism_domain}{qs}"
        else:
            self.url_source = None

    @property
    def prism_url(self):
        if self._prism_url_override:
            return self._prism_url_override
        try:
            return self.settings.get("PRISM_URL", "https://prism.calyprium.com")
        except AttributeError:
            return "https://prism.calyprium.com"

    @property
    def _pending_count(self) -> int:
        """Requests still queued in the scheduler — the signal that gates lazy
        refill of the next URL batch.

        Reads the engine's real scheduler depth, which cannot leak. The old
        estimate (``_urls_yielded - _urls_responded``) undercounted responses
        whenever a request failed in a downstream middleware without reaching
        the spider callback OR errback — e.g. httpcloak "cookie replay infra
        error" drops, which never fire either. Those requests inflated the
        estimate permanently, pinning it above ``_REFILL_THRESHOLD`` so refill
        never fired again: every run stalled after the ~200k startup burst with
        millions of targets still pending. The scheduler depth reflects actual
        queued work regardless of how a request resolves, so refill keeps firing
        until the source is genuinely exhausted.

        Falls back to the bookkeeping estimate only if the engine/scheduler is
        not reachable (e.g. before the engine is fully started, or a custom
        scheduler without ``__len__``)."""
        try:
            return len(self.crawler.engine.slot.scheduler)
        except Exception:
            return self._urls_yielded - self._urls_responded

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        # Dupe-filtered requests never reach a callback or errback.
        crawler.signals.connect(spider._on_request_dropped, signal=signals.request_dropped)
        crawler.signals.connect(spider._on_idle, signal=signals.spider_idle)
        return spider

    def _get_forge_auth(self) -> ForgeAuth:
        """Forge credentials for the recrawl/targets/freshness endpoints:
        Bearer CALYPRIUM_API_KEY, legacy FORGE_SERVICE_SECRET fallback."""
        auth = getattr(self, "_forge_auth", None)
        if auth is None:
            try:
                settings = self.settings
            except AttributeError:
                settings = None
            auth = ForgeAuth.from_settings(settings)
            self._forge_auth = auth
        return auth

    def start_requests(self):
        if not self.url_source:
            logger.error("No url_source and no prism_domain set")
            return

        parsed = urlparse(self.url_source)

        if parsed.scheme == "targets":
            yield from self._start_from_targets(parsed)
        elif parsed.scheme == "recrawl":
            yield from self._start_from_recrawl(parsed)
        elif parsed.scheme == "prism":
            self._prism_parsed = parsed
            # Preserve start_offset if set by spider subclass / checkpoint
            if not self._prism_next_offset:
                self._prism_next_offset = 0
            self._prism_cycle_start = self._prism_next_offset
            self._prism_checkpoint_offset = self._prism_next_offset
            yield self._make_refill_request()
        elif parsed.scheme == "file":
            yield from self._start_from_file(parsed.path)
        elif parsed.scheme == "inline":
            for url in parsed.path.split(","):
                url = url.strip()
                if url:
                    yield scrapy.Request(url, callback=self.parse_item)
        else:
            yield scrapy.Request(self.url_source, callback=self.parse_item)

    def _start_from_targets(self, parsed):
        """Fetch pending crawl targets from Forge's targets API.

        URL source: targets://spider-slug?target_type=document
        Fetches one batch at a time, refills lazily like recrawl://.
        """
        import requests as req

        spider_slug = parsed.netloc or parsed.path
        from urllib.parse import parse_qs
        params = parse_qs(parsed.query)
        target_type = params.get("target_type", [None])[0]

        try:
            forge_url = self.settings.get("FORGE_API_URL", "http://calyprium-backend:8000")
        except AttributeError:
            forge_url = "http://calyprium-backend:8000"

        try:
            max_urls_setting = self.settings.getint("RECRAWL_MAX_URLS", 0)
        except AttributeError:
            max_urls_setting = 0

        self._targets_forge_url = forge_url
        self._targets_spider_slug = spider_slug
        self._targets_type = target_type
        self._targets_exhausted = False
        # Keyset cursor (last id seen) — NOT an offset. Offset pagination skips
        # rows and terminates early as the pending set shrinks under concurrent
        # mark-crawled (a run died at ~737k of millions that way). `id > cursor`
        # only moves forward through still-pending rows, so one run drains the
        # whole backlog. 0 = first page.
        self._targets_cursor = 0
        self._targets_fetch_failures = 0

        urls = self._fetch_targets_batch()
        if not urls:
            return

        for url in urls:
            if max_urls_setting and self._urls_yielded >= max_urls_setting:
                self._targets_exhausted = True
                return
            self._urls_yielded += 1
            yield scrapy.Request(
                url,
                callback=self._parse_and_maybe_refill_targets,
                errback=self._targets_errback,
            )

    def _fetch_targets_batch(self):
        """Fetch one batch of pending targets from Forge."""
        import requests as req

        limit = min(self.batch_size, 50000)
        # Keyset pagination: ask for rows with id strictly greater than the last
        # one we saw. Stable while mark-crawled shrinks the pending set.
        api_params = {"limit": limit, "after_id": self._targets_cursor}
        if self._targets_type:
            api_params["target_type"] = self._targets_type

        try:
            resp = self._get_forge_auth().call(lambda h: req.get(
                f"{self._targets_forge_url}/spiders/{self._targets_spider_slug}/targets/pending",
                params=api_params,
                headers=h,
                timeout=120))
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            # Don't permanently exhaust on a transient error — retry on the next
            # refill (there are still in-flight requests to drive it). Only give
            # up after a sustained run of failures (Forge genuinely down).
            self._targets_fetch_failures += 1
            if self._targets_fetch_failures >= _TARGETS_MAX_FETCH_FAILURES:
                logger.error(
                    f"Targets: {self._targets_fetch_failures} consecutive fetch "
                    f"failures (cursor={self._targets_cursor:,}), stopping refill: {e}")
                self._targets_exhausted = True
            else:
                logger.warning(
                    f"Targets: transient fetch failure "
                    f"{self._targets_fetch_failures}/{_TARGETS_MAX_FETCH_FAILURES} "
                    f"(cursor={self._targets_cursor:,}), will retry: {e}")
            return []

        # A successful fetch clears the consecutive-failure run.
        self._targets_fetch_failures = 0

        urls = data.get("urls", [])
        total = data.get("total_pending")  # None in keyset mode (COUNT skipped)

        if not urls:
            logger.info(f"Targets: no more pending targets (cursor={self._targets_cursor:,})")
            self._targets_exhausted = True
            return []

        # Advance the keyset cursor to the last id in this batch. The server
        # returns next_cursor; fall back to the old offset bump only if a legacy
        # server omits it (so we never silently re-fetch the same page forever).
        next_cursor = data.get("next_cursor")
        total_str = f", total={total:,}" if isinstance(total, int) else ""
        logger.info(
            f"Targets: got {len(urls):,} pending "
            f"(cursor={self._targets_cursor:,}{total_str})")
        if next_cursor is not None:
            self._targets_cursor = next_cursor
        else:
            self._targets_cursor += len(urls)
        if len(urls) < limit:
            self._targets_exhausted = True

        return urls

    def _maybe_refill_targets(self):
        """Fetch the next batch of targets when the scheduler queue runs low.

        Yields the new Requests (possibly none). Driven from the success
        callback AND the errback so refill keeps getting a chance to fire as
        the queue drains. The leak that previously stalled refill (failed
        requests bypassing both callback and errback) is now neutralised at the
        source: ``_pending_count`` reads the real scheduler depth, so it can't
        be pinned above ``_REFILL_THRESHOLD`` by un-acked failures (see the
        property docstring; AAR: runs died at ~25k then ~200k of 8.5M)."""
        if (self._targets_exhausted
                or self._refill_in_flight
                or self._pending_count >= _REFILL_THRESHOLD):
            return
        self._refill_in_flight = True
        try:
            urls = self._fetch_targets_batch()
        finally:
            self._refill_in_flight = False
        for url in urls:
            self._urls_yielded += 1
            yield scrapy.Request(
                url,
                callback=self._parse_and_maybe_refill_targets,
                errback=self._targets_errback,
            )

    def _parse_and_maybe_refill_targets(self, response):
        """Wrapper for targets:// -- parse item and refill when queue is low."""
        self._urls_responded += 1
        yield from self.parse_item(response)
        yield from self._maybe_refill_targets()

    def _targets_errback(self, failure):
        """A failed target request still leaves the in-flight queue. Count it so
        _pending_count stays accurate, then drive refill — otherwise accumulated
        errors permanently block refill and end the crawl early."""
        self._urls_responded += 1
        yield from self._maybe_refill_targets()

    def _start_from_recrawl(self, parsed):
        """Fetch first batch of stale URLs, then refill lazily via callbacks.

        Only loads one batch in start_requests. Subsequent batches are
        fetched on-demand by _recrawl_refill() when the pending queue
        drops below the threshold -- same pattern as prism:// but using
        direct HTTP (Forge needs auth headers).
        """
        self._prism_parsed = parsed
        self._prism_next_offset = 0
        self._recrawl_exhausted = False

        # Resolve settings once
        try:
            self._recrawl_forge_url = self.settings.get("FORGE_API_URL", "http://calyprium-backend:8000")
        except AttributeError:
            self._recrawl_forge_url = "http://calyprium-backend:8000"
        try:
            self._recrawl_max_urls = self.settings.getint("RECRAWL_MAX_URLS", 0)
        except AttributeError:
            self._recrawl_max_urls = 0

        # Fetch first batch and yield URLs with refill callback
        urls = self._fetch_recrawl_batch()
        if not urls:
            return

        for url in urls:
            if self._recrawl_max_urls and self._urls_yielded >= self._recrawl_max_urls:
                self._recrawl_exhausted = True
                return
            self._urls_yielded += 1
            yield scrapy.Request(url, callback=self._parse_and_maybe_refill_recrawl)

    def _fetch_recrawl_batch(self):
        """Fetch one batch of stale URLs from Forge via sync HTTP."""
        import requests as req

        spider_slug = (self._prism_parsed.netloc or self._prism_parsed.path)
        limit = min(self.batch_size, 50000)

        if self._recrawl_max_urls:
            remaining = self._recrawl_max_urls - self._urls_yielded
            if remaining <= 0:
                return []
            limit = min(limit, remaining)

        api_url = f"{self._recrawl_forge_url}/spiders/{spider_slug}/recrawl/stale-urls"
        try:
            # AAR-XX: pass the Prism cursor so Forge skips past the
            # already-scanned region of Prism on each call instead of
            # re-walking the entire fresh prefix (~100s on a 1M-row freshness
            # table). The response includes `next_prism_offset` which we
            # adopt for the next batch.
            params = {"limit": limit, "prism_offset": self._prism_next_offset}
            resp = self._get_forge_auth().call(lambda h: req.get(
                api_url, params=params, headers=h, timeout=300,
            ))
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Recrawl: failed to fetch stale URLs: {e}")
            self._recrawl_exhausted = True
            return []

        urls = data.get("urls", [])
        total_stale = data.get("total_stale", 0)
        next_prism_offset = data.get("next_prism_offset", self._prism_next_offset + len(urls))

        if not urls:
            logger.info(f"Recrawl: no more stale URLs (total_stale={total_stale})")
            self._recrawl_exhausted = True
            return []

        logger.info(
            f"Recrawl: got {len(urls):,} stale URLs "
            f"(prism_offset={self._prism_next_offset:,} -> {next_prism_offset:,}, "
            f"total_stale={total_stale:,})"
        )

        # Advance the cursor by however many Prism URLs the server scanned,
        # not just the number of stale URLs we got back. Otherwise we'd
        # re-scan the same fresh prefix on the next call.
        self._prism_next_offset = next_prism_offset
        if len(urls) < limit:
            self._recrawl_exhausted = True

        return urls

    def _parse_and_maybe_refill_recrawl(self, response):
        """Wrapper around parse_item that triggers recrawl refill when queue is low."""
        self._urls_responded += 1
        yield from self.parse_item(response)

        if (
            not self._recrawl_exhausted
            and not self._refill_in_flight
            and self._pending_count < _REFILL_THRESHOLD
        ):
            self._refill_in_flight = True
            urls = self._fetch_recrawl_batch()
            self._refill_in_flight = False

            if not urls:
                return

            for url in urls:
                if self._recrawl_max_urls and self._urls_yielded >= self._recrawl_max_urls:
                    self._recrawl_exhausted = True
                    return
                self._urls_yielded += 1
                yield scrapy.Request(url, callback=self._parse_and_maybe_refill_recrawl)

    def _build_recrawl_api_url(self, parsed, offset: int) -> str:
        """Build the Forge stale-urls API URL for a page of stale URLs."""
        spider_slug = parsed.netloc or parsed.path
        try:
            forge_url = self.settings.get("FORGE_API_URL", "http://calyprium-backend:8000")
        except AttributeError:
            forge_url = "http://calyprium-backend:8000"

        max_urls_setting = 0
        try:
            max_urls_setting = self.settings.getint("RECRAWL_MAX_URLS", 0)
        except AttributeError:
            pass

        limit = min(self.batch_size, 100000)
        if max_urls_setting:
            limit = min(limit, max_urls_setting - self._urls_yielded)
            if limit <= 0:
                return None

        api_params = {
            "limit": limit,
            "offset": offset,
        }
        return f"{forge_url}/spiders/{spider_slug}/recrawl/stale-urls?{urlencode(api_params)}"

    def _build_prism_api_url(self, parsed, offset: int, limit: Optional[int] = None,
                             domain: Optional[str] = None) -> str:
        """Build the Prism API URL for a page of URLs (of ``domain``, default the source's)."""
        domain = domain or parsed.netloc or parsed.path
        params = parse_qs(parsed.query)

        api_params = {
            "limit": limit or min(self.batch_size, 100000),
            "offset": offset,
            "format": "json",
        }
        path_prefix = params.get("path_prefix", [None])[0]
        if path_prefix:
            api_params["path_prefix"] = path_prefix
        pattern = params.get("pattern", [None])[0]
        if pattern:
            api_params["pattern"] = pattern

        return f"{self.prism_url}/api/domains/{domain}/urls?{urlencode(api_params)}"

    # -- prism:// checkpoint bookkeeping (AAR-65) ---------------------------

    def _prism_track(self, page_start: int, delta: int) -> None:
        n = self._prism_open_pages.get(page_start, 0) + delta
        if n > 0:
            self._prism_open_pages[page_start] = n
        else:
            self._prism_open_pages.pop(page_start, None)
        self._update_prism_checkpoint()

    def _update_prism_checkpoint(self) -> None:
        """Lowest page start with URLs in flight, else the fetch frontier.

        Assigned as a plain int so the checkpoint thread reads it atomically.
        """
        if self._prism_checkpoint_offset is None:
            return
        if self._prism_open_pages:
            self._prism_checkpoint_offset = min(self._prism_open_pages)
        else:
            self._prism_checkpoint_offset = self._prism_next_offset

    def _prism_request_done(self, request) -> None:
        page = (getattr(request, "meta", None) or {}).get("_prism_page")
        if page is not None:
            self._prism_track(page, -1)

    def _on_request_dropped(self, request, spider=None, **kwargs) -> None:
        self._prism_request_done(request)

    def _wrap_enabled(self) -> bool:
        try:
            return self.settings.getbool("PRISM_WRAP_ON_EXHAUST", True)
        except AttributeError:
            return True

    def _maybe_wrap(self) -> bool:
        """At the end of the corpus, wrap to 0 and crawl up to where this run
        started. Returns True if wrapped."""
        if self._prism_wrapped or not self._prism_cycle_start or not self._wrap_enabled():
            return False
        self._prism_wrapped = True
        self._prism_stop_at = self._prism_cycle_start
        self._prism_next_offset = 0
        self._prism_page_limit = min(self.batch_size, 100000)
        logger.info(
            f"Prism: reached the end of the corpus; wrapping to offset 0 "
            f"(will stop at {self._prism_stop_at:,})"
        )
        return True

    def _handle_prism_page(self, response):
        """Process one page of Prism URLs."""
        self._refill_in_flight = False

        data = response.json()
        domain = response.meta.get("_prism_alt") or self._prism_domain()
        raw_urls = [_placeholder_host_fixed(u, domain) for u in data.get("urls", [])]
        total = data.get("total", 0) or data.get("total_stale", 0)

        alt = response.meta.get("_prism_alt")
        if alt:
            if not raw_urls:
                # Prism knows the other www form but has no URLs for it either.
                yield from self._prism_not_found_requests()
                return
            logger.warning(
                f"Prism: no URLs for {self._prism_domain()} but {len(raw_urls):,} for {alt}; "
                f"using {alt} for this run"
            )
            self._prism_parsed = _with_domain(self._prism_parsed, alt)
            self._stat_inc("prism/domain_switched")
        # Prism answered: the failure run (and any wait for a scan) is over.
        self._prism_refill_failures = 0
        self._prism_wait_deadline = None
        page_start = response.meta.get("_prism_offset", self._prism_next_offset)
        limit = response.meta.get("_prism_limit") or min(self.batch_size, 100000)

        # After a wrap, stop where this run's pass began.
        end_of_cycle = False
        if self._prism_stop_at is not None and page_start + len(raw_urls) >= self._prism_stop_at:
            raw_urls = raw_urls[: max(0, self._prism_stop_at - page_start)]
            end_of_cycle = True
        raw_count = len(raw_urls)

        if not raw_urls:
            if not end_of_cycle and self._maybe_wrap():
                self._update_prism_checkpoint()
                yield self._make_refill_request()
                return
            if page_start == 0 and any(self._prism_filter()) and self._can_discover():
                yield from self._start_discovery(
                    f"none of {self._prism_domain()}'s URLs match the filter")
                return
            logger.info(f"No more URLs from Prism (total={total})")
            self._prism_exhausted = True
            self._update_prism_checkpoint()
            self._close_if_all_offsite()
            return

        # Filter out fresh URLs if recrawl tracking is enabled.
        # Track raw_count separately so offset advances correctly.
        urls = self._drop_offsite(self._filter_fresh_urls(raw_urls))

        logger.info(
            f"Prism: got {len(urls):,} stale / {raw_count:,} total URLs "
            f"(offset={page_start:,}, total={total:,}, "
            f"pending={self._pending_count:,})"
        )

        # Decide everything before yielding: the generator is consumed lazily,
        # and refill checks must already see the advanced frontier.
        page_end = page_start + raw_count
        if self.max_urls:
            remaining = max(0, self.max_urls - self._urls_yielded)
            if len(urls) >= remaining:
                if len(urls) > remaining:
                    # Checkpoint up to the first URL we won't crawl, not the
                    # whole page.
                    page_end = page_start + raw_urls.index(urls[remaining])
                urls = urls[:remaining]
                logger.info(f"Reached max_urls limit ({self.max_urls:,})")
                self._prism_exhausted = True

        at_end = end_of_cycle or raw_count < limit
        self._prism_next_offset = page_end
        if urls:
            self._prism_open_pages[page_start] = (
                self._prism_open_pages.get(page_start, 0) + len(urls)
            )
            self._urls_yielded += len(urls)
            self._prism_page_limit = min(self.batch_size, 100000)
        elif not at_end and not self._prism_exhausted:
            # Fully fresh: widen the next page instead of skipping ahead.
            self._prism_page_limit = min(max(limit, 1) * 2, max(limit, _FRESH_PAGE_MAX))

        if at_end and not self._prism_exhausted:
            if end_of_cycle or not self._maybe_wrap():
                self._prism_exhausted = True
        self._update_prism_checkpoint()
        if self._prism_exhausted or self._offsite_count >= _OFFSITE_GIVE_UP:
            self._close_if_all_offsite()

        for url in urls:
            yield scrapy.Request(
                url,
                callback=self._parse_and_maybe_refill,
                errback=self._prism_errback,
                meta={"_prism_page": page_start},
            )

        if not urls and not self._prism_exhausted:
            logger.info(
                f"Batch fully fresh, continuing at offset "
                f"{self._prism_next_offset:,} (next page={self._prism_page_limit:,})"
            )
            yield self._make_refill_request()

    def _filter_fresh_urls(self, urls):
        """Filter out recently-crawled URLs via Forge's freshness API.

        Active when RECRAWL_TRACKING_ENABLED=true. Calls Forge's
        /filter-stale endpoint to skip URLs already in crawl_freshness
        with a recent last_crawled_at. This way full runs skip the
        ~1.15M already-tracked URLs and only scrape the ~15M that have
        never been crawled (or are overdue for refresh). If the call
        fails, returns all URLs (fail-open).
        """
        try:
            enabled = self.settings.getbool("RECRAWL_TRACKING_ENABLED", False)
        except AttributeError:
            return urls
        if not enabled:
            return urls

        try:
            forge_url = self.settings.get("FORGE_API_URL", "")
            spider_slug = self.settings.get("RECRAWL_SPIDER_SLUG", "") or self.name
        except AttributeError:
            return urls

        auth = self._get_forge_auth()
        if not forge_url or not auth:
            return urls

        import requests as req
        try:
            resp = auth.call(lambda h: req.post(
                f"{forge_url}/spiders/{spider_slug}/recrawl/filter-stale",
                json={"urls": urls},
                headers=h,
                timeout=30,
            ))
            resp.raise_for_status()
            data = resp.json()
            stale = data.get("stale_urls", urls)
            fresh_count = data.get("fresh_count", 0)
            if fresh_count > 0:
                logger.info(
                    f"Freshness filter: {fresh_count} fresh, "
                    f"{len(stale)} stale out of {len(urls)}"
                )
            return stale
        except Exception as e:
            logger.warning(f"Freshness filter failed (proceeding with all URLs): {e}")
            return urls

    def _parse_and_maybe_refill(self, response):
        """Wrapper around parse_item that triggers refill when queue is low."""
        self._urls_responded += 1

        # Yield parse results; the page counts as done for this URL only once
        # its callback has finished (AAR-65).
        try:
            yield from self.parse_item(response)
        finally:
            self._prism_request_done(response.request or response)

        yield from self._maybe_prism_refill()

    def _prism_errback(self, failure):
        """A failed URL still completes its page, and must keep refill going."""
        self._urls_responded += 1
        self._prism_request_done(getattr(failure, "request", None))
        yield from self._maybe_prism_refill()

    def _maybe_prism_refill(self):
        # Check if we should fetch the next batch
        if (
            not self._prism_exhausted
            and not self._refill_in_flight
            and self._pending_count < _REFILL_THRESHOLD
        ):
            yield self._make_refill_request()

    def _make_refill_request(self, alt_domain: Optional[str] = None):
        """Create a request to fetch the next Prism page (of ``alt_domain`` when probing
        the other www form of a domain Prism does not know).

        Sets ``_refill_in_flight = True`` so any concurrent call sites
        (fast-skip path in ``_handle_prism_page`` and the per-response
        refill check in ``_parse_and_maybe_refill``) cannot fire a second
        refill at the same offset.
        """
        self._refill_in_flight = True
        offset = self._prism_next_offset
        limit = self._prism_page_limit
        logger.info(
            f"Prism: refilling from offset {offset:,} "
            f"(pending={self._pending_count:,})"
            + (f" from {alt_domain}" if alt_domain else "")
        )
        meta = {
            "_internal": True,
            "_prism_offset": offset,
            "_prism_limit": limit,
            "download_timeout": 120,
            # Use a separate download slot so Prism API calls don't
            # compete with proxy-routed scrape requests. Without this,
            # when all CONCURRENT_REQUESTS slots are occupied by
            # stuck/failing proxy requests, the Prism pagination
            # request can't get through → no new URLs → spider
            # finishes prematurely.
            "download_slot": "_prism_internal",
        }
        if alt_domain:
            meta["_prism_alt"] = alt_domain
        return scrapy.Request(
            self._build_prism_api_url(self._prism_parsed, offset=offset, limit=limit,
                                      domain=alt_domain),
            callback=self._handle_prism_page,
            errback=self._handle_prism_error,
            meta=meta,
            dont_filter=True,
            priority=100,  # higher priority than normal scrape requests
        )

    async def _handle_prism_error(self, failure):
        """A failed Prism page request: try again after a backoff, never in a loop.

        Without a retry, a failed Prism fetch left ``_refill_in_flight=True``
        forever and the spider exited once the pending queue drained; an
        immediate re-queue, though, hot-looped (a 404 for a domain Prism has
        not indexed made ~2,265 requests in 5 minutes and 0 items). So the
        retry waits (2 s doubling to 60 s, with jitter), and a 404 goes through
        ``_prism_not_found``: the other www form once, one scan request per
        run, then polling until ``PRISM_WAIT_FOR_SCAN_S``, then give up.

        The wait happens here, in this errback's coroutine (``_wait.sleep``):
        the failure stays in Scrapy's scraper meanwhile, so the spider is not
        idle and does not close under it.
        """
        self._refill_in_flight = False
        if self._prism_exhausted:
            return []
        delay, requests = self._prism_after_failure(failure)
        if delay > 0:
            await _wait.sleep(delay)
        return requests

    def _prism_after_failure(self, failure) -> Tuple[float, List[scrapy.Request]]:
        """How long to wait and what to send after a failed Prism page request (the
        requests are made now, so ``_refill_in_flight`` holds off other refills while
        waiting). Raises CloseSpider when the run gives up on an unindexed domain."""
        self._prism_refill_failures += 1
        status = _http_status(failure)
        if status == 404:
            meta = getattr(getattr(failure, "request", None), "meta", None) or {}
            return self._prism_not_found(meta.get("_prism_alt"))
        self._stat_inc("prism/refill_failures")
        delay = self._prism_backoff()
        logger.warning(
            f"Prism refill request failed at offset {self._prism_next_offset:,}: "
            f"{failure.value!r}. Retrying in {delay:.1f}s "
            f"(failure {self._prism_refill_failures})."
        )
        return delay, [self._make_refill_request()]

    def _prism_not_found(self, alt_failed: Optional[str] = None
                         ) -> Tuple[float, List[scrapy.Request]]:
        """Prism has no URLs for the domain (404): the other www form once, a scan request
        once, then poll with backoff until the wait is over, then give up."""
        domain = self._prism_domain()
        now = time.monotonic()
        if self._prism_wait_deadline is None:
            self._prism_wait_deadline = now + self._prism_wait_for_scan_s()
            self._stat_inc("prism/not_indexed")
        if alt_failed:
            logger.warning(f"Prism has no URLs for {alt_failed} either")
        alt = _other_www(domain)
        if alt and not self._prism_alt_tried and not self._urls_yielded:
            self._prism_alt_tried = True
            logger.warning(f"Prism has no URLs for {domain}; trying {alt}")
            return 0.0, [self._make_refill_request(alt_domain=alt)]
        requests = self._prism_not_found_requests(poll=False)
        remaining = self._prism_wait_deadline - now
        if remaining <= 0 and not requests:  # the scan request always goes out first
            self._prism_give_up(domain)
            return 0.0, []
        delay = min(self._prism_backoff(), max(0.0, remaining))
        logger.warning(
            f"Prism has no URLs for {domain} yet (scan requested); polling again in "
            f"{delay:.1f}s, giving up in {max(0.0, remaining):.0f}s"
        )
        return delay, requests + [self._make_refill_request()]

    def _prism_not_found_requests(self, poll: bool = True) -> List[scrapy.Request]:
        """The scan request (once per run) and, with ``poll``, the next URL page request."""
        requests = []
        if not self._prism_scan_requested:
            self._prism_scan_requested = True
            if self._prism_wait_deadline is None:
                self._prism_wait_deadline = time.monotonic() + self._prism_wait_for_scan_s()
            requests.append(self._make_scan_request(self._prism_domain()))
        if poll:
            requests.append(self._make_refill_request())
        return requests

    def _make_scan_request(self, domain: str) -> scrapy.Request:
        """Ask Prism to index ``domain`` (POST /api/sitemaps/fetch). The URL listing is
        public, but this endpoint needs credentials: the run's API key (Prism accepts any
        valid ``clp_`` key) or, without one, the legacy service secret."""
        logger.warning(f"Prism: asking Prism to scan {domain}")
        return scrapy.Request(
            f"{self.prism_url}/api/sitemaps/fetch?{urlencode({'domain': domain})}",
            method="POST",
            body=json.dumps({"scan_type": "full", "force": False}),
            headers={"Content-Type": "application/json", **self._get_forge_auth().headers()},
            callback=self._handle_prism_scan,
            errback=self._handle_prism_scan_error,
            meta={"_internal": True, "_prism_scan": domain, "download_timeout": 60,
                  "download_slot": "_prism_internal", "handle_httpstatus_all": True},
            dont_filter=True,
            priority=100,
        )

    def _handle_prism_scan(self, response):
        domain = response.meta.get("_prism_scan")
        if response.status in (200, 202, 409):
            # 200: a recent scan exists; 202: scan started; 409: one is already running.
            logger.info(f"Prism: scan of {domain} requested ({response.status})")
            self._stat_inc("prism/scan_requested")
        else:
            logger.warning(
                f"Prism did not take the scan request for {domain} ({response.status}); "
                f"waiting for its URLs anyway"
            )
            self._stat_inc("prism/scan_refused")
        return []

    def _handle_prism_scan_error(self, failure):
        logger.warning(f"Prism scan request failed: {failure.value!r}; waiting for URLs anyway")
        self._stat_inc("prism/scan_refused")
        return []

    def _prism_give_up(self, domain: str) -> None:
        """Stop the refill; close the spider if the run has no URLs at all."""
        self._prism_exhausted = True
        self._stat_inc("prism/no_urls")
        alt = _other_www(domain)
        waited = self._prism_wait_for_scan_s()
        tried = f"{domain}" + (f" or {alt}" if alt and self._prism_alt_tried else "")
        if self._urls_yielded:
            logger.error(
                f"Prism: no URLs for {tried} after waiting {waited:.0f}s for a scan; "
                f"stopping the refill ({PRISM_NO_URLS})"
            )
            return
        logger.error(
            f"Prism: no URLs for {tried} after waiting {waited:.0f}s for a scan; "
            f"closing the spider ({PRISM_NO_URLS})"
        )
        raise CloseSpider(PRISM_NO_URLS)

    def _prism_backoff(self) -> float:
        """2 s doubling to 60 s with the consecutive failures, jittered down by up to 25%."""
        n = max(1, self._prism_refill_failures)
        base = min(_PRISM_BACKOFF_MAX_S, _PRISM_BACKOFF_BASE_S * 2 ** min(n - 1, 16))
        return base * random.uniform(0.75, 1.0)

    def _prism_wait_for_scan_s(self) -> float:
        """PRISM_WAIT_FOR_SCAN_S, ending 30 s before CLOSESPIDER_TIMEOUT when that is set, so
        the run closes as ``prism_no_urls`` rather than on its timeout."""
        try:
            wait = self.settings.getfloat("PRISM_WAIT_FOR_SCAN_S", _PRISM_WAIT_FOR_SCAN_S)
            timeout = self.settings.getfloat("CLOSESPIDER_TIMEOUT", 0)
        except AttributeError:
            return _PRISM_WAIT_FOR_SCAN_S
        if timeout > 0:
            wait = min(wait, max(0.0, timeout - 30))
        return max(0.0, wait)

    def _prism_domain(self) -> str:
        parsed = self._prism_parsed
        return (parsed.netloc or parsed.path) if parsed is not None else (self.prism_domain or "")

    def _stat_inc(self, key: str, count: int = 1) -> None:
        stats = getattr(getattr(self, "crawler", None), "stats", None)
        if stats is not None:
            try:
                stats.inc_value(key, count)
            except Exception:  # noqa: BLE001 -- stats never break a crawl
                pass

    # -- Prism URLs off the spider's domains ----------------------------------------------

    def _drop_offsite(self, urls: List[str]) -> List[str]:
        """The URLs on the spider's allowed_domains (all of them when it sets none)."""
        allowed = [d for d in (getattr(self, "allowed_domains", None) or []) if d]
        if not allowed:
            self._onsite_seen = self._onsite_seen or bool(urls)
            return urls
        keep = []
        for url in urls:
            if _on_domains(url, allowed):
                keep.append(url)
                continue
            try:
                host = urlsplit(url).hostname or "?"
            except ValueError:
                host = "?"
            if host in self._offsite_hosts or len(self._offsite_hosts) < 50:
                self._offsite_hosts[host] = self._offsite_hosts.get(host, 0) + 1
            self._offsite_count += 1
        if len(keep) < len(urls):
            self._stat_inc("prism/urls_offsite", len(urls) - len(keep))
        self._onsite_seen = self._onsite_seen or bool(keep)
        return keep

    def _close_if_all_offsite(self) -> None:
        """Every Prism URL so far was off the spider's domains: close as PRISM_URLS_OFFSITE."""
        if self._onsite_seen or not self._offsite_count or self._urls_yielded:
            return
        self._prism_exhausted = True
        hosts = ", ".join(h for h, _ in sorted(self._offsite_hosts.items(), key=lambda kv: -kv[1])[:5])
        logger.error(
            f"Prism: all {self._offsite_count:,} URLs for {self._prism_domain()} are on {hosts}, "
            f"outside allowed_domains {list(getattr(self, 'allowed_domains', None) or [])}; the "
            f"site may have moved ({PRISM_URLS_OFFSITE})"
        )
        raise CloseSpider(PRISM_URLS_OFFSITE)

    # -- link discovery (see the module docstring) -------------------------------------------

    def _setting(self, name: str, default, kind=int):
        try:
            getter = {int: self.settings.getint, bool: self.settings.getbool}[kind]
            return getter(name, default)
        except AttributeError:
            return default

    def _prism_filter(self) -> Tuple[Optional[str], Optional[str]]:
        """The ``(path_prefix, pattern)`` of the ``prism://`` source."""
        parsed = self._prism_parsed
        params = parse_qs(parsed.query) if parsed is not None else {}
        return params.get("path_prefix", [None])[0], params.get("pattern", [None])[0]

    def _matches_filter(self, url: str) -> bool:
        """Prism's own test: the prefix anywhere in the URL (``loc LIKE %prefix%``), the pattern
        searched in it (``regexp_matches``)."""
        prefix, pattern = self._prism_filter()
        if not prefix and not pattern:
            return False
        if prefix and prefix not in url:
            return False
        if pattern:
            if self._filter_rx is None:
                try:
                    self._filter_rx = re.compile(pattern)
                except re.error:
                    self._filter_rx = False
            if not self._filter_rx or not self._filter_rx.search(url):
                return False
        return True

    def _can_discover(self) -> bool:
        return (self._prism_parsed is not None and not self._discovery_on
                and not self._urls_yielded
                and self._setting("PRISM_DISCOVERY_ENABLED", True, bool))

    def _discovery_hosts(self) -> List[str]:
        hosts = [d for d in (getattr(self, "allowed_domains", None) or []) if d]
        domain = self._prism_domain()
        return hosts or [d for d in (domain, _other_www(domain)) if d]

    def _start_discovery(self, why: str) -> List[scrapy.Request]:
        """Read the unfiltered listing to pick seeds from, then crawl (``_discovery_seed``)."""
        self._discovery_on = True
        self._prism_exhausted = True
        self._update_prism_checkpoint()
        self._stat_inc("prism/discovery_started")
        prefix, pattern = self._prism_filter()
        wanted = " and ".join(f"{k}={v}" for k, v in (("path_prefix", prefix), ("pattern", pattern))
                              if v)
        logger.warning(
            f"Prism: {why} ({wanted}); finding those pages from the site's own links "
            f"(up to {self._setting('PRISM_DISCOVERY_MAX_PAGES', _DISCOVERY_MAX_PAGES):,} pages)"
        )
        return [scrapy.Request(
            self._build_prism_api_url(self._prism_parsed._replace(query=""), offset=0,
                                      limit=_DISCOVERY_SEED_LISTING),
            callback=self._handle_discovery_seeds,
            errback=self._discovery_seeds_failed,
            meta={"_internal": True, "download_timeout": 120, "download_slot": "_prism_internal"},
            dont_filter=True,
            priority=100,
        )]

    def _handle_discovery_seeds(self, response):
        try:
            urls = (response.json() or {}).get("urls") or []
        except Exception:  # noqa: BLE001 -- seeds are a hint; the home page still works
            urls = []
        return self._discovery_seed([u for u in urls if isinstance(u, str)])

    def _discovery_seeds_failed(self, failure):
        return self._discovery_seed([])

    def _discovery_seed(self, prism_urls: List[str]) -> List[scrapy.Request]:
        """The pages above the prefix, the Prism URLs nearest the filter, the home page."""
        domain = self._prism_domain()
        prefix, _ = self._prism_filter()
        out = []
        prism_urls = [_placeholder_host_fixed(u, domain) for u in prism_urls]
        for url in prism_urls:
            if self._matches_filter(url):
                request = self._discovery_target(url, 0)
                if request is not None:
                    out.append(request)
        seeds = []
        if prefix and prefix.startswith("/"):
            segments = [s for s in prefix.split("/") if s]
            seeds += [f"https://{domain}/" + "/".join(segments[:n]) + "/"
                      for n in range(len(segments) - 1, 0, -1)]
        ranked = sorted(prism_urls, key=self._link_score, reverse=True)[:_DISCOVERY_SEEDS]
        seeds += [u for u in ranked if self._link_score(u) > 0]
        seeds.append(f"https://{domain}/")
        for rank, url in enumerate(dict.fromkeys(seeds)):
            self._discovery_push(url, 0, bonus=_SEED_BONUS - rank)
        return out + list(self._discovery_pump())

    def _link_score(self, url: str) -> int:
        """How likely a link leads to the filter's pages: the prefix's leading path segments it
        shares, catalogue words in its path, less with a query string (facets, sorting)."""
        try:
            parts = urlsplit(url)
        except ValueError:
            return -10
        path = parts.path.lower()
        score = 0
        prefix, _ = self._prism_filter()
        if prefix:
            have = [s for s in path.split("/") if s]
            for want, got in zip([s for s in prefix.lower().split("/") if s], have):
                if want != got:
                    score += 1 if got.startswith(want) or want.startswith(got) else 0
                    break
                score += 2
        score += sum(1 for word in _CATALOGUE_WORDS if word in path)
        return score - (2 if parts.query else 0)

    def _discovery_push(self, url: str, depth: int, bonus: int = 0) -> None:
        url = url.split("#", 1)[0]
        if url in self._discovery_seen or len(self._discovery_frontier) >= _DISCOVERY_FRONTIER_MAX:
            return
        try:
            path = urlsplit(url).path or "/"
        except ValueError:
            return
        if _DISCOVERY_DENY.search(path):
            return
        self._discovery_seen.add(url)
        self._discovery_seq += 1
        heapq.heappush(self._discovery_frontier,
                       (-(self._link_score(url) + bonus), depth, self._discovery_seq, url))

    def _discovery_pump(self):
        """The best pages of the frontier, a few in flight at a time, within the page budget."""
        budget = self._setting("PRISM_DISCOVERY_MAX_PAGES", _DISCOVERY_MAX_PAGES)
        while (self._discovery_frontier and self._discovery_inflight < _DISCOVERY_CONCURRENCY
               and self._discovery_pages < budget):
            _, depth, _, url = heapq.heappop(self._discovery_frontier)
            self._discovery_inflight += 1
            self._discovery_pages += 1
            self._stat_inc("prism/discovery_pages")
            yield scrapy.Request(url, callback=self._discovery_page,
                                 errback=self._discovery_page_failed,
                                 meta={"_prism_discovery_depth": depth}, priority=-depth)

    def _discovery_target(self, url: str, depth: int) -> Optional[scrapy.Request]:
        if url in self._discovery_seen or (self.max_urls and self._urls_yielded >= self.max_urls):
            return None
        self._discovery_seen.add(url)
        self._urls_yielded += 1
        self._discovery_found += 1
        self._stat_inc("prism/discovery_targets")
        return scrapy.Request(url, callback=self._discovery_parse,
                              errback=self._discovery_target_failed,
                              meta={"_prism_discovery_depth": depth}, priority=20)

    def _discovery_links(self, response):
        """A page's same-site links: those matching the filter are requested for parse_item,
        the others join the frontier."""
        if not isinstance(response, TextResponse):
            return
        depth = (response.meta.get("_prism_discovery_depth") or 0) + 1
        max_depth = self._setting("PRISM_DISCOVERY_MAX_DEPTH", _DISCOVERY_MAX_DEPTH)
        hosts = self._discovery_hosts()
        try:
            links = LinkExtractor().extract_links(response)
        except Exception:  # noqa: BLE001 -- a page we cannot read links from is a dead end
            return
        for link in links:
            url = link.url.split("#", 1)[0]
            if not _on_domains(url, hosts):
                continue
            if self._matches_filter(url):
                request = self._discovery_target(url, depth)
                if request is not None:
                    yield request
            elif depth <= max_depth:
                self._discovery_push(url, depth)

    def _discovery_page(self, response):
        self._discovery_inflight = max(0, self._discovery_inflight - 1)
        yield from self._discovery_links(response)
        yield from self._discovery_pump()

    def _discovery_page_failed(self, failure):
        self._discovery_inflight = max(0, self._discovery_inflight - 1)
        yield from self._discovery_pump()

    def _discovery_parse(self, response):
        self._urls_responded += 1
        yield from self.parse_item(response)
        yield from self._discovery_links(response)
        yield from self._discovery_pump()

    def _discovery_target_failed(self, failure):
        self._urls_responded += 1
        yield from self._discovery_pump()

    def _on_idle(self, spider=None) -> None:
        """Nothing is in flight: send more of the frontier, or end discovery. Requests dropped
        on the way (offsite, duplicates) never reach a callback, so the in-flight count is reset
        here rather than trusted."""
        if not self._discovery_on:
            return
        self._discovery_inflight = 0
        more = list(self._discovery_pump())
        if more:
            engine = self.crawler.engine
            for request in more:
                try:
                    engine.crawl(request)
                except TypeError:  # Scrapy < 2.10: crawl(request, spider)
                    engine.crawl(request, self)
            raise DontCloseSpider
        self._discovery_on = False
        if self._discovery_found:
            logger.info(f"Prism discovery: {self._discovery_found:,} matching pages from "
                        f"{self._discovery_pages:,} pages visited")
            return
        prefix, pattern = self._prism_filter()
        logger.error(
            f"Prism discovery: no page matching path_prefix={prefix!r} pattern={pattern!r} "
            f"in {self._discovery_pages:,} pages of {self._prism_domain()} ({PRISM_NO_MATCHING_URLS})"
        )
        self._stat_inc("prism/discovery_found_nothing")
        raise CloseSpider(PRISM_NO_MATCHING_URLS)

    def _start_from_file(self, path):
        """Read URLs from a text file (one per line)."""
        try:
            with open(path) as f:
                for line in f:
                    url = line.strip()
                    if url and not url.startswith("#"):
                        if self.max_urls and self._urls_yielded >= self.max_urls:
                            return
                        self._urls_yielded += 1
                        yield scrapy.Request(url, callback=self.parse_item)
        except FileNotFoundError:
            logger.error(f"URL file not found: {path}")

    def parse_item(self, response):
        """Override this in your spider subclass.

        This is the callback for each URL from the sitemap database.
        Extract data from the response and yield dicts or Scrapy Items.

        Example::

            def parse_item(self, response):
                yield {
                    "url": response.url,
                    "title": response.css("h1::text").get(),
                }
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement parse_item()"
        )
