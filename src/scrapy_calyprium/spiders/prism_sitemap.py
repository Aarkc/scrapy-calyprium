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
"""

import logging
from typing import Dict, Optional
from urllib.parse import urlparse, parse_qs, urlencode

import scrapy
from scrapy import signals

from scrapy_calyprium._forge import ForgeAuth

logger = logging.getLogger(__name__)

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

        # Build url_source from class attributes if not provided
        if url_source:
            self.url_source = url_source
        elif self.prism_domain:
            parts = []
            if self.prism_path_prefix:
                parts.append(f"path_prefix={self.prism_path_prefix}")
            if self.prism_pattern:
                parts.append(f"pattern={self.prism_pattern}")
            qs = "?" + "&".join(parts) if parts else ""
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

    def _build_prism_api_url(self, parsed, offset: int, limit: Optional[int] = None) -> str:
        """Build the Prism API URL for a page of URLs."""
        domain = parsed.netloc or parsed.path
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
        raw_urls = data.get("urls", [])
        total = data.get("total", 0) or data.get("total_stale", 0)
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
            logger.info(f"No more URLs from Prism (total={total})")
            self._prism_exhausted = True
            self._update_prism_checkpoint()
            return

        # Filter out fresh URLs if recrawl tracking is enabled.
        # Track raw_count separately so offset advances correctly.
        urls = self._filter_fresh_urls(raw_urls)

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

    def _make_refill_request(self):
        """Create a request to fetch the next Prism page.

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
        )
        return scrapy.Request(
            self._build_prism_api_url(self._prism_parsed, offset=offset, limit=limit),
            callback=self._handle_prism_page,
            errback=self._handle_prism_error,
            meta={
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
            },
            dont_filter=True,
            priority=100,  # higher priority than normal scrape requests
        )

    def _handle_prism_error(self, failure):
        """Reset refill flag and re-queue if the Prism page request fails.

        Without this, a failed Prism fetch leaves ``_refill_in_flight=True``
        forever, no further refills are attempted, and the spider exits
        with finish_reason='finished' once the pending queue drains. We
        re-queue the request so the spider self-heals from transient
        Prism outages even if no scrape response arrives to re-trigger
        the refill check.
        """
        self._refill_in_flight = False
        logger.warning(
            f"Prism refill request failed at offset {self._prism_next_offset:,}: "
            f"{failure.value!r}. Re-queuing."
        )
        if not self._prism_exhausted:
            return self._make_refill_request()

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
