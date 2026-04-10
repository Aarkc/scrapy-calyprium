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
"""

import logging
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode

import scrapy

logger = logging.getLogger(__name__)

# Only fetch the next batch when pending requests drop below this
_REFILL_THRESHOLD = 1000


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
        return self._urls_yielded - self._urls_responded

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
            self._prism_next_offset = 0
            yield scrapy.Request(
                self._build_prism_api_url(parsed, offset=0),
                callback=self._handle_prism_page,
                meta={
                    "_internal": True,
                    "download_timeout": 120,
                },
                dont_filter=True,
            )
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
            api_key = self.settings.get("FORGE_SERVICE_SECRET", "")
            user_id = self.settings.get("RECRAWL_USER_ID", "") or self.settings.get("SPIDER_USER_ID", "internal")
        except AttributeError:
            forge_url = "http://calyprium-backend:8000"
            api_key = ""
            user_id = "internal"

        try:
            max_urls_setting = self.settings.getint("RECRAWL_MAX_URLS", 0)
        except AttributeError:
            max_urls_setting = 0

        self._targets_forge_url = forge_url
        self._targets_api_key = api_key
        self._targets_user_id = user_id
        self._targets_spider_slug = spider_slug
        self._targets_type = target_type
        self._targets_exhausted = False
        self._targets_offset = 0

        urls = self._fetch_targets_batch()
        if not urls:
            return

        for url in urls:
            if max_urls_setting and self._urls_yielded >= max_urls_setting:
                self._targets_exhausted = True
                return
            self._urls_yielded += 1
            yield scrapy.Request(url, callback=self._parse_and_maybe_refill_targets)

    def _fetch_targets_batch(self):
        """Fetch one batch of pending targets from Forge."""
        import requests as req

        limit = min(self.batch_size, 50000)
        api_params = {"limit": limit, "offset": self._targets_offset}
        if self._targets_type:
            api_params["target_type"] = self._targets_type

        try:
            resp = req.get(
                f"{self._targets_forge_url}/spiders/{self._targets_spider_slug}/targets/pending",
                params=api_params,
                headers={"X-Service-Secret": self._targets_api_key,
                          "X-User-Id": self._targets_user_id},
                timeout=120)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Targets: failed to fetch pending targets: {e}")
            self._targets_exhausted = True
            return []

        urls = data.get("urls", [])
        total = data.get("total_pending", 0)

        if not urls:
            logger.info(f"Targets: no more pending targets (total={total})")
            self._targets_exhausted = True
            return []

        logger.info(f"Targets: got {len(urls):,} pending (offset={self._targets_offset:,}, total={total:,})")
        self._targets_offset += len(urls)
        if len(urls) < limit:
            self._targets_exhausted = True

        return urls

    def _parse_and_maybe_refill_targets(self, response):
        """Wrapper for targets:// -- parse item and refill when queue is low."""
        self._urls_responded += 1
        yield from self.parse_item(response)

        if (not self._targets_exhausted
                and not self._refill_in_flight
                and self._pending_count < _REFILL_THRESHOLD):
            self._refill_in_flight = True
            urls = self._fetch_targets_batch()
            self._refill_in_flight = False
            if urls:
                for url in urls:
                    self._urls_yielded += 1
                    yield scrapy.Request(url, callback=self._parse_and_maybe_refill_targets)

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
            self._recrawl_api_key = self.settings.get("FORGE_SERVICE_SECRET", "") or self.settings.get("CALYPRIUM_API_KEY", "")
        except AttributeError:
            self._recrawl_api_key = ""
        try:
            self._recrawl_user_id = self.settings.get("RECRAWL_USER_ID", "") or self.settings.get("SPIDER_USER_ID", "internal")
        except AttributeError:
            self._recrawl_user_id = "internal"
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
            resp = req.get(
                api_url,
                params={
                    "limit": limit,
                    "prism_offset": self._prism_next_offset,
                },
                headers={
                    "X-Service-Secret": self._recrawl_api_key,
                    "X-User-Id": self._recrawl_user_id,
                },
                timeout=300,
            )
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

    def _build_prism_api_url(self, parsed, offset: int) -> str:
        """Build the Prism API URL for a page of URLs."""
        domain = parsed.netloc or parsed.path
        params = parse_qs(parsed.query)

        api_params = {
            "limit": min(self.batch_size, 100000),
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

    def _handle_prism_page(self, response):
        """Process one page of Prism URLs."""
        self._refill_in_flight = False

        data = response.json()
        raw_urls = data.get("urls", [])
        total = data.get("total", 0) or data.get("total_stale", 0)
        raw_count = len(raw_urls)

        if not raw_urls:
            logger.info(f"No more URLs from Prism (total={total})")
            self._prism_exhausted = True
            return

        # Filter out fresh URLs if recrawl tracking is enabled.
        # Track raw_count separately so offset advances correctly.
        urls = self._filter_fresh_urls(raw_urls)

        logger.info(
            f"Prism: got {len(urls):,} stale / {raw_count:,} total URLs "
            f"(offset={self._prism_next_offset:,}, total={total:,}, "
            f"pending={self._pending_count:,})"
        )

        # Advance offset by the RAW batch size (not filtered)
        self._prism_next_offset += raw_count

        for url in urls:
            if self.max_urls and self._urls_yielded >= self.max_urls:
                logger.info(f"Reached max_urls limit ({self.max_urls:,})")
                self._prism_exhausted = True
                return
            self._urls_yielded += 1
            yield scrapy.Request(url, callback=self._parse_and_maybe_refill)

        # Prism is exhausted only if the RAW batch was smaller than requested
        if raw_count < min(self.batch_size, 100000):
            self._prism_exhausted = True
        # If filtering removed all URLs but Prism has more, skip forward
        # aggressively. With ~1.15M already-fresh URLs at the start of the
        # Prism corpus, the spider needs to advance past them quickly
        # instead of fetching 5k at a time (230+ empty batches). Jump by
        # 50k per hop to clear the fresh prefix in ~23 hops (~30s) instead
        # of ~230 hops (~5 min). Once we hit batches with stale URLs, the
        # normal 5k cadence resumes.
        elif not urls and not self._prism_exhausted:
            skip_stride = 50000
            self._prism_next_offset += skip_stride - raw_count
            logger.info(
                f"Batch fully fresh, skipping ahead to offset "
                f"{self._prism_next_offset:,} (stride={skip_stride:,})"
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
            api_key = self.settings.get("FORGE_SERVICE_SECRET", "")
            user_id = self.settings.get("RECRAWL_USER_ID", "") or self.settings.get("SPIDER_USER_ID", "internal")
            spider_slug = self.settings.get("RECRAWL_SPIDER_SLUG", "") or self.name
        except AttributeError:
            return urls

        if not forge_url or not api_key:
            return urls

        import requests as req
        try:
            resp = req.post(
                f"{forge_url}/spiders/{spider_slug}/recrawl/filter-stale",
                json={"urls": urls},
                headers={"X-Service-Secret": api_key, "X-User-Id": user_id},
                timeout=30,
            )
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

        # Yield parse results
        yield from self.parse_item(response)

        # Check if we should fetch the next batch
        if (
            not self._prism_exhausted
            and not self._refill_in_flight
            and self._pending_count < _REFILL_THRESHOLD
        ):
            self._refill_in_flight = True
            yield self._make_refill_request()

    def _make_refill_request(self):
        """Create a request to fetch the next Prism page."""
        logger.info(
            f"Prism: refilling from offset {self._prism_next_offset:,} "
            f"(pending={self._pending_count:,})"
        )
        return scrapy.Request(
            self._build_prism_api_url(self._prism_parsed, offset=self._prism_next_offset),
            callback=self._handle_prism_page,
            meta={
                "_internal": True,
                "download_timeout": 120,
            },
            dont_filter=True,
        )

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
