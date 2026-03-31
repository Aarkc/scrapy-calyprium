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

        if parsed.scheme == "prism":
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
        urls = data.get("urls", [])
        total = data.get("total", 0)

        if not urls:
            logger.info(f"No more URLs from Prism (total={total})")
            self._prism_exhausted = True
            return

        logger.info(
            f"Prism: got {len(urls):,} URLs "
            f"(offset={self._prism_next_offset:,}, total={total:,}, "
            f"pending={self._pending_count:,})"
        )

        for url in urls:
            if self.max_urls and self._urls_yielded >= self.max_urls:
                logger.info(f"Reached max_urls limit ({self.max_urls:,})")
                self._prism_exhausted = True
                return
            self._urls_yielded += 1
            yield scrapy.Request(url, callback=self._parse_and_maybe_refill)

        self._prism_next_offset += len(urls)

        # If this batch was smaller than requested, Prism is exhausted
        if len(urls) < min(self.batch_size, 100000):
            self._prism_exhausted = True

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
