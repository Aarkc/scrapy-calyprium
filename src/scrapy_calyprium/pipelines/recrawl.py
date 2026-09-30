"""
RecrawlTrackingPipeline — reports crawled URLs back to Forge for freshness tracking.

When enabled (RECRAWL_TRACKING_ENABLED=true), this pipeline sends batches
of crawled URLs to Forge's /crawl-complete endpoint. Forge records the
timestamp so subsequent recrawl runs can skip URLs that are still fresh.

Works alongside S3BatchPipeline — this tracks freshness, S3Batch stores data.
When S3BatchPipeline is enabled, a URL is reported only after its item has been
durably stored (S3BatchPipeline's ``on_items_persisted`` hook), so a failed or
lost batch never marks its URLs fresh (AAR-59). Without S3BatchPipeline, URLs
are reported as items pass through, as before.

POSTs run off the reactor thread (AAR-64).

Settings:
    RECRAWL_TRACKING_ENABLED: bool — activate this pipeline (default: False)
    FORGE_API_URL: str — Forge backend URL (default: http://calyprium-backend:8000)
    RECRAWL_SPIDER_SLUG: str — spider slug for API calls (default: spider name)
    CALYPRIUM_API_KEY: str — API key for authentication
    RECRAWL_BATCH_SIZE: int — URLs per POST (default: 100)
"""
import logging
from typing import Any, Dict, List, Optional

from itemadapter import ItemAdapter
from scrapy.exceptions import NotConfigured
from twisted.internet import defer
from twisted.internet.threads import deferToThread

from scrapy_calyprium._forge import ForgeAuth
from scrapy_calyprium.pipelines.s3_batch import on_items_persisted

logger = logging.getLogger(__name__)


def s3batch_enabled(settings: Any) -> bool:
    """True if S3BatchPipeline is in ITEM_PIPELINES (and not disabled)."""
    try:
        pipelines = settings.getwithbase("ITEM_PIPELINES")
    except AttributeError:
        pipelines = settings.getdict("ITEM_PIPELINES")
    for path, order in dict(pipelines).items():
        name = path if isinstance(path, str) else getattr(path, "__name__", "")
        if order is not None and name.rsplit(".", 1)[-1] == "S3BatchPipeline":
            return True
    return False


class RecrawlTrackingPipeline:
    """Reports crawled URLs to Forge for freshness tracking."""

    #: Runs a blocking callable off the reactor; overridable in tests.
    _run_blocking = staticmethod(deferToThread)

    def __init__(
        self,
        forge_url: str,
        spider_slug: str,
        api_key: str,
        run_number: int,
        batch_size: int = 100,
        service_secret: Optional[str] = None,
        user_id: str = "internal",
        wait_for_persistence: bool = False,
    ):
        self.forge_url = forge_url.rstrip("/")
        self.spider_slug = spider_slug
        self.api_key = api_key
        self.run_number = run_number
        self.batch_size = batch_size
        self._user_id = user_id
        self.auth = ForgeAuth(api_key, service_secret, user_id)
        self.wait_for_persistence = wait_for_persistence
        self._buffer: List[Dict] = []
        self._total_reported = 0
        self._closing = False
        self._in_flight: List[defer.Deferred] = []

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("RECRAWL_TRACKING_ENABLED", False):
            raise NotConfigured("RECRAWL_TRACKING_ENABLED is not set")

        forge_url = crawler.settings.get(
            "FORGE_API_URL", "http://calyprium-backend:8000"
        )
        spider_slug = crawler.settings.get("RECRAWL_SPIDER_SLUG", "")
        api_key = crawler.settings.get("CALYPRIUM_API_KEY", "")
        service_secret = crawler.settings.get("FORGE_SERVICE_SECRET", "")
        run_number = crawler.settings.getint("SPIDER_RUN_NUMBER", 0)
        batch_size = crawler.settings.getint("RECRAWL_BATCH_SIZE", 100)
        user_id = crawler.settings.get("RECRAWL_USER_ID", "") or crawler.settings.get("SPIDER_USER_ID", "internal")

        if not api_key and not service_secret:
            raise NotConfigured(
                "RecrawlTrackingPipeline requires CALYPRIUM_API_KEY"
            )

        pipeline = cls(
            forge_url=forge_url,
            spider_slug=spider_slug,
            api_key=api_key,
            run_number=run_number,
            batch_size=batch_size,
            service_secret=service_secret,
            user_id=user_id,
            wait_for_persistence=s3batch_enabled(crawler.settings),
        )
        if pipeline.wait_for_persistence:
            on_items_persisted(crawler, pipeline.items_persisted)
        return pipeline

    def open_spider(self, spider):
        if not self.spider_slug:
            self.spider_slug = spider.name
        logger.info(
            f"RecrawlTracking: enabled for {self.spider_slug} "
            f"(forge={self.forge_url}, batch_size={self.batch_size}, "
            f"after_persistence={self.wait_for_persistence})"
        )

    @staticmethod
    def _record(item, fallback_url: Optional[str]) -> Optional[Dict]:
        try:
            adapter = ItemAdapter(item)
            url = adapter.get("url") or fallback_url
            status = adapter.get("_http_status", 200)
        except TypeError:
            return None
        return {"url": url, "status": status} if url else None

    def _add(self, record: Optional[Dict]) -> None:
        if record:
            self._buffer.append(record)

    def process_item(self, item, spider):
        if not self.wait_for_persistence:
            self._add(self._record(item, getattr(spider, "_current_url", None)))
            if len(self._buffer) >= self.batch_size:
                self._flush(spider)
        return item

    def items_persisted(self, entries, spider=None):
        """S3BatchPipeline stored these items — now they may be marked fresh."""
        for item, fallback_url in entries:
            self._add(self._record(item, fallback_url))
        if len(self._buffer) >= self.batch_size or self._closing:
            return self._flush(spider)
        return None

    def close_spider(self, spider):
        # Pipelines close in parallel; S3BatchPipeline's final batch may be
        # persisted after this runs, so later signals flush immediately.
        self._closing = True
        self._flush(spider)
        d = defer.DeferredList(list(self._in_flight))

        def _log(_):
            logger.info(
                f"RecrawlTracking: reported {self._total_reported} URLs "
                f"for {self.spider_slug}"
            )

        d.addCallback(_log)
        return d

    def _flush(self, spider=None) -> Optional[defer.Deferred]:
        """POST buffered URLs to Forge's crawl-complete endpoint, off-reactor."""
        if not self._buffer:
            return None
        batch, self._buffer = self._buffer, []
        d = self._run_blocking(self._post, batch)
        d.addErrback(lambda f: logger.warning(f"RecrawlTracking: flush failed: {f.value}"))
        self._in_flight.append(d)

        def _untrack(result):
            if d in self._in_flight:
                self._in_flight.remove(d)
            return result

        d.addBoth(_untrack)
        return d

    def _post(self, batch: List[Dict]) -> None:
        import httpx

        endpoint = (
            f"{self.forge_url}/spiders/{self.spider_slug}"
            f"/recrawl/crawl-complete"
        )
        try:
            payload = {"urls": batch, "run_number": self.run_number}
            response = self.auth.call(lambda h: httpx.post(
                endpoint, json=payload, headers=h, timeout=30.0,
            ))
            if response.status_code == 200:
                self._total_reported += len(batch)
                logger.debug(
                    f"RecrawlTracking: reported {len(batch)} URLs "
                    f"(total: {self._total_reported})"
                )
            else:
                logger.warning(
                    f"RecrawlTracking: failed to report {len(batch)} URLs "
                    f"(status={response.status_code}: {response.text[:200]})"
                )
        except Exception as e:
            logger.warning(f"RecrawlTracking: flush failed: {e}")
