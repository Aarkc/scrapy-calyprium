"""
RecrawlTrackingPipeline — reports crawled URLs back to Forge for freshness tracking.

When enabled (RECRAWL_TRACKING_ENABLED=true), this pipeline sends batches
of crawled URLs to Forge's /crawl-complete endpoint. Forge records the
timestamp so subsequent recrawl runs can skip URLs that are still fresh.

Works alongside S3BatchPipeline — this tracks freshness, S3Batch stores data.

Settings:
    RECRAWL_TRACKING_ENABLED: bool — activate this pipeline (default: False)
    FORGE_API_URL: str — Forge backend URL (default: http://calyprium-backend:8000)
    RECRAWL_SPIDER_SLUG: str — spider slug for API calls (default: spider name)
    CALYPRIUM_API_KEY: str — API key for authentication
    RECRAWL_BATCH_SIZE: int — URLs per POST (default: 100)
"""
import logging
from typing import Dict, List, Optional

import scrapy
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


class RecrawlTrackingPipeline:
    """Reports crawled URLs to Forge for freshness tracking."""

    def __init__(
        self,
        forge_url: str,
        spider_slug: str,
        api_key: str,
        run_number: int,
        batch_size: int = 100,
    ):
        self.forge_url = forge_url.rstrip("/")
        self.spider_slug = spider_slug
        self.api_key = api_key
        self.run_number = run_number
        self.batch_size = batch_size
        self._user_id = "internal"
        self._buffer: List[Dict] = []
        self._total_reported = 0

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("RECRAWL_TRACKING_ENABLED", False):
            raise NotConfigured("RECRAWL_TRACKING_ENABLED is not set")

        forge_url = crawler.settings.get(
            "FORGE_API_URL", "http://calyprium-backend:8000"
        )
        spider_slug = crawler.settings.get("RECRAWL_SPIDER_SLUG", "")
        api_key = crawler.settings.get("CALYPRIUM_API_KEY", "")
        run_number = crawler.settings.getint("SPIDER_RUN_NUMBER", 0)
        batch_size = crawler.settings.getint("RECRAWL_BATCH_SIZE", 100)
        user_id = crawler.settings.get("RECRAWL_USER_ID", "") or crawler.settings.get("SPIDER_USER_ID", "internal")

        if not api_key:
            raise NotConfigured(
                "RecrawlTrackingPipeline requires CALYPRIUM_API_KEY"
            )

        pipeline = cls(
            forge_url=forge_url,
            spider_slug=spider_slug,
            api_key=api_key,
            run_number=run_number,
            batch_size=batch_size,
        )
        pipeline._user_id = user_id
        return pipeline

    def open_spider(self, spider):
        if not self.spider_slug:
            self.spider_slug = spider.name
        logger.info(
            f"RecrawlTracking: enabled for {self.spider_slug} "
            f"(forge={self.forge_url}, batch_size={self.batch_size})"
        )

    def process_item(self, item, spider):
        url = item.get("url") or getattr(spider, "_current_url", None)
        if url:
            self._buffer.append({
                "url": url,
                "status": item.get("_http_status", 200),
            })
        if len(self._buffer) >= self.batch_size:
            self._flush(spider)
        return item

    def close_spider(self, spider):
        if self._buffer:
            self._flush(spider)
        logger.info(
            f"RecrawlTracking: reported {self._total_reported} URLs "
            f"for {self.spider_slug}"
        )

    def _flush(self, spider):
        """POST buffered URLs to Forge's crawl-complete endpoint."""
        import httpx

        if not self._buffer:
            return

        endpoint = (
            f"{self.forge_url}/spiders/{self.spider_slug}"
            f"/recrawl/crawl-complete"
        )

        try:
            response = httpx.post(
                endpoint,
                json={
                    "urls": self._buffer,
                    "run_number": self.run_number,
                },
                headers={
                    "X-Service-Secret": self.api_key,
                    "X-User-Id": self._user_id,
                },
                timeout=30.0,
            )
            if response.status_code == 200:
                self._total_reported += len(self._buffer)
                logger.debug(
                    f"RecrawlTracking: reported {len(self._buffer)} URLs "
                    f"(total: {self._total_reported})"
                )
            else:
                logger.warning(
                    f"RecrawlTracking: failed to report {len(self._buffer)} URLs "
                    f"(status={response.status_code}: {response.text[:200]})"
                )
        except Exception as e:
            logger.warning(f"RecrawlTracking: flush failed: {e}")
        finally:
            self._buffer = []
