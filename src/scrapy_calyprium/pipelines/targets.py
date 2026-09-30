"""
Crawl target pipelines for multi-spider derived URL workflows.

TargetDiscoveryPipeline: Extracts URLs from items, submits as targets.
TargetCompletionPipeline: Marks targets as crawled after processing.
"""
import logging
from typing import Dict, List
from scrapy.exceptions import NotConfigured
from twisted.internet import defer
from twisted.internet.threads import deferToThread

from scrapy_calyprium._forge import ForgeAuth

logger = logging.getLogger(__name__)


class _OffReactorFlush:
    """Buffered POSTs run in a thread (AAR-64): a slow Forge must not stall
    every in-flight request. close_spider waits for outstanding POSTs."""

    #: Runs a blocking callable off the reactor; overridable in tests.
    _run_blocking = staticmethod(deferToThread)

    def _flush(self):
        if not self._buffer:
            return None
        batch, self._buffer = self._buffer, []
        d = self._run_blocking(self._post, batch)
        d.addErrback(lambda f: logger.warning(
            f"{type(self).__name__}: flush failed: {f.value}"))
        self._in_flight.append(d)

        def _untrack(result):
            if d in self._in_flight:
                self._in_flight.remove(d)
            return result

        d.addBoth(_untrack)
        return d

    def _wait_in_flight(self):
        return defer.DeferredList(list(self._in_flight))


class TargetDiscoveryPipeline(_OffReactorFlush):
    def __init__(self, forge_url, api_key, user_id, target_slug, source_slug,
                 url_fields, nested_fields, batch_size=50, service_secret=None):
        self.forge_url = forge_url.rstrip("/")
        self.api_key = api_key
        self.user_id = user_id
        self.auth = ForgeAuth(api_key, service_secret, user_id)
        self.target_slug = target_slug
        self.source_slug = source_slug
        self.url_fields = url_fields
        self.nested_fields = nested_fields
        self.batch_size = batch_size
        self._buffer = []
        self._total = 0
        self._in_flight = []

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("TARGETS_DISCOVERY_ENABLED", False):
            raise NotConfigured("TARGETS_DISCOVERY_ENABLED is not set")
        s = crawler.settings
        forge_url = s.get("FORGE_API_URL", "")
        api_key = s.get("CALYPRIUM_API_KEY", "")
        secret = s.get("FORGE_SERVICE_SECRET", "")
        user_id = s.get("RECRAWL_USER_ID", "") or s.get("SPIDER_USER_ID", "internal")
        target_slug = s.get("TARGETS_SPIDER_SLUG", "")
        source_slug = s.get("TARGETS_SOURCE_SPIDER_SLUG", "") or s.get("RECRAWL_SPIDER_SLUG", "")
        if not forge_url or not (api_key or secret) or not target_slug:
            raise NotConfigured("Requires FORGE_API_URL, CALYPRIUM_API_KEY, TARGETS_SPIDER_SLUG")
        return cls(forge_url, api_key, user_id, target_slug, source_slug,
                   s.getdict("TARGETS_URL_FIELDS", {}),
                   s.getdict("TARGETS_NESTED_FIELDS", {}),
                   s.getint("TARGETS_BATCH_SIZE", 50), service_secret=secret)

    def open_spider(self, spider):
        logger.info(f"TargetDiscovery: target={self.target_slug} "
                     f"fields={list(self.url_fields.keys())} "
                     f"nested={list(self.nested_fields.keys())}")

    def process_item(self, item, spider):
        source_url = item.get("url", "")
        for field, target_type in self.url_fields.items():
            value = item.get(field)
            if not value:
                continue
            urls = value if isinstance(value, list) else [value]
            for url in urls:
                if url and isinstance(url, str):
                    self._buffer.append({"url": url, "source_url": source_url,
                                         "target_type": target_type})
        for field, config in self.nested_fields.items():
            entries = item.get(field, [])
            if not isinstance(entries, list):
                continue
            url_key = config.get("url_key", "url")
            target_type = config.get("type", field)
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                url = entry.get(url_key)
                if url:
                    meta = {k: v for k, v in entry.items() if k != url_key and v}
                    self._buffer.append({"url": url, "source_url": source_url,
                                         "target_type": target_type,
                                         "metadata": meta or None})
        if len(self._buffer) >= self.batch_size:
            self._flush()
        return item

    def close_spider(self, spider):
        self._flush()
        d = self._wait_in_flight()
        d.addCallback(lambda _: logger.info(
            f"TargetDiscovery: submitted {self._total} targets for {self.target_slug}"))
        return d

    def _post(self, batch):
        import httpx
        try:
            payload = {"targets": batch, "source_spider_slug": self.source_slug}
            resp = self.auth.call(lambda h: httpx.post(
                f"{self.forge_url}/spiders/{self.target_slug}/targets/submit",
                json=payload, headers=h, timeout=30.0))
            if resp.status_code == 200:
                self._total += len(batch)
            else:
                logger.warning(f"TargetDiscovery: submit failed ({resp.status_code})")
        except Exception as e:
            logger.warning(f"TargetDiscovery: flush failed: {e}")


class TargetCompletionPipeline(_OffReactorFlush):
    def __init__(self, forge_url, api_key, user_id, spider_slug, batch_size=50,
                 service_secret=None):
        self.forge_url = forge_url.rstrip("/")
        self.api_key = api_key
        self.user_id = user_id
        self.auth = ForgeAuth(api_key, service_secret, user_id)
        self.spider_slug = spider_slug
        self.batch_size = batch_size
        self._buffer = []
        self._total = 0
        self._in_flight = []

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("TARGETS_COMPLETION_ENABLED", False):
            raise NotConfigured("TARGETS_COMPLETION_ENABLED is not set")
        s = crawler.settings
        forge_url = s.get("FORGE_API_URL", "")
        api_key = s.get("CALYPRIUM_API_KEY", "")
        secret = s.get("FORGE_SERVICE_SECRET", "")
        user_id = s.get("RECRAWL_USER_ID", "") or s.get("SPIDER_USER_ID", "internal")
        spider_slug = s.get("TARGETS_SPIDER_SLUG", "") or s.get("RECRAWL_SPIDER_SLUG", "")
        if not forge_url or not (api_key or secret):
            raise NotConfigured("Requires FORGE_API_URL and CALYPRIUM_API_KEY")
        return cls(forge_url, api_key, user_id, spider_slug,
                   s.getint("TARGETS_BATCH_SIZE", 50), service_secret=secret)

    def open_spider(self, spider):
        if not self.spider_slug:
            self.spider_slug = spider.name

    def process_item(self, item, spider):
        url = item.get("url") or item.get("file_url") or ""
        status = item.get("_http_status", 200)
        if url:
            self._buffer.append({"url": url, "status": status})
        if len(self._buffer) >= self.batch_size:
            self._flush()
        return item

    def close_spider(self, spider):
        self._flush()
        d = self._wait_in_flight()
        d.addCallback(lambda _: logger.info(
            f"TargetCompletion: marked {self._total} targets for {self.spider_slug}"))
        return d

    def _post(self, batch):
        import httpx
        try:
            payload = {"urls": batch}
            resp = self.auth.call(lambda h: httpx.post(
                f"{self.forge_url}/spiders/{self.spider_slug}/targets/mark-crawled",
                json=payload, headers=h, timeout=30.0))
            if resp.status_code == 200:
                self._total += len(batch)
        except Exception as e:
            logger.warning(f"TargetCompletion: flush failed: {e}")
