"""CalypriumRequestTracer — per-URL trace spans to ClickHouse via Forge.

Emits one top-level span per URL processed by SpiderAutoRouter.fetch().
Each span captures: URL, routing method, status code, duration, slot_id,
egress_ip, and outcome. Batched and flushed every 5 seconds.

The spider's auto-router calls `tracer.record_span()` after each fetch
completes. The tracer buffers spans and POSTs them to Forge's
`/jobs/spiders/{slug}/runs/{n}/traces` endpoint in batches.

Enabled via settings:
    EXTENSIONS = {
        "scrapy_calyprium.extensions.request_tracer.CalypriumRequestTracer": 501,
    }

Required settings (same as CalypriumRunStats):
    FORGE_API_URL, FORGE_SERVICE_SECRET, RECRAWL_SPIDER_SLUG,
    RECRAWL_USER_ID, SPIDER_RUN_NUMBER
"""
from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
from scrapy import signals
from scrapy.crawler import Crawler

logger = logging.getLogger(__name__)

# Max spans to buffer before forcing a flush
BATCH_SIZE = 100
FLUSH_INTERVAL = 5.0  # seconds


class CalypriumRequestTracer:
    def __init__(
        self,
        forge_url: str,
        service_secret: str,
        user_id: str,
        spider_slug: str,
        run_number: Optional[int],
    ):
        self.forge_url = forge_url.rstrip("/")
        self.service_secret = service_secret
        self.user_id = user_id
        self.spider_slug = spider_slug
        self.run_number = run_number

        self._buffer: List[Dict] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        settings = crawler.settings
        forge_url = settings.get("FORGE_API_URL", "http://calyprium-backend:8000")
        secret = settings.get("FORGE_SERVICE_SECRET", "")
        user_id = (
            settings.get("RECRAWL_USER_ID")
            or settings.get("SPIDER_USER_ID")
            or os.getenv("SPIDER_USER_ID", "internal")
        )
        slug = (
            settings.get("RECRAWL_SPIDER_SLUG")
            or (crawler.spider.name if hasattr(crawler, "spider") and crawler.spider else "")
        )
        run_number_raw = (
            settings.get("SPIDER_RUN_NUMBER")
            or settings.get("CALYPRIUM_RUN_NUMBER")
            or os.getenv("CALYPRIUM_RUN_NUMBER")
        )
        try:
            run_number = int(run_number_raw) if run_number_raw else None
        except (TypeError, ValueError):
            run_number = None

        ext = cls(forge_url, secret, user_id, slug or "", run_number)

        if not slug or not run_number:
            logger.info(
                "CalypriumRequestTracer: disabled (slug=%r, run_number=%r)",
                slug, run_number,
            )
            return ext

        crawler.signals.connect(ext.spider_opened, signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signals.spider_closed)
        return ext

    # -- Public API (called by SpiderAutoRouter) ---------------------------

    def record_span(
        self,
        *,
        trace_id: str,
        url: str,
        domain: str,
        component: str = "spider",
        operation: str = "fetch",
        status: str = "success",
        status_code: int = 0,
        duration_ms: int = 0,
        routing_method: str = "",
        slot_id: str = "",
        egress_ip: str = "",
        proxy_session_id: str = "",
        engine: str = "",
        response_bytes: int = 0,
        error_message: str = "",
        parent_span_id: Optional[str] = None,
    ) -> None:
        """Buffer a span. Thread-safe — called from Scrapy's async context."""
        span = {
            "trace_id": trace_id,
            "parent_span_id": parent_span_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_ms": duration_ms,
            "user_id": self.user_id,
            "spider_slug": self.spider_slug,
            "run_number": self.run_number,
            "url": url,
            "domain": domain,
            "component": component,
            "operation": operation,
            "status": status,
            "status_code": status_code,
            "engine": engine,
            "proxy_session_id": proxy_session_id,
            "egress_ip": egress_ip,
            "slot_id": slot_id,
            "routing_method": routing_method,
            "response_bytes": response_bytes,
            "error_message": error_message,
        }
        with self._lock:
            self._buffer.append(span)
            if len(self._buffer) >= BATCH_SIZE:
                batch = self._buffer[:]
                self._buffer.clear()
        else:
            return
        # Flush outside lock
        self._post_batch(batch)

    # -- Lifecycle ---------------------------------------------------------

    def spider_opened(self, spider):
        if not self.run_number:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._flush_loop, name="calyprium-tracer", daemon=True,
        )
        self._thread.start()
        logger.info(
            "CalypriumRequestTracer: started for %s run %d",
            self.spider_slug, self.run_number,
        )

    def spider_closed(self, spider, reason):
        self._stop.set()
        self._flush()
        if self._thread:
            self._thread.join(timeout=5)

    def _flush_loop(self):
        while not self._stop.wait(FLUSH_INTERVAL):
            self._flush()

    def _flush(self):
        with self._lock:
            if not self._buffer:
                return
            batch = self._buffer[:]
            self._buffer.clear()
        self._post_batch(batch)

    def _post_batch(self, batch: List[Dict]):
        if not batch or not self.run_number:
            return
        url = (
            f"{self.forge_url}/jobs/spiders/{self.spider_slug}/"
            f"runs/{self.run_number}/traces"
        )
        try:
            httpx.post(
                url,
                json={"spans": batch},
                headers={
                    "X-Service-Secret": self.service_secret,
                    "X-User-Id": self.user_id,
                    "Content-Type": "application/json",
                },
                timeout=10.0,
            )
        except Exception as exc:
            logger.debug("CalypriumRequestTracer POST failed: %s", exc)
