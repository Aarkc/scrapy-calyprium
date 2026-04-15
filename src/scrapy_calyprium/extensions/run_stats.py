"""CalypriumRunStats — periodic per-run telemetry reporter.

Hooks into Scrapy signals to keep rolling counts of responses, status
codes, items, and bytes, then POSTs cumulative totals to Forge every
REPORT_INTERVAL seconds. Forge writes each snapshot as a row in
ClickHouse; the UI aggregates them for charts.

Enabled via settings:
    EXTENSIONS = {
        "scrapy_calyprium.extensions.run_stats.CalypriumRunStats": 500,
    }

Required settings:
    FORGE_API_URL         (e.g. "http://calyprium-backend:8000")
    FORGE_SERVICE_SECRET  (service-to-service auth)
    RECRAWL_SPIDER_SLUG   (Forge slug for this run)
    RECRAWL_USER_ID       (the owning user — used for ClickHouse partitioning)
    SCRAPY_JOB            (Scrapyd job id, auto-set by Scrapyd)

Optional:
    CALYPRIUM_STATS_INTERVAL (seconds, default 30)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx
from scrapy import signals
from scrapy.crawler import Crawler

logger = logging.getLogger(__name__)


class CalypriumRunStats:
    def __init__(
        self,
        forge_url: str,
        service_secret: str,
        user_id: str,
        spider_slug: str,
        scrapyd_job_id: str,
        run_number: Optional[int] = None,
        interval: float = 30.0,
    ):
        self.forge_url = forge_url.rstrip("/")
        self.service_secret = service_secret
        self.user_id = user_id
        self.spider_slug = spider_slug
        self.scrapyd_job_id = scrapyd_job_id
        self.run_number = run_number
        self.interval = interval

        # Rolling counts — cumulative from spider start, not deltas. Forge
        # stores each snapshot as a row and uses max() over the window so
        # duplicates from slow / retried reports are idempotent.
        self._lock = threading.Lock()
        self._request_count = 0
        self._response_count = 0
        self._item_count = 0
        self._bytes_downloaded = 0
        self._status_counts: Counter = Counter()
        self._routing_counts: Counter = Counter()
        self._started_at = datetime.now(timezone.utc)

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
            or crawler.spider.name if hasattr(crawler, "spider") else ""
        )
        job_id = settings.get("SCRAPY_JOB", os.getenv("SCRAPY_JOB", ""))

        # Forge sets SPIDER_RUN_NUMBER when scheduling via scrapyd_service.
        # Also accept CALYPRIUM_RUN_NUMBER for future-proofing.
        run_number_raw = (
            settings.get("SPIDER_RUN_NUMBER")
            or settings.get("CALYPRIUM_RUN_NUMBER")
            or os.getenv("CALYPRIUM_RUN_NUMBER")
        )
        try:
            run_number = int(run_number_raw) if run_number_raw else None
        except (TypeError, ValueError):
            run_number = None

        interval = settings.getfloat("CALYPRIUM_STATS_INTERVAL", 30.0)

        if not slug or not run_number:
            logger.info(
                "CalypriumRunStats: disabled (slug=%r, run_number=%r)",
                slug, run_number,
            )
            return cls(forge_url, secret, user_id, slug or "", job_id,
                       run_number, interval)

        ext = cls(forge_url, secret, user_id, slug, job_id, run_number, interval)
        crawler.signals.connect(ext.spider_opened, signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signals.spider_closed)
        crawler.signals.connect(ext.response_received, signals.response_received)
        crawler.signals.connect(ext.request_scheduled, signals.request_scheduled)
        crawler.signals.connect(ext.item_scraped, signals.item_scraped)
        return ext

    # -- signal handlers -------------------------------------------------

    def request_scheduled(self, request, spider):
        with self._lock:
            self._request_count += 1

    def response_received(self, response, request, spider):
        with self._lock:
            self._response_count += 1
            self._status_counts[str(response.status)] += 1
            # Scrapy's Response has .body (bytes) — use len() for size.
            # Fall back to Content-Length header if body isn't loaded.
            try:
                self._bytes_downloaded += len(response.body or b"")
            except Exception:
                cl = response.headers.get(b"Content-Length")
                if cl:
                    try:
                        self._bytes_downloaded += int(cl)
                    except ValueError:
                        pass
            # Routing method from our own middleware (if set)
            method = request.meta.get("calyprium_routing_method")
            if method:
                self._routing_counts[method] += 1

    def item_scraped(self, item, response, spider):
        with self._lock:
            self._item_count += 1

    # -- lifecycle -------------------------------------------------------

    def spider_opened(self, spider):
        if not self.spider_slug or not self.run_number:
            return
        self._started_at = datetime.now(timezone.utc)
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._report_loop,
            name="calyprium-stats-reporter",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "CalypriumRunStats: reporter started for %s (interval=%ds)",
            self.spider_slug, int(self.interval),
        )

    def spider_closed(self, spider, reason):
        self._stop.set()
        # Final flush so the UI sees the last counts even if the reporter
        # thread was mid-sleep at shutdown.
        self._flush()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info(
            "CalypriumRunStats: reporter closed (reason=%s, final requests=%d, "
            "items=%d, status=%s)",
            reason, self._request_count, self._item_count,
            dict(self._status_counts),
        )

    def _report_loop(self):
        while not self._stop.wait(self.interval):
            try:
                self._flush()
            except Exception as exc:
                logger.debug("CalypriumRunStats flush error: %s", exc)

    def _flush(self):
        # Snapshot under lock, then POST outside it so HTTP latency doesn't
        # block signal handlers.
        with self._lock:
            payload = {
                "request_count": self._request_count,
                "response_count": self._response_count,
                "item_count": self._item_count,
                "bytes_downloaded": self._bytes_downloaded,
                "status_code_counts": dict(self._status_counts),
                "routing_method_counts": dict(self._routing_counts),
                "window_start": self._started_at.isoformat(),
                "window_end": datetime.now(timezone.utc).isoformat(),
            }

        if not self.run_number:
            return

        url = (
            f"{self.forge_url}/jobs/spiders/{self.spider_slug}/runs/"
            f"{self.run_number}/stats"
        )
        try:
            httpx.post(
                url,
                json=payload,
                headers={
                    "X-Service-Secret": self.service_secret,
                    "X-User-Id": self.user_id,
                    "Content-Type": "application/json",
                },
                timeout=10.0,
            )
        except Exception as exc:
            logger.debug("CalypriumRunStats POST failed: %s", exc)

