"""PrismOffsetCheckpoint — resume PrismSitemapSpider from the last offset.

Without this, every spider run restarts at ``_prism_next_offset = 0`` and
walks back through the freshness-cached prefix (hundreds of thousands of
URLs) before reaching the actual work region. That's ~30 min of wasted
warmup on every restart.

This extension persists ``spider._prism_next_offset`` to Forge between
runs:
  * On ``spider_opened`` it GETs the last saved offset and assigns it to
    the spider *before* ``start_requests`` reads it, so the first Prism
    fetch starts at the resumed offset.
  * A background thread POSTs the current offset every
    ``PRISM_CHECKPOINT_INTERVAL`` seconds while the spider is alive.
  * On ``spider_closed`` it flushes one final POST.

Enabled via settings::

    EXTENSIONS = {
        "scrapy_calyprium.extensions.prism_checkpoint.PrismOffsetCheckpoint": 510,
    }
    PRISM_CHECKPOINT_ENABLED = True

Required settings (same as other Forge-aware extensions):
    FORGE_API_URL        backend base URL (e.g. http://calyprium-backend:8000)
    FORGE_SERVICE_SECRET service-to-service auth
    RECRAWL_SPIDER_SLUG  Forge slug for this spider (or it falls back to spider.name)
    RECRAWL_USER_ID      owning user

Optional settings:
    PRISM_CHECKPOINT_INTERVAL  seconds between background saves (default 30)

Forge contract:
    GET  /spiders/{slug}/checkpoint                → {"offset": int} or 404
    POST /spiders/{slug}/checkpoint  {"offset": int} → 200 OK

The spider attribute checkpointed is ``_prism_next_offset``; the extension
silently no-ops on spiders that don't expose it (e.g. non-PrismSitemap
subclasses).
"""
from __future__ import annotations

import logging
import os
import threading
from typing import Optional

import httpx
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


class PrismOffsetCheckpoint:
    def __init__(
        self,
        forge_url: str,
        service_secret: str,
        user_id: str,
        spider_slug: str,
        interval: float = 30.0,
    ):
        self.forge_url = forge_url.rstrip("/")
        self.service_secret = service_secret
        self.user_id = user_id
        self.spider_slug = spider_slug
        self.interval = interval

        self._spider = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Tracks the last value we successfully POSTed so we can skip
        # no-op writes between saves when the spider hasn't advanced
        # (e.g. it's stuck waiting on slot solves).
        self._last_saved: Optional[int] = None

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        settings = crawler.settings
        if not settings.getbool("PRISM_CHECKPOINT_ENABLED", False):
            raise NotConfigured("PRISM_CHECKPOINT_ENABLED is False")

        forge_url = settings.get("FORGE_API_URL", "http://calyprium-backend:8000")
        secret = settings.get("FORGE_SERVICE_SECRET") or os.getenv("FORGE_SERVICE_SECRET", "")
        if not secret:
            raise NotConfigured(
                "PrismOffsetCheckpoint requires FORGE_SERVICE_SECRET to authenticate"
            )

        user_id = (
            settings.get("RECRAWL_USER_ID")
            or settings.get("SPIDER_USER_ID")
            or os.getenv("SPIDER_USER_ID", "internal")
        )
        slug = settings.get("RECRAWL_SPIDER_SLUG", "")
        interval = settings.getfloat("PRISM_CHECKPOINT_INTERVAL", 30.0)

        ext = cls(forge_url, secret, user_id, slug, interval)
        crawler.signals.connect(ext.spider_opened, signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signals.spider_closed)
        return ext

    # -- HTTP helpers ----------------------------------------------------

    def _headers(self) -> dict:
        return {
            "X-Service-Secret": self.service_secret,
            "X-User-Id": self.user_id,
        }

    def _checkpoint_url(self, slug: str) -> str:
        return f"{self.forge_url}/spiders/{slug}/checkpoint"

    def _load_offset(self, slug: str) -> Optional[int]:
        """GET the last saved offset for this spider. Returns None on miss."""
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.get(self._checkpoint_url(slug), headers=self._headers())
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            offset = data.get("offset")
            if isinstance(offset, int) and offset >= 0:
                return offset
            logger.warning(
                "PrismOffsetCheckpoint: malformed checkpoint payload for %s: %r",
                slug, data,
            )
            return None
        except Exception as exc:
            logger.warning(
                "PrismOffsetCheckpoint: failed to load checkpoint for %s: %s",
                slug, exc,
            )
            return None

    def _save_offset(self, slug: str, offset: int) -> bool:
        """POST the current offset. Returns True on success."""
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(
                    self._checkpoint_url(slug),
                    headers=self._headers(),
                    json={"offset": offset},
                )
            resp.raise_for_status()
            return True
        except Exception as exc:
            logger.warning(
                "PrismOffsetCheckpoint: failed to save offset=%d for %s: %s",
                offset, slug, exc,
            )
            return False

    def _current_offset(self) -> Optional[int]:
        spider = self._spider
        if spider is None:
            return None
        offset = getattr(spider, "_prism_next_offset", None)
        if isinstance(offset, int) and offset >= 0:
            return offset
        return None

    # -- lifecycle -------------------------------------------------------

    def spider_opened(self, spider):
        self._spider = spider
        slug = self.spider_slug or getattr(spider, "name", "")
        if not slug:
            logger.warning("PrismOffsetCheckpoint: no spider slug, disabling")
            return

        # Spider arg start_offset wins over checkpoint — explicit beats remembered.
        explicit = getattr(spider, "start_offset", None)
        if explicit not in (None, "", "0", 0):
            logger.info(
                "PrismOffsetCheckpoint: explicit start_offset=%s set on %s; "
                "skipping checkpoint load",
                explicit, slug,
            )
        elif not hasattr(spider, "_prism_next_offset"):
            logger.info(
                "PrismOffsetCheckpoint: spider %s has no _prism_next_offset attr; "
                "skipping (extension is a no-op on non-Prism spiders)",
                slug,
            )
            return
        else:
            saved = self._load_offset(slug)
            if saved is None or saved == 0:
                logger.info(
                    "PrismOffsetCheckpoint: no checkpoint found for %s, starting from 0",
                    slug,
                )
            else:
                # Set on spider *before* start_requests runs. Scrapy fires
                # spider_opened synchronously before consuming start_requests,
                # so this assignment is visible to the first Prism refill.
                spider._prism_next_offset = saved
                self._last_saved = saved
                logger.info(
                    "PrismOffsetCheckpoint: resuming %s at offset=%d",
                    slug, saved,
                )

        # Start periodic save loop regardless of whether we loaded — the spider
        # is still expected to advance the offset and we want to checkpoint it.
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._save_loop,
            name="prism-checkpoint",
            daemon=True,
        )
        self._thread.start()

    def spider_closed(self, spider, reason):
        self._stop.set()
        # Final flush so the next run picks up where we left off, even if
        # the periodic save was mid-sleep at shutdown.
        offset = self._current_offset()
        slug = self.spider_slug or getattr(spider, "name", "")
        if offset is not None and slug and offset != self._last_saved:
            if self._save_offset(slug, offset):
                logger.info(
                    "PrismOffsetCheckpoint: final save offset=%d for %s "
                    "(reason=%s)",
                    offset, slug, reason,
                )
        if self._thread:
            self._thread.join(timeout=5)

    def _save_loop(self):
        slug = self.spider_slug or getattr(self._spider, "name", "")
        while not self._stop.wait(self.interval):
            offset = self._current_offset()
            if offset is None:
                continue
            if offset == self._last_saved:
                # No advance — skip to avoid churning the endpoint.
                continue
            if self._save_offset(slug, offset):
                self._last_saved = offset
