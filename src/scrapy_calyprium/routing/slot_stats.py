"""Background reporter that batches per-slot stats from the local DomainCache
and POSTs them to Mimic /api/slot-stats/report.

AAR-17 follow-up. With local-first auto-routing the spider learns its own
per-slot rate cap, but Mimic loses cross-spider visibility. This reporter
gives Mimic enough data to keep its dashboards and admin endpoints honest.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from scrapy_calyprium.routing.domain_cache import DomainCache

logger = logging.getLogger(__name__)


class SlotStatsReporter:
    """Periodically POST a snapshot of the local DomainCache to Mimic.

    Uses prior counter values to compute deltas so each report is the
    activity since the last report (vs cumulative).
    """

    def __init__(
        self,
        *,
        cache: DomainCache,
        service_url: str,
        api_key: Optional[str] = None,
        service_secret: Optional[str] = None,
        user_id: Optional[str] = None,
        spider: Optional[str] = None,
        interval_seconds: float = 30.0,
        timeout: float = 10.0,
    ):
        self.cache = cache
        self.service_url = service_url.rstrip("/")
        self.api_key = api_key
        self.service_secret = service_secret
        self.user_id = user_id
        self.spider = spider
        self.interval_seconds = interval_seconds
        self.timeout = timeout

        self._client: Optional[httpx.AsyncClient] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # (domain, proxy_session_id) -> (last_success, last_block)
        self._last_counts: dict = {}

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
            h["X-API-Key"] = self.api_key
        if self.service_secret:
            h["X-Service-Secret"] = self.service_secret
        if self.user_id:
            h["X-User-Id"] = self.user_id
            h["X-Service-Name"] = "scrapy-calyprium"
        return h

    async def start(self):
        if self._task is not None:
            return
        self._client = httpx.AsyncClient(timeout=self.timeout)
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._task is None:
            return
        self._stop.set()
        try:
            await asyncio.wait_for(self._task, timeout=self.interval_seconds + 5)
        except asyncio.TimeoutError:
            self._task.cancel()
        self._task = None
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _run(self):
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self.interval_seconds,
                )
                return  # stop event was set
            except asyncio.TimeoutError:
                pass
            try:
                await self._report_once()
            except Exception as e:  # noqa: BLE001
                logger.warning("SlotStatsReporter: report failed: %s", e)

    def _build_batch(self) -> dict:
        entries = []
        for domain, entry in self.cache._entries.items():
            if entry.level != "cookies":
                continue
            for slot in entry.slots:
                key = (domain, slot.proxy_session_id)
                last_success, last_block = self._last_counts.get(key, (0, 0))
                d_success = max(0, slot.success_count - last_success)
                d_block = max(0, slot.block_count - last_block)
                self._last_counts[key] = (slot.success_count, slot.block_count)
                entries.append({
                    "domain": domain,
                    "proxy_session_id": slot.proxy_session_id,
                    "rpm": slot.requests_per_minute(),
                    "successes": d_success,
                    "blocks": d_block,
                    "learned_rpm_cap": entry.learned_rpm_cap,
                })
        return {"spider": self.spider, "entries": entries}

    async def _report_once(self):
        batch = self._build_batch()
        if not batch["entries"]:
            return
        assert self._client is not None
        url = f"{self.service_url}/api/slot-stats/report"
        try:
            resp = await self._client.post(url, json=batch, headers=self._headers())
        except httpx.HTTPError as exc:
            logger.debug("SlotStatsReporter: transport error: %s", exc)
            return
        if resp.status_code >= 500:
            logger.warning(
                "SlotStatsReporter: Mimic returned %d for %d entries",
                resp.status_code, len(batch["entries"]),
            )
            return
        if resp.status_code == 200:
            logger.debug(
                "SlotStatsReporter: reported %d entries to Mimic",
                len(batch["entries"]),
            )
