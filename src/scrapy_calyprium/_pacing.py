"""Scrapy's per-domain download rules for the requests MimicBrowserMiddleware answers itself.

Scrapy applies DOWNLOAD_DELAY (randomised by RANDOMIZE_DOWNLOAD_DELAY), AutoThrottle's delay,
CONCURRENT_REQUESTS_PER_DOMAIN / _PER_IP and DOWNLOAD_SLOTS in its downloader's slot, and a request
reaches the slot only after every downloader middleware passed it on. A response the middleware
returns (the local route, Mimic's /api/fetch) skipped all of them: only the global
CONCURRENT_REQUESTS held, so a spider declaring a 3 s delay sent requests back to back.

``SlotPacer`` puts each such fetch through the slot Scrapy would have used (the downloader's own
``_get_slot``: its key, concurrency and delay): a fetch starts no sooner than the slot's delay after
the previous one and no more run at once than the slot's concurrency. The slot's ``lastseen`` is
shared with the downloader, so requests Scrapy downloads itself keep the same spacing.
MIMIC_PACING=False turns it off.
"""
import asyncio
import contextlib
import logging
import time
from typing import Any, Dict, Optional, Tuple

from scrapy_calyprium import _wait

logger = logging.getLogger(__name__)

# meta key the downloader reads the slot from (Downloader.DOWNLOAD_SLOT) and AutoThrottle finds it by
DOWNLOAD_SLOT = "download_slot"
# how often a fetch waiting for a free place in its slot looks again
_POLL_S = 0.05


class SlotPacer:
    def __init__(self, downloader: Any):
        self.downloader = downloader
        self._gates: Dict[str, asyncio.Lock] = {}
        self._in_flight: Dict[str, int] = {}

    def _slot(self, request, spider) -> Optional[Tuple[str, Any]]:
        try:
            return self.downloader._get_slot(request, spider)
        except Exception as exc:  # noqa: BLE001 — pacing must never fail a fetch
            logger.debug("SlotPacer: no download slot for %s: %s", request.url, exc)
            return None

    @contextlib.asynccontextmanager
    async def paced(self, request, spider):
        """Hold the request until its slot allows a fetch, then count it in flight until the
        block ends. Sets the request's ``download_slot`` meta as the downloader does."""
        found = self._slot(request, spider)
        if found is None:
            yield
            return
        key, slot = found
        gate = self._gates.setdefault(key, asyncio.Lock())
        async with gate:  # fetches of one slot start in order, one at a time
            while self._in_flight.get(key, 0) >= max(1, int(slot.concurrency or 1)):
                await _wait.sleep(_POLL_S)
            delay = slot.download_delay()
            if delay:
                wait = slot.lastseen + delay - time.time()
                if wait > 0:
                    await _wait.sleep(wait)
            slot.lastseen = time.time()
            self._in_flight[key] = self._in_flight.get(key, 0) + 1
        request.meta[DOWNLOAD_SLOT] = key
        try:
            yield
        finally:
            self._in_flight[key] -= 1
