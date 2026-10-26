"""The spider's share of a capacity-capped proxy pool (the spider portfolio): a spider-wide pace, and backpressure
when the proxy gateway has no free connection.

The platform shares a proxy vendor capped at so many concurrent connections across its spiders, by group weight,
minimum and maximum. The gateway queues a CONNECT until a connection is free (~15 s); when none comes in time it
refuses the request as ``pool_busy`` (see ``scrapy_calyprium._pool_busy``). Two downloader middlewares:

``PaceMiddleware`` (priority 120) holds the spider to ``CALYPRIUM_PACE_PPM`` pages a minute, all domains together,
with a token bucket of ``CALYPRIUM_PACE_BURST`` tokens (default 1). It waits in process_request. That is after
Scrapy's OffsiteMiddleware (50) and RobotsTxtMiddleware (100), which drop requests there, so a dropped request
spends no token. It is also before MimicBrowserMiddleware (200), which answers most requests itself on the local
route. Those requests never reach a download slot, so DOWNLOAD_DELAY and the per-domain concurrency never apply
to them; only CONCURRENT_REQUESTS does, and a waiting request keeps its place in it. Every attempt is paced,
retries and requeues included. Without CALYPRIUM_PACE_PPM (or with 0) it is not installed. Stats:
``calyprium/pace/delayed``, ``calyprium/pace/wait_seconds``.

``PoolBusyMiddleware`` (priority 950) requeues a request the gateway refused for a busy pool. It recognises the
TunnelError of an HTTPS download, a plain-HTTP 503 carrying ``X-Calyprium-Proxy-Error: pool_busy``, the local
route's PoolBusyError and any other download exception naming ``pool_busy``. MimicBrowserMiddleware raises
PoolBusyError from process_request, and Scrapy hands an exception raised there to every process_exception.
process_exception and process_response run from the highest priority down. At 950 this one sees a refusal
before HttpCacheMiddleware (900) can store it, DownloaderStats (850) count it, CookiesMiddleware (700) keep its
cookies, Scrapy's RetryMiddleware (550) retry it or MimicBrowserMiddleware (200) retry or trace it. None of them
should, since the refusal is not the site's answer.

The request goes again unfiltered after a jittered backoff: between CALYPRIUM_POOL_BUSY_DELAY and a ceiling that
doubles with each of its refusals, up to CALYPRIUM_POOL_BUSY_MAX_DELAY. Its retry counts (``retry_times``, the
Calyprium retry's) are untouched, and ``meta["calyprium_pool_busy"]`` counts its refusals. After
CALYPRIUM_POOL_BUSY_MAX_REQUEUES refusals the usual error handling takes over. With CALYPRIUM_POOL_AIMD (on by
default) it also backs the crawl's concurrency off: a refusal lowers the downloader's CONCURRENT_REQUESTS to 3/4
(at most once every 5 s, never below 1), and 20 responses in a row without one raise it by 1, up to the
configured value. It is always installed, and it does nothing without refusals. Stats: ``calyprium/pool_busy``
(refusals), ``calyprium/pool_busy/requeued``, ``calyprium/pool_busy/max_reached``,
``calyprium/pool_busy/wait_seconds``, ``calyprium/pool_busy/concurrency``, ``calyprium/pool_busy/concurrency_min``.
A requeue is not a new request, a failure or a traced attempt; CalypriumRunStats reports the refusals as
``pool_busy_count``.

    DOWNLOADER_MIDDLEWARES = {
        "scrapy_calyprium.middleware.portfolio.PaceMiddleware": 120,
        "scrapy_calyprium.middleware.portfolio.PoolBusyMiddleware": 950,
    }
"""
import logging
import math
import random
import time
from typing import Any, Callable, Optional

from scrapy.exceptions import NotConfigured

from scrapy_calyprium import _pool_busy, _trace, _wait
from scrapy_calyprium._pool_busy import PoolBusyError
from scrapy_calyprium.middleware.tracing import FIRST_ATTEMPT

logger = logging.getLogger(__name__)

__all__ = ["PaceMiddleware", "PoolBusyError", "PoolBusyMiddleware", "TokenBucket"]

# AIMD: a refusal multiplies the crawl's concurrency by this (once per interval), a run of this many
# responses without one adds 1 back.
AIMD_DECREASE = 0.75
AIMD_DECREASE_INTERVAL_S = 5.0
AIMD_INCREASE_AFTER = 20
# INFO logs of refusals and concurrency changes: at most one per interval.
BUSY_LOG_INTERVAL_S = 30.0
CONCURRENCY_LOG_INTERVAL_S = 10.0


def _float_setting(settings: Any, name: str, default: float = 0.0) -> float:
    """A float setting; unset, empty or unreadable is ``default`` (forge may render an unset value as None)."""
    try:
        value = settings.get(name)
        return default if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return default


def _bool_setting(settings: Any, name: str, default: bool) -> bool:
    try:
        value = settings.get(name)
        return default if value is None or value == "" else settings.getbool(name, default)
    except (TypeError, ValueError):
        return default


def _restart_attempt(request: Any) -> None:
    """The download attempt starts when the pace lets it go. CalypriumTraceMiddleware (50) started it before the
    wait, which is time in the queue (the request's ``spider.queue_wait_ms`` on a first attempt), not the
    download's."""
    meta = getattr(request, "meta", None)
    attempt = meta.get(_trace.ATTEMPT_KEY) if isinstance(meta, dict) else None
    if not isinstance(attempt, dict) or "start" not in attempt:
        return
    now = time.time()
    if meta.get(FIRST_ATTEMPT) == attempt["start"]:
        meta[FIRST_ATTEMPT] = now
    attempt["start"] = now


class TokenBucket:
    """``rate`` tokens a second, at most ``burst`` saved up. ``take()`` hands out the next token and returns the
    seconds to wait for it: a token may be owed, so takers queue in the order they came and concurrent
    requests leave ``1 / rate`` apart."""

    def __init__(self, rate: float, burst: int = 1, clock: Callable[[], float] = time.monotonic):
        if not rate > 0:
            raise ValueError(f"rate must be positive, not {rate!r}")
        self.rate = float(rate)
        self.burst = max(1, int(burst))
        self.clock = clock
        self._tokens = float(self.burst)
        self._at = clock()

    def take(self) -> float:
        now = self.clock()
        self._tokens = min(float(self.burst), self._tokens + max(0.0, now - self._at) * self.rate)
        self._at = now
        self._tokens -= 1.0
        return 0.0 if self._tokens >= 0 else -self._tokens / self.rate


class PaceMiddleware:
    """Spider-wide pages per minute (CALYPRIUM_PACE_PPM, burst CALYPRIUM_PACE_BURST); see the module's notes."""

    def __init__(self, ppm: float, burst: int = 1, stats: Any = None,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Optional[Callable[[float], Any]] = None):
        self.ppm = float(ppm)
        self.bucket = TokenBucket(self.ppm / 60.0, burst, clock)
        self.stats = stats
        self._sleep = sleep or _wait.sleep

    @classmethod
    def from_crawler(cls, crawler):
        ppm = _float_setting(crawler.settings, "CALYPRIUM_PACE_PPM")
        if not (ppm > 0 and math.isfinite(ppm)):
            raise NotConfigured("CALYPRIUM_PACE_PPM is not set")
        burst = int(_float_setting(crawler.settings, "CALYPRIUM_PACE_BURST", 1) or 1)
        middleware = cls(ppm, burst, stats=getattr(crawler, "stats", None))
        logger.info("PaceMiddleware: %g pages/min spider-wide (burst %d)", ppm, middleware.bucket.burst)
        return middleware

    async def process_request(self, request, spider):
        if request.meta.get("_internal"):
            return None  # the SDK's own calls (Prism's API): no page, no proxy
        wait = self.bucket.take()
        if wait > 0:
            if self.stats is not None:
                self.stats.inc_value("calyprium/pace/delayed")
                self.stats.inc_value("calyprium/pace/wait_seconds", round(wait, 3))
            await self._sleep(wait)
            _restart_attempt(request)
        return None


class PoolBusyMiddleware:
    """Requeue what the proxy gateway refused for a busy pool, and back the concurrency off; see the module's
    notes."""

    def __init__(self, crawler: Any = None, *, max_requeues: int = 30, delay: float = 2.0,
                 max_delay: float = 10.0, aimd: bool = True,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Optional[Callable[[float], Any]] = None,
                 rng: Optional[random.Random] = None):
        self.crawler = crawler
        self.stats = getattr(crawler, "stats", None)
        settings = getattr(crawler, "settings", None)
        self.max_requeues = max(0, int(max_requeues))
        self.delay = max(0.0, float(delay))
        self.max_delay = max(self.delay, float(max_delay))
        self.aimd = bool(aimd)
        # the concurrency AIMD climbs back to: the configured CONCURRENT_REQUESTS (the downloader's start)
        self.ceiling = max(1, settings.getint("CONCURRENT_REQUESTS", 16)) if settings is not None else 16
        self._clock = clock
        self._sleep = sleep or _wait.sleep
        self._random = rng or random.Random()
        self._successes = 0
        self._decreased_at: Optional[float] = None
        self._busy_unlogged = 0
        self._busy_logged_at: Optional[float] = None
        self._gave_up_logged_at: Optional[float] = None
        self._concurrency_logged_at: Optional[float] = None
        self._concurrency_logged: Optional[int] = None

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            crawler,
            max_requeues=int(_float_setting(settings, "CALYPRIUM_POOL_BUSY_MAX_REQUEUES", 30)),
            delay=_float_setting(settings, "CALYPRIUM_POOL_BUSY_DELAY", 2.0),
            max_delay=_float_setting(settings, "CALYPRIUM_POOL_BUSY_MAX_DELAY", 10.0),
            aimd=_bool_setting(settings, "CALYPRIUM_POOL_AIMD", True),
        )

    # -- downloader --------------------------------------------------------
    async def process_response(self, request, response, spider):
        if request.meta.get("_internal"):
            return response
        if not _pool_busy.is_pool_busy_response(response):
            self._succeeded()
            return response
        requeue = await self._requeue(
            request, f"HTTP {response.status} {_pool_busy.PROXY_ERROR_HEADER}: {_pool_busy.POOL_BUSY}")
        return requeue if requeue is not None else response

    async def process_exception(self, request, exception, spider):
        if request.meta.get("_internal") or not _pool_busy.is_pool_busy_error(exception):
            return None
        return await self._requeue(request, exception)

    # -- requeue -------------------------------------------------------------
    async def _requeue(self, request, why: Any):
        """The request again after a backoff, its retries untouched; None once it was refused
        max_requeues times (the refusal goes on to the usual error handling)."""
        self._inc("calyprium/pool_busy")
        self._backoff()
        try:
            busy = max(0, int(request.meta.get(_pool_busy.BUSY_COUNT) or 0))
        except (TypeError, ValueError):
            busy = 0
        if busy >= self.max_requeues:
            self._inc("calyprium/pool_busy/max_reached")
            now = self._clock()
            if self._gave_up_logged_at is None or now - self._gave_up_logged_at >= BUSY_LOG_INTERVAL_S:
                self._gave_up_logged_at = now
                logger.warning(
                    "PoolBusyMiddleware: %s was refused %d times for a busy proxy pool; handing it to the "
                    "usual error handling", request.url, busy + 1)
            return None
        delay = self._delay(busy + 1)
        self._inc("calyprium/pool_busy/requeued")
        self._inc("calyprium/pool_busy/wait_seconds", round(delay, 3))
        self._note_busy(request, why, busy + 1, delay)
        if delay > 0:
            await self._sleep(delay)
        requeue = request.replace(dont_filter=True)
        requeue.meta[_pool_busy.BUSY_COUNT] = busy + 1
        requeue.meta[_pool_busy.REQUEUED] = True
        return requeue

    def _delay(self, refusals: int) -> float:
        """Seconds before the request's next try: jittered between the base delay and a ceiling that doubles
        with each refusal (capped at max_delay)."""
        ceiling = min(self.max_delay, self.delay * (2 ** min(refusals, 16)))
        return self._random.uniform(self.delay, max(self.delay, ceiling))

    def _note_busy(self, request, why: Any, refusals: int, delay: float) -> None:
        self._busy_unlogged += 1
        now = self._clock()
        if self._busy_logged_at is not None and now - self._busy_logged_at < BUSY_LOG_INTERVAL_S:
            logger.debug("PoolBusyMiddleware: %s refused for a busy proxy pool (%s); again in %.1fs (refusal %d)",
                         request.url, why, delay, refusals)
            return
        since = f" in the last {now - self._busy_logged_at:.0f}s" if self._busy_logged_at is not None else ""
        logger.info(
            "PoolBusyMiddleware: the proxy pool is busy: %d request(s) refused%s, requeued without spending "
            "their retries (%s again in %.1fs, refusal %d)",
            self._busy_unlogged, since, request.url, delay, refusals)
        self._busy_logged_at = now
        self._busy_unlogged = 0

    # -- AIMD ----------------------------------------------------------------
    def _downloader(self):
        return getattr(getattr(self.crawler, "engine", None), "downloader", None)

    def _backoff(self) -> None:
        """A refusal: the concurrency to 3/4 (once per AIMD_DECREASE_INTERVAL_S, never below 1)."""
        self._successes = 0
        downloader = self._downloader() if self.aimd else None
        if downloader is None:
            return
        now = self._clock()
        if self._decreased_at is not None and now - self._decreased_at < AIMD_DECREASE_INTERVAL_S:
            return
        current = int(downloader.total_concurrency)
        lowered = max(1, int(current * AIMD_DECREASE))
        if lowered >= current:
            return
        self._decreased_at = now
        downloader.total_concurrency = lowered
        self._concurrency_changed(current, lowered)

    def _succeeded(self) -> None:
        """A response the gateway let through: after AIMD_INCREASE_AFTER in a row, the concurrency up by 1."""
        downloader = self._downloader() if self.aimd else None
        if downloader is None:
            return
        current = int(downloader.total_concurrency)
        if current >= self.ceiling:
            self._successes = 0
            return
        self._successes += 1
        if self._successes < AIMD_INCREASE_AFTER:
            return
        self._successes = 0
        downloader.total_concurrency = current + 1
        self._concurrency_changed(current, current + 1)

    def _concurrency_changed(self, old: int, new: int) -> None:
        if self.stats is not None:
            self.stats.set_value("calyprium/pool_busy/concurrency", new)
            self.stats.min_value("calyprium/pool_busy/concurrency_min", new)
        now = self._clock()
        if (new < self.ceiling and self._concurrency_logged_at is not None
                and now - self._concurrency_logged_at < CONCURRENCY_LOG_INTERVAL_S):
            return
        logger.info(
            "PoolBusyMiddleware: concurrency %d -> %d (%s; CONCURRENT_REQUESTS %d)",
            self._concurrency_logged if self._concurrency_logged is not None else old, new,
            "the proxy pool is busy" if new < old else "the proxy pool has room", self.ceiling)
        self._concurrency_logged_at = now
        self._concurrency_logged = new

    def _inc(self, key: str, count: float = 1) -> None:
        if self.stats is not None:
            self.stats.inc_value(key, count)
