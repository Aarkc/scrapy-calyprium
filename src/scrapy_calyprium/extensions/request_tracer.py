"""CalypriumRequestTracer — per-URL trace spans to ClickHouse via Forge.

Emits one top-level span per download attempt the spider makes. Forge prices
each top-level span into ``usage_events`` (Phase 5 metering), so every
request a spider pays for must produce exactly one:

* **Local-first** (``SpiderAutoRouter.fetch()``): the router records the span
  itself (it knows the slot, egress IP and cookie path) and the Mimic
  middleware tags the request with ``meta["calyprium_trace_id"]`` so the
  response is not traced a second time.
* **Everything else** (Mimic ``/api/fetch`` and browser sessions, the Veil
  proxy path, plain downloads): traced from Scrapy's ``response_received``
  signal. The routing comes from ``meta["calyprium_routing"]`` (filled by the
  Mimic middleware from mimic's ``routing`` object) or, without it, from the
  download path: ``veil_proxy`` (tier ``fast``, or ``residential`` when
  ``VEIL_PROXY_TYPE`` is residential) or ``direct``.
* Transport errors (timeouts, resets) are traced by the Mimic middleware via
  :meth:`CalypriumRequestTracer.trace_exception`, one span per failed attempt,
  and so is a response the middleware retries (``response_received`` never
  fires for it) via :meth:`CalypriumRequestTracer.trace_response`.

A span's outcome (``status``) is the attempt's diagnosis (the fetch layer's
shared classifier, see ``_outcome``): ``blocked`` iff its class is a block,
``success`` for a fetched page, ``error`` / ``timeout`` otherwise. Each span
also carries ``failure_class``, ``vendor`` and ``evidence`` (the diagnosis),
``rung`` (Mimic's ladder rung, when it reports one) and ``attempt`` (the
Calyprium retries made before it). An older Forge ignores the extra fields.

Spans are buffered (bounded: beyond ``MAX_BUFFER`` new spans are dropped and
counted) and POSTed to Forge's ``/jobs/spiders/{slug}/runs/{n}/traces``
endpoint from a background thread every few seconds. A Forge outage only
loses spans; it never fails or slows the crawl.

Enabled via settings:
    EXTENSIONS = {
        "scrapy_calyprium.extensions.request_tracer.CalypriumRequestTracer": 501,
    }

Required settings (same as CalypriumRunStats):
    FORGE_API_URL, CALYPRIUM_API_KEY (or legacy FORGE_SERVICE_SECRET),
    RECRAWL_SPIDER_SLUG,
    RECRAWL_USER_ID, SPIDER_RUN_NUMBER
Optional: VEIL_PROXY_TYPE (tier of the Veil proxy path).
"""
from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx
from scrapy import signals
from scrapy.crawler import Crawler

from scrapy_calyprium import _outcome, _trace
from scrapy_calyprium._forge import ForgeAuth

logger = logging.getLogger(__name__)

# Max spans to buffer before forcing a flush
BATCH_SIZE = 100
FLUSH_INTERVAL = 5.0  # seconds
# Hard cap on buffered spans (memory bound if Forge is slow or unreachable).
MAX_BUFFER = 10_000

TIER_IDS = ("fast", "residential", "browser")
NETWORKS = ("datacenter", "residential", "direct")
_RESIDENTIAL_PROXY_TYPES = ("residential", "residential_rotating", "mobile", "isp")
_HTTP_ENGINES = ("", "httpcloak", "http", "curl_cffi", "scrapy")
_BROWSER_METHODS = ("engine_fallback", "mimic", "page_load")
_BLOCK_STATUSES = (401, 403, 429, 503)


def _is_browser(method: str, engine: str = "") -> bool:
    m = (method or "").strip().lower()
    if m.startswith("browser") or m in _BROWSER_METHODS:
        return True
    if m:
        return False  # httpcloak*, cookie_*, veil_proxy, direct, ...
    return (engine or "").strip().lower() not in _HTTP_ENGINES


def tier_for(method: str, engine: str = "", network: str = "") -> str:
    """Tier for a fetch: a browser engine is ``browser``; otherwise the
    network decides. ``""`` when the network is unknown (Forge then derives
    it from the run's routing snapshot)."""
    if _is_browser(method, engine):
        return "browser"
    if network == "residential":
        return "residential"
    if network in ("datacenter", "direct"):
        return "fast"
    return ""


def _escalated_tier(value: str, network: str) -> str:
    """Mimic reports ``escalated_from`` as the first *method* it tried; spans
    carry the tier that method ran on (best effort: assumes the final
    attempt's network)."""
    v = (value or "").strip().lower()
    if not v or v in TIER_IDS:
        return v
    if _is_browser(v):
        return "browser"
    return "residential" if network == "residential" else "fast"


def _outcome_of(status_code: int, blocked: bool = False,
                diagnosis: Optional[Dict[str, Any]] = None) -> str:
    """A span's status: the diagnosis when there is one, else the status code (and a
    routing ``blocked`` flag)."""
    if diagnosis:
        return _outcome.outcome(diagnosis, status_code)
    if blocked or status_code in _BLOCK_STATUSES:
        return "blocked"
    if 200 <= status_code < 400:
        return "success"
    return "error"


class CalypriumRequestTracer:
    def __init__(
        self,
        forge_url: str,
        service_secret: str,
        user_id: str,
        spider_slug: str,
        run_number: Optional[int],
        api_key: Optional[str] = None,
        proxy_type: Optional[str] = None,
    ):
        self.forge_url = forge_url.rstrip("/")
        self.service_secret = service_secret
        self.user_id = user_id
        self.auth = ForgeAuth(api_key, service_secret, user_id)
        self.spider_slug = spider_slug
        self.run_number = run_number

        self._buffer: List[Dict] = []
        # Spans of the request's trace that are not priced attempts (spider.request, spider.parse), POSTed to
        # Forge's /spans; dropped for good once Forge says it has no such endpoint (an older Forge).
        self._spans: List[Dict] = []
        self.spans_dropped = 0
        self._spans_unsupported = False
        self._lock = threading.Lock()
        self._stop = threading.Event()
        # Set when the buffer fills, so the flush thread POSTs early. The POST
        # never happens on the caller's (reactor) thread (AAR-64).
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.dropped = 0

        # Veil proxy path: tier/network from the run's proxy type.
        pt = (proxy_type or "").strip().lower()
        self.veil_network = "residential" if pt in _RESIDENTIAL_PROXY_TYPES else "datacenter"
        self.veil_tier = "residential" if self.veil_network == "residential" else "fast"

    @property
    def enabled(self) -> bool:
        return bool(self.spider_slug and self.run_number)

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        settings = crawler.settings
        forge_url = settings.get("FORGE_API_URL", "http://calyprium-backend:8000")
        secret = settings.get("FORGE_SERVICE_SECRET", "")
        api_key = settings.get("CALYPRIUM_API_KEY") or os.getenv("CALYPRIUM_API_KEY", "")
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

        ext = cls(
            forge_url, secret, user_id, slug or "", run_number, api_key=api_key,
            proxy_type=settings.get("VEIL_PROXY_TYPE"),
        )

        if not slug or not run_number:
            logger.info(
                "CalypriumRequestTracer: disabled (slug=%r, run_number=%r)",
                slug, run_number,
            )
            return ext

        crawler.signals.connect(ext.spider_opened, signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signals.spider_closed)
        crawler.signals.connect(ext.response_received, signals.response_received)
        return ext

    # -- Public API (called by SpiderAutoRouter / MimicBrowserMiddleware) ---

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
        tier: str = "",
        network: str = "",
        escalated_from: str = "",
        escalation_reason: str = "",
        trace_ids: Optional[tuple] = None,
        start: Optional[float] = None,
        diagnosis: Optional[Dict[str, Any]] = None,
        rung: Optional[int] = None,
        attempt: Optional[int] = None,
    ) -> None:
        """Buffer a span. Thread-safe — called from Scrapy's async context.

        ``tier``/``network``/``escalated_from``/``escalation_reason`` are sent
        only when known; Forge derives what is missing. ``trace_ids`` (trace id,
        attempt span id, request span id) place the attempt in its request's
        trace (default: the attempt current in this task, see ``_trace``);
        ``start`` is when it began (epoch seconds). ``diagnosis`` (the attempt's,
        see ``_outcome``) gives ``failure_class``/``vendor``/``evidence``;
        ``rung`` is Mimic's ladder rung when known; ``attempt`` the Calyprium
        retries before this attempt (default: the one current in this task)."""
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
        for key, value in (
            ("tier", tier), ("network", network),
            ("escalated_from", escalated_from), ("escalation_reason", escalation_reason),
        ):
            if value:
                span[key] = value
        if isinstance(diagnosis, dict) and diagnosis.get("class"):
            span["failure_class"] = str(diagnosis["class"])
            for key in ("vendor", "evidence"):
                if diagnosis.get(key):
                    span[key] = str(diagnosis[key])[:200]
        if rung is not None:
            try:
                span["rung"] = int(rung)
            except (TypeError, ValueError):
                pass
        span["attempt"] = int(attempt if attempt is not None else _outcome.CURRENT_ATTEMPT.get())
        ids = trace_ids or _trace.ATTEMPT.get()
        if ids and len(ids) == 3:
            began = start if start is not None else time.time() - duration_ms / 1000
            span.update(otel_trace_id=ids[0], otel_span_id=ids[1], otel_parent_span_id=ids[2],
                        otel_start=datetime.fromtimestamp(began, timezone.utc).isoformat())
        with self._lock:
            if len(self._buffer) >= MAX_BUFFER:
                self.dropped += 1
                return
            self._buffer.append(span)
            full = len(self._buffer) >= BATCH_SIZE
        if full:
            self._wake.set()

    def record_trace_span(
        self, name: str, trace_id: str, span_id: str, parent_span_id: Optional[str],
        start: float, end: float, attributes: Optional[Dict[str, Any]] = None, error: Optional[str] = None,
    ) -> None:
        """Buffer a span of a request's trace that is not a priced attempt (``spider.request``,
        ``spider.parse``). Thread-safe; never raises."""
        if not self.enabled or self._spans_unsupported:
            return
        span = {
            "name": name, "trace_id": trace_id, "span_id": span_id, "parent_span_id": parent_span_id or "",
            "start": datetime.fromtimestamp(start, timezone.utc).isoformat(),
            "end": datetime.fromtimestamp(max(end, start), timezone.utc).isoformat(),
            "attributes": {k: v for k, v in (attributes or {}).items()
                           if v is not None and v != "" and isinstance(v, (str, bool, int, float))},
        }
        if error:
            span["error"] = error[:500]
        with self._lock:
            if len(self._spans) >= MAX_BUFFER:
                self.spans_dropped += 1
                return
            self._spans.append(span)
            full = len(self._spans) >= BATCH_SIZE
        if full:
            self._wake.set()

    # -- Download-path tracing (Mimic /api/fetch + sessions, Veil, plain) --

    def response_received(self, response, request, spider=None) -> None:
        """``response_received`` signal: one span per response not already
        traced by SpiderAutoRouter. Never raises."""
        try:
            self.trace_response(request, response)
        except Exception as exc:  # noqa: BLE001 — tracing must never break a crawl
            logger.debug("CalypriumRequestTracer: trace failed for %s: %s",
                         getattr(request, "url", "?"), exc)

    def trace_response(self, request, response) -> None:
        """One span for a response (from ``response_received``, or from the Mimic
        middleware for a response it retries instead of returning)."""
        if not self._should_trace(request):
            return
        if "cached" in (getattr(response, "flags", None) or ()):
            return  # HTTP cache hit: no request went out
        routing = self._routing(request)
        status_code = int(getattr(response, "status", 0) or 0)
        body = getattr(response, "body", b"") or b""
        diagnosis = _outcome.for_response(request, response)
        status = _outcome_of(status_code, bool(routing.get("blocked")), diagnosis)
        self._record(
            request, routing,
            status=status,
            status_code=status_code,
            response_bytes=len(body) or int(routing.get("bytes") or 0),
            # Mimic's own error (x-mimic-error) explains a failed attempt.
            error_message="" if status == "success"
            else str(request.meta.get(_outcome.FETCH_ERROR) or "")[:500],
            diagnosis=diagnosis,
        )

    def trace_exception(self, request, exception: BaseException) -> None:
        """One span for a download attempt that failed in transport (called by
        the Mimic middleware for each failed attempt). Never raises."""
        try:
            if not self._should_trace(request):
                return
            name = type(exception).__name__
            timed_out = any(k in name.lower() for k in ("timeout", "timedout"))
            self._record(
                request, self._routing(request),
                status="timeout" if timed_out else "error",
                status_code=0, response_bytes=0,
                error_message=f"{name}: {exception}"[:500],
                diagnosis=_outcome.of_error(exception),
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("CalypriumRequestTracer: exception trace failed: %s", exc)

    def _should_trace(self, request) -> bool:
        if not self.enabled or request is None:
            return False
        meta = getattr(request, "meta", None) or {}
        # _internal: SDK plumbing requests. calyprium_trace_id: SpiderAutoRouter
        # already recorded this attempt.
        return not (meta.get("_internal") or meta.get("calyprium_trace_id"))

    def _routing(self, request) -> Dict[str, Any]:
        """Routing of one attempt: Mimic's (via meta) or the download path."""
        meta = request.meta
        mimic = meta.get("calyprium_routing")
        if isinstance(mimic, dict) and mimic:
            method = str(mimic.get("routing_method") or "")
            engine = str(mimic.get("engine") or "")
            network = str(mimic.get("network") or "").strip().lower()
            if network not in NETWORKS:
                network = ""
            tier = str(mimic.get("tier") or "").strip().lower()
            if tier not in TIER_IDS:
                tier = tier_for(method, engine, network)
            return {
                "routing_method": method, "engine": engine, "network": network,
                "tier": tier,
                "escalated_from": _escalated_tier(str(mimic.get("escalated_from") or ""), network),
                "escalation_reason": str(mimic.get("escalation_reason") or ""),
                "blocked": bool(mimic.get("blocked")),
                "bytes": mimic.get("bytes") or 0,
                "rung": mimic.get("rung"),
            }
        if meta.get("proxy"):
            return {"routing_method": "veil_proxy", "engine": "scrapy",
                    "network": self.veil_network, "tier": self.veil_tier}
        return {"routing_method": "direct", "engine": "scrapy",
                "network": "direct", "tier": "fast"}

    def _record(self, request, routing: Dict[str, Any], *, status: str,
                status_code: int, response_bytes: int, error_message: str = "",
                diagnosis: Optional[Dict[str, Any]] = None) -> None:
        meta = request.meta
        t0 = meta.get("_calyprium_t0")
        if t0 is not None:
            duration_ms = int((time.monotonic() - float(t0)) * 1000)
        else:
            duration_ms = int(float(meta.get("download_latency") or 0) * 1000)
        try:
            domain = urlparse(request.url).netloc
        except ValueError:
            domain = ""
        attempt = meta.get(_trace.ATTEMPT_KEY) or {}
        self.record_span(
            trace_id=uuid.uuid4().hex,
            trace_ids=_trace.attempt_ids(request),
            start=attempt.get("start"),
            url=request.url,
            domain=domain,
            status=status,
            status_code=status_code,
            duration_ms=max(0, duration_ms),
            routing_method=routing.get("routing_method", ""),
            engine=routing.get("engine", ""),
            response_bytes=response_bytes,
            error_message=error_message,
            tier=routing.get("tier", ""),
            network=routing.get("network", ""),
            escalated_from=routing.get("escalated_from", ""),
            escalation_reason=routing.get("escalation_reason", ""),
            diagnosis=diagnosis,
            rung=routing.get("rung"),
            attempt=_outcome.attempt_number(request),
        )

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
        if self.dropped:
            logger.warning(
                "CalypriumRequestTracer: dropped %d spans (buffer full; Forge slow "
                "or unreachable)", self.dropped,
            )
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=15)  # the loop does the final flush
        else:
            self._flush()

    def _flush_loop(self):
        while not self._stop.is_set():
            self._wake.wait(FLUSH_INTERVAL)
            self._wake.clear()
            self._flush()
        self._flush()

    def _flush(self):
        with self._lock:
            batch = self._buffer[:]
            self._buffer.clear()
            spans = self._spans[:]
            self._spans.clear()
        if batch:
            self._post_batch(batch)
        if spans:
            self._post_spans(spans)

    def _post_batch(self, batch: List[Dict]):
        if not batch or not self.run_number:
            return
        url = (
            f"{self.forge_url}/jobs/spiders/{self.spider_slug}/"
            f"runs/{self.run_number}/traces"
        )
        try:
            self.auth.call(lambda h: httpx.post(
                url, json={"spans": batch}, headers=h, timeout=10.0,
            ))
        except Exception as exc:
            logger.debug("CalypriumRequestTracer POST failed: %s", exc)

    def _post_spans(self, spans: List[Dict]):
        if not spans or not self.run_number or self._spans_unsupported:
            return
        url = (
            f"{self.forge_url}/jobs/spiders/{self.spider_slug}/"
            f"runs/{self.run_number}/spans"
        )
        try:
            response = self.auth.call(lambda h: httpx.post(
                url, json={"spans": spans}, headers=h, timeout=10.0,
            ))
            if getattr(response, "status_code", 0) in (404, 405):
                self._spans_unsupported = True
                logger.info("CalypriumRequestTracer: Forge takes no request spans (older Forge); not sending them")
        except Exception as exc:
            logger.debug("CalypriumRequestTracer spans POST failed: %s", exc)
