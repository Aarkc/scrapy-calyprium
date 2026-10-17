"""CalypriumTraceMiddleware: one trace per Scrapy request, from when it is yielded to when its callback is done.

Both a downloader and a spider middleware (register it in both):

    DOWNLOADER_MIDDLEWARES = {"scrapy_calyprium.middleware.tracing.CalypriumTraceMiddleware": 50}
    SPIDER_MIDDLEWARES = {"scrapy_calyprium.middleware.tracing.CalypriumTraceMiddleware": 50}

* A request gets its trace when a callback (or start_requests) yields it: that time is the start of its
  `spider.request` span, so the wait in Scrapy's queue is part of it. A follow-up request names the trace of the
  page it came from (`spider.parent_trace_id`).
* Every download attempt gets its own span id (downloader priority 50, before Veil, Spectre and Mimic), which the
  tracer's per-attempt span and every service call made for it (`traceparent`) use.
* When the callback has consumed the response, the `spider.parse` span (callback time, items, follow-ups) and the
  request's own span are handed to CalypriumRequestTracer, which sends them to Forge.

Nothing here talks to the network or ever raises into the crawl; without the tracer extension the ids are still
made, so the services' spans join up, but no spider span is sent.
"""
import logging
import time
from typing import Optional

from scrapy import Request

from scrapy_calyprium import _trace

logger = logging.getLogger(__name__)

PARSE_START = "calyprium_parse_start"
ATTEMPTS = "calyprium_attempts"
FIRST_ATTEMPT = "calyprium_first_attempt"


class CalypriumTraceMiddleware:
    def __init__(self, crawler=None):
        self.crawler = crawler
        self._tracer = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def _find_tracer(self):
        """The CalypriumRequestTracer extension, when it runs (found lazily: extensions load after middlewares)."""
        if self._tracer is None and self.crawler is not None:
            extensions = getattr(getattr(self.crawler, "extensions", None), "middlewares", None) or ()
            self._tracer = next((ext for ext in extensions if hasattr(ext, "record_trace_span")), False)
        return self._tracer or None

    # -- downloader --------------------------------------------------------
    def process_request(self, request, spider):
        if request.meta.get("_internal"):
            return None
        try:
            attempt = _trace.start_attempt(request)
            request.meta[ATTEMPTS] = int(request.meta.get(ATTEMPTS) or 0) + 1
            request.meta.setdefault(FIRST_ATTEMPT, attempt["start"])
        except Exception as exc:  # noqa: BLE001 -- tracing never breaks a crawl
            logger.debug("CalypriumTraceMiddleware: no attempt for %s: %s", request.url, exc)
        return None

    # -- spider ------------------------------------------------------------
    def process_start_requests(self, start_requests, spider):
        for item in start_requests:
            if isinstance(item, Request):
                _trace.request_trace(item)
            yield item

    def process_spider_input(self, response, spider):
        try:
            response.meta[PARSE_START] = time.time()
        except AttributeError:  # a response with no request
            pass
        return None

    def process_spider_output(self, response, result, spider):
        counts = {"items": 0, "requests": 0}
        parent = self._trace_id(response)
        try:
            for item in result:
                self._note(item, counts, parent)
                yield item
        except Exception as exc:
            self._finish(response, counts, exc)
            raise
        self._finish(response, counts)

    async def process_spider_output_async(self, response, result, spider):
        counts = {"items": 0, "requests": 0}
        parent = self._trace_id(response)
        try:
            async for item in result:
                self._note(item, counts, parent)
                yield item
        except Exception as exc:
            self._finish(response, counts, exc)
            raise
        self._finish(response, counts)

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def _trace_id(response) -> Optional[str]:
        trace = (getattr(response, "meta", None) or {}).get(_trace.TRACE_KEY) if _has_meta(response) else None
        return trace.get("trace_id") if isinstance(trace, dict) else None

    @staticmethod
    def _note(item, counts, parent: Optional[str]) -> None:
        if isinstance(item, Request):
            counts["requests"] += 1
            try:
                _trace.request_trace(item, parent_trace_id=parent)
            except Exception:  # noqa: BLE001
                pass
        else:
            counts["items"] += 1

    def _finish(self, response, counts, error: Optional[BaseException] = None) -> None:
        try:
            tracer = self._find_tracer()
            if tracer is None or not getattr(tracer, "enabled", False) or not _has_meta(response):
                return
            meta = response.meta
            trace = meta.get(_trace.TRACE_KEY)
            if not isinstance(trace, dict) or not trace.get("trace_id"):
                return
            now = time.time()
            request = response.request
            callback = getattr(request.callback, "__name__", None) or "parse"
            message = f"{type(error).__name__}: {error}"[:500] if error else None
            tracer.record_trace_span(
                "spider.parse", trace["trace_id"], _trace.new_span_id(), trace["span_id"],
                meta.get(PARSE_START, now), now,
                {"spider.callback": callback, "spider.items": counts["items"], "spider.requests": counts["requests"]},
                error=message,
            )
            first = meta.get(FIRST_ATTEMPT)
            tracer.record_trace_span(
                "spider.request", trace["trace_id"], trace["span_id"], None, trace.get("start", now), now, {
                    "url.full": request.url, "http.request.method": request.method,
                    "http.response.status_code": getattr(response, "status", 0),
                    "spider.callback": callback, "spider.depth": meta.get("depth"),
                    "spider.priority": request.priority, "spider.attempts": meta.get(ATTEMPTS),
                    "spider.queue_wait_ms": int((first - trace["start"]) * 1000) if first and trace.get("start") else None,
                    "spider.items": counts["items"], "spider.requests": counts["requests"],
                    "spider.route": meta.get("mimic_local_route") or (meta.get("calyprium_routing") or {}).get(
                        "routing_method"),
                    "spider.parent_trace_id": trace.get("parent_trace_id"),
                    "spider.fingerprint_id": meta.get("spectre_fingerprint_id"),
                    "spider.slot_id": meta.get("mimic_slot_id"),
                },
                error=message,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("CalypriumTraceMiddleware: spans for %s not recorded: %s", getattr(response, "url", "?"), exc)


def _has_meta(response) -> bool:
    try:
        return response is not None and response.meta is not None
    except AttributeError:
        return False
