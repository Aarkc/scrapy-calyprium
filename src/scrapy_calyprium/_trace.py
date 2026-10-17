"""Trace context of the spider's requests, in W3C terms, without the OpenTelemetry SDK.

Each Scrapy request is one trace (its download attempts, any challenge solve, and the callback that parses it), so
the platform can show what happened to it end to end. The ids live in the request's meta, where every middleware
can read them:

    meta["calyprium_trace"]    {"trace_id", "span_id" (the request's own span), "start", "parent_trace_id"}
    meta["calyprium_attempt"]  {"span_id", "start"}: the download attempt in progress

Calls the SDK makes to Mimic, Tessera and Spectre carry `traceparent` (see headers()), so their spans join the
request's trace. Inside a download, ATTEMPT holds the attempt for code that has no request at hand (the local
router, the solve client); it is set inside the middleware's own task, never in a callback shared by requests.
"""
import secrets
import time
from contextvars import ContextVar
from typing import Dict, Optional, Tuple

TRACE_KEY = "calyprium_trace"
ATTEMPT_KEY = "calyprium_attempt"

# (trace id, attempt span id, request span id) of the download attempt being made in this task
ATTEMPT: ContextVar[Optional[Tuple[str, str, str]]] = ContextVar("calyprium_attempt", default=None)


def new_trace_id() -> str:
    return secrets.token_hex(16)


def new_span_id() -> str:
    return secrets.token_hex(8)


def request_trace(request, parent_trace_id: Optional[str] = None) -> Dict:
    """The request's trace, made the first time it is asked for (when the request is yielded or first
    downloaded); a retried or replaced request keeps it because Scrapy copies meta."""
    meta = request.meta
    trace = meta.get(TRACE_KEY)
    if not isinstance(trace, dict) or not trace.get("trace_id"):
        trace = {"trace_id": new_trace_id(), "span_id": new_span_id(), "start": time.time()}
        if parent_trace_id:
            trace["parent_trace_id"] = parent_trace_id
        meta[TRACE_KEY] = trace
    return trace


def start_attempt(request) -> Dict:
    """A new download attempt of the request (its own span under the request's)."""
    request_trace(request)
    attempt = {"span_id": new_span_id(), "start": time.time()}
    request.meta[ATTEMPT_KEY] = attempt
    return attempt


def attempt_ids(request) -> Optional[Tuple[str, str, str]]:
    """(trace id, attempt span id, request span id) of the request's current attempt, or None."""
    meta = getattr(request, "meta", None) or {}
    trace, attempt = meta.get(TRACE_KEY), meta.get(ATTEMPT_KEY)
    if isinstance(trace, dict) and isinstance(attempt, dict) and trace.get("trace_id") and attempt.get("span_id"):
        return trace["trace_id"], attempt["span_id"], trace.get("span_id", "")
    return None


def traceparent(trace_id: str, span_id: str) -> str:
    return f"00-{trace_id}-{span_id}-01"


def headers(request=None) -> Dict[str, str]:
    """`traceparent` for a call made on behalf of `request` (or of the attempt current in this task)."""
    ids = attempt_ids(request)[:2] if request is not None and attempt_ids(request) else None
    if ids is None and request is not None:
        trace = (getattr(request, "meta", None) or {}).get(TRACE_KEY)
        if isinstance(trace, dict) and trace.get("trace_id"):
            ids = (trace["trace_id"], trace["span_id"])
    if ids is None and ATTEMPT.get():
        ids = ATTEMPT.get()[:2]
    return {"traceparent": traceparent(*ids)} if ids else {}
