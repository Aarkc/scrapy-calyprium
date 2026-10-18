"""What happened to a download attempt, as the fetch layer's shared classifier names it.

Every attempt ends in one diagnosis class (routing/diagnosis.py, vendored from Mimic) and the
class, not a blocked yes/no, picks the next move. The Mimic middleware keeps the attempt's
diagnosis in the request's meta, where the retry, the request tracer and the run stats read it:

    meta["calyprium_diagnosis"]
        the attempt's diagnosis: Diagnosis.to_dict() ({"class", "number", "side", "move"} plus
        "vendor", "status", "evidence", "confidence", "retry_after" when known); Mimic's own
        when it sends one
    meta["calyprium_fetch_error"]
        Mimic's error text (its x-mimic-error header, or the error of a Mimic call that
        failed); never page content or cookies
    meta["calyprium_fallback_diagnosis"]
        why the attempt left the Mimic / local path for a plain download
    meta["calyprium_attempt_number"]   Calyprium retries made before this download (absent: 0)
    meta["calyprium_prior_class"]      the class that caused the last Calyprium retry
    meta["calyprium_routing_method"]   how the attempt was served (counted by CalypriumRunStats)

A diagnosis carries marker names, never content, cookies or credentials (see diagnosis.py).
"""
from contextvars import ContextVar
from typing import Any, Dict, Optional

from scrapy_calyprium.routing import diagnosis as dg
from scrapy_calyprium.routing.block_detect import BLOCKED_CLASSES, diagnose

DIAGNOSIS = "calyprium_diagnosis"
FETCH_ERROR = "calyprium_fetch_error"
FALLBACK_DIAGNOSIS = "calyprium_fallback_diagnosis"
ATTEMPT_NUMBER = "calyprium_attempt_number"
PRIOR_CLASS = "calyprium_prior_class"
ROUTING_METHOD = "calyprium_routing_method"

# Moves worth another attempt; FIX_OURS gets one at most, STOP and NONE none.
RETRY_MOVES = frozenset((dg.RETRY_ELSEWHERE, dg.RETRY_LATER, dg.SLOW_DOWN, dg.CLIMB))

# Calyprium retries made before the download in progress in this task, for code with no request
# at hand (the local router's span). Set by the Mimic middleware inside its own coroutine, like
# _trace.ATTEMPT, never in a callback shared by requests.
CURRENT_ATTEMPT: ContextVar[int] = ContextVar("calyprium_attempt_number", default=0)

__all__ = [
    "ATTEMPT_NUMBER", "BLOCKED_CLASSES", "CURRENT_ATTEMPT", "DIAGNOSIS", "FALLBACK_DIAGNOSIS",
    "FETCH_ERROR", "PRIOR_CLASS", "RETRY_MOVES", "ROUTING_METHOD", "attempt_number", "blocked",
    "for_response", "from_mimic", "of_error", "of_response", "of_scrapy_response", "outcome",
]


def attempt_number(request: Any) -> int:
    """Calyprium retries made before this download of ``request`` (0 for the first)."""
    meta = _meta(request)
    try:
        return max(0, int((meta or {}).get(ATTEMPT_NUMBER) or 0))
    except (TypeError, ValueError):
        return 0


def from_mimic(routing: Any) -> Optional[Dict[str, Any]]:
    """Mimic's ``routing.diagnosis`` when it sent a usable one (an older Mimic sends none)."""
    d = routing.get("diagnosis") if isinstance(routing, dict) else None
    if not isinstance(d, dict) or not isinstance(d.get("class"), str) or not d["class"]:
        return None
    out = dict(d)
    info = dg.CLASSES.get(out["class"])
    if info is not None:
        out.setdefault("number", info.number)
        out.setdefault("side", info.side)
        out.setdefault("move", info.move)
    return out


def of_response(status: int, body: Any, content_type: Optional[str] = None,
                headers: Optional[Any] = None, url: Optional[str] = None,
                final_url: Optional[str] = None) -> Dict[str, Any]:
    return diagnose(status, body, content_type, headers, url, final_url).to_dict()


def of_error(error: Any) -> Dict[str, Any]:
    return dg.diagnose_error(error).to_dict()


def of_scrapy_response(response: Any, request: Any = None) -> Dict[str, Any]:
    """The diagnosis of a Scrapy response (a plain download, or one built from Mimic's reply)."""
    headers = getattr(response, "headers", None) or {}
    try:
        ctype = headers.get("Content-Type") or b""
    except Exception:  # noqa: BLE001 -- odd header containers
        ctype = b""
    if isinstance(ctype, bytes):
        ctype = ctype.decode("latin-1", "ignore")
    meta = _meta(request)
    first = (meta.get("redirect_urls") or [None])[0] if meta else None
    url = first or getattr(request, "url", None) or getattr(response, "url", None)
    status = int(getattr(response, "status", 0) or 0)
    body = getattr(response, "body", b"") or b""
    return of_response(status, body, ctype or None, headers, url, getattr(response, "url", None))


def for_response(request: Any, response: Any) -> Dict[str, Any]:
    """The attempt's diagnosis: the one kept in meta (the Mimic middleware's), else computed
    from the response and kept there for the next reader."""
    meta = _meta(request)
    diag = meta.get(DIAGNOSIS) if meta is not None else None
    if isinstance(diag, dict) and diag.get("class"):
        return diag
    diag = of_scrapy_response(response, request)
    if meta is not None:
        meta[DIAGNOSIS] = diag
    return diag


def blocked(diag: Optional[Dict[str, Any]]) -> bool:
    return bool(diag) and diag.get("class") in BLOCKED_CLASSES


def outcome(diag: Optional[Dict[str, Any]], status_code: int = 0) -> str:
    """A trace span's status for an attempt: ``blocked`` iff the class is a block, ``success``
    for a fetched page, ``timeout`` / ``error`` otherwise."""
    if isinstance(diag, dict) and diag.get("class"):
        cls = diag["class"]
        if cls in BLOCKED_CLASSES:
            return "blocked"
        if cls == dg.OK:
            return "success"
        if not status_code and diag.get("evidence") in ("timeout", "deadline"):
            return "timeout"
        return "error"
    if status_code in (401, 403, 429, 503):
        return "blocked"
    if 200 <= status_code < 400:
        return "success"
    return "error"


def _meta(request: Any) -> Optional[dict]:
    try:
        meta = request.meta
    except AttributeError:
        return None
    return meta if isinstance(meta, dict) else None
