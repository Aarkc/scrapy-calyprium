"""The proxy gateway's "pool busy" answer: no free connection for the request, not the site's answer.

A capacity-capped proxy vendor is shared by the platform's spiders (the spider portfolio). When the gateway
cannot give a request a connection in time (it queues a CONNECT for up to ~15 s) it refuses it with

    HTTP/1.1 503 Service Unavailable (pool_busy)
    X-Calyprium-Proxy-Error: pool_busy

Clients never expose a CONNECT reply's headers, so an HTTPS request learns it only from the status phrase in
the error text:

    Scrapy 2.12 TunnelError (core/downloader/handlers/http11.py, processProxyResponse):
        Could not open CONNECT tunnel with proxy <host>:<port> [{'status': 503, 'reason': b'Service Unavailable (pool_busy)'}]
    httpcloak 1.7.2 (LocalFetchError, routing/local_fetch.py), for http:// URLs too (it tunnels every request):
        httpcloak fetch failed for <url>: dial_proxy <host>: proxy CONNECT failed: 503 Service Unavailable (pool_busy)
    curl_cffi 0.16 says only "CONNECT tunnel failed, response 503"; the fetcher adds the reply's
        X-Calyprium-Proxy-Error, which the error carries (routing/local_fetch.py, _proxy_refusal)

and a plain-HTTP request through the proxy (Scrapy, curl_cffi) gets a 503 Response carrying the header. The
local route raises ``PoolBusyError`` for it (MimicBrowserMiddleware), so PoolBusyMiddleware
(middleware/portfolio.py) requeues every one of them the same way. A pool-busy refusal is never a site failure
or a block: nothing escalates, no slot, exit or domain is blamed, and the run's failure counts leave it out.
"""
import re
from typing import Any, Dict, Optional

from scrapy_calyprium.routing import diagnosis as dg

POOL_BUSY = "pool_busy"
PROXY_ERROR_HEADER = "X-Calyprium-Proxy-Error"
# meta: how many times the request was refused for a busy pool (requeues made; never reset)
BUSY_COUNT = "calyprium_pool_busy"
# meta: set on a requeue until CalypriumRunStats sees it scheduled (a requeue is not a new request)
REQUEUED = "_calyprium_pool_requeue"

_URL_RE = re.compile(r"\b[a-z][a-z0-9+.-]*://\S+", re.I)
_BUSY_RE = re.compile(r"\bpool_busy\b", re.I)


class PoolBusyError(ConnectionError):
    """The proxy gateway had no free connection for the request (``pool_busy``). A ConnectionError, so
    without PoolBusyMiddleware it gets the handling of any transport error."""

    def __init__(self, detail: str = ""):
        detail = str(detail or "").strip()
        super().__init__(f"proxy pool busy ({PROXY_ERROR_HEADER}: {POOL_BUSY})" + (f": {detail}" if detail else ""))


def is_pool_busy_text(text: Any) -> bool:
    """Whether an error text names the gateway's pool_busy refusal (a URL in the text is no evidence)."""
    if not text:
        return False
    return bool(_BUSY_RE.search(_URL_RE.sub(" ", str(text))))


def is_pool_busy_error(error: Optional[BaseException]) -> bool:
    """A download exception that is the gateway's pool_busy refusal: PoolBusyError, a backend error that kept
    the gateway's reason (``proxy_error``), or one whose text names it (TunnelError, LocalFetchError), or
    whose explicit cause does."""
    for _ in range(4):
        if error is None:
            return False
        if isinstance(error, PoolBusyError) or getattr(error, "proxy_error", None) == POOL_BUSY:
            return True
        try:
            if is_pool_busy_text(str(error)):
                return True
        except Exception:  # noqa: BLE001 -- an exception whose str() fails says nothing
            pass
        error = error.__cause__
    return False


def header_value(headers: Any, name: str = PROXY_ERROR_HEADER) -> str:
    """A header's value from Scrapy Headers (bytes) or a plain mapping (any case); "" when absent."""
    if not headers:
        return ""
    getlist = getattr(headers, "getlist", None)
    if callable(getlist):
        try:
            values = getlist(name)
        except Exception:  # noqa: BLE001 -- odd header containers: read the items below
            values = None
        if values:
            return ", ".join(_text(v) for v in values)
    items = headers.items() if hasattr(headers, "items") else ()
    for key, value in items:
        if _text(key).lower() == name.lower():
            if isinstance(value, (list, tuple)):
                return ", ".join(_text(v) for v in value)
            return _text(value)
    return ""


def is_pool_busy_headers(headers: Any) -> bool:
    """Whether a response's headers carry the gateway's ``X-Calyprium-Proxy-Error: pool_busy``."""
    value = header_value(headers)
    return any(part.strip().lower() == POOL_BUSY for part in value.split(","))


def is_pool_busy_response(response: Any) -> bool:
    return is_pool_busy_headers(getattr(response, "headers", None))


def diagnosis(status: int = 503) -> Dict[str, Any]:
    """The attempt's diagnosis: the vendored classifier's verdict on the gateway's header (a proxy failure,
    evidence pool_busy)."""
    return dg.Diagnosis(dg.PROXY_FAILURE, "", status, POOL_BUSY).to_dict()


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("latin-1")
    return str(value)
