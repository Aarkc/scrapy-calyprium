"""Which of a request's headers the spider chose.

SpectreMiddleware dresses a request in a device fingerprint's headers, and the local fetch path
(MimicBrowserMiddleware) sends a request through a TLS-fingerprinted client that has a browser
identity of its own. Both keep what the spider set on its Request and nothing the crawl added.

Scrapy's RefererMiddleware gives every request a callback yields a Referer (by its referrer policy,
the page the request came from) before any downloader middleware runs. CalypriumTraceMiddleware,
which sees the request right after it, keeps the value Scrapy gave in ``meta[SCRAPY_REFERER]``. A
Referer equal to it is Scrapy's, not the spider's, and keeps the treatment it always had: replaced
by the fingerprint's, and not sent by the local fetch. (A spider that sets exactly the value Scrapy
would have set cannot be told apart; ``meta["referrer_policy"] = "no-referrer"`` makes its own count.)
"""
from typing import Any, Optional

#: request.meta key: the Referer Scrapy's RefererMiddleware gave the request (absent: none).
SCRAPY_REFERER = "calyprium_scrapy_referer"


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("latin-1")
    return str(value)


def scrapy_referer(request) -> bool:
    """True when the request's Referer is the one Scrapy's RefererMiddleware gave it."""
    meta = getattr(request, "meta", None) or {}
    given = meta.get(SCRAPY_REFERER)
    return bool(given) and _text(request.headers.get("Referer")) == given


def referer_middleware(settings: Any):
    """A RefererMiddleware computing what the crawl's own one sets, or None when the crawl runs
    without it (REFERER_ENABLED off, or the middleware switched off)."""
    from scrapy.settings import Settings
    from scrapy.spidermiddlewares.referer import RefererMiddleware

    settings = settings if settings is not None else Settings()
    try:
        if not settings.getbool("REFERER_ENABLED", True):
            return None
        for key, order in dict(settings.getwithbase("SPIDER_MIDDLEWARES")).items():
            name = key if isinstance(key, str) else getattr(key, "__name__", "")
            if name.rsplit(".", 1)[-1] == "RefererMiddleware" and order is None:
                return None
        return RefererMiddleware(settings)
    except Exception:  # noqa: BLE001 -- no Referer is then counted as Scrapy's
        return None


def note_scrapy_referer(request, response, middleware) -> None:
    """Keep the Referer Scrapy's RefererMiddleware gave ``request`` (yielded by the callback of
    ``response``) in its meta; drop any copied from another request."""
    request.meta.pop(SCRAPY_REFERER, None)
    if middleware is None or response is None or not getattr(response, "url", None):
        return
    given = middleware.policy(response, request).referrer(response.url, request.url)
    if given:
        request.meta[SCRAPY_REFERER] = _text(given)
