"""
Stealth Fetch Middleware for Scrapy.

Replaces Scrapy's default HTTP client with a TLS-fingerprinted backend
(curl_cffi or httpcloak) for every request. No browser sessions, no cookie
solves, no domain cache — just a drop-in HTTP client with a proper browser
TLS fingerprint.

Settings:
    STEALTH_FETCH_PRESET: TLS preset (default: firefox-135).
        Firefox presets use curl_cffi, Chrome presets use httpcloak.
    STEALTH_FETCH_BACKEND: Force a backend ("curl_cffi" or "httpcloak").
        If omitted, auto-selects based on preset.
    STEALTH_FETCH_TIMEOUT: Request timeout in seconds (default: 30).
"""

import logging
from typing import Optional

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse, Response, TextResponse

logger = logging.getLogger(__name__)


class StealthFetchMiddleware:
    """Scrapy downloader middleware that fetches via curl_cffi or httpcloak."""

    def __init__(self, preset: str, timeout: int, backend: Optional[str]):
        self.preset = preset
        self.timeout = timeout
        self.forced_backend = backend
        self._fetcher = None

    @classmethod
    def from_crawler(cls, crawler):
        preset = crawler.settings.get("STEALTH_FETCH_PRESET", "firefox-135")
        timeout = crawler.settings.getint("STEALTH_FETCH_TIMEOUT", 30)
        backend = crawler.settings.get("STEALTH_FETCH_BACKEND")

        try:
            from scrapy_calyprium.routing.local_fetch import (
                is_local_fetch_available,
            )
            if not is_local_fetch_available():
                raise NotConfigured(
                    "StealthFetchMiddleware requires httpcloak or curl_cffi. "
                    "Install scrapy-calyprium[local]."
                )
        except ImportError:
            raise NotConfigured("scrapy-calyprium[local] not installed")

        middleware = cls(preset=preset, timeout=timeout, backend=backend)
        crawler.signals.connect(
            middleware.spider_opened, signal=signals.spider_opened
        )
        return middleware

    def spider_opened(self, spider):
        from scrapy_calyprium.routing.local_fetch import LocalFetcher

        self._fetcher = LocalFetcher(
            default_preset=self.preset,
            timeout=self.timeout,
            backend=self.forced_backend,
        )
        logger.info(
            "StealthFetchMiddleware: preset=%s, backend=%s, timeout=%ds",
            self.preset, self._fetcher.backend, self.timeout,
        )

    async def process_request(self, request, spider):
        if request.meta.get("_internal"):
            return None

        # Use the proxy from Scrapy's meta (set by VeilProxyMiddleware)
        proxy_url = request.meta.get("proxy")

        try:
            result = await self._fetcher.fetch(
                url=request.url,
                proxy_url=proxy_url,
                preset=self.preset,
                timeout=self.timeout,
            )
        except Exception as exc:
            logger.debug("StealthFetch failed for %s: %s", request.url, exc)
            return None  # let Scrapy's default handler try

        body = result.body
        status = result.status_code
        headers = result.headers
        url = result.final_url or request.url

        ct = (
            headers.get("content-type")
            or headers.get("Content-Type")
            or ""
        ).lower()
        if "text/html" in ct or "application/xhtml" in ct:
            encoding = "utf-8"
            if "charset=" in ct:
                encoding = (
                    ct.split("charset=", 1)[1].split(";")[0].strip()
                    or "utf-8"
                )
            return HtmlResponse(
                url=url, status=status, headers=headers, body=body,
                encoding=encoding, request=request,
            )
        if "text/" in ct or "json" in ct or "xml" in ct:
            return TextResponse(
                url=url, status=status, headers=headers, body=body,
                encoding="utf-8", request=request,
            )
        return Response(
            url=url, status=status, headers=headers, body=body,
            request=request,
        )
