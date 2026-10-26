"""
Scrapy middleware components for anti-detection.

- ``VeilProxyMiddleware`` — routes requests through the Veil proxy gateway
- ``SpectreMiddleware`` — applies device fingerprints from the Spectre service
- ``MimicBrowserMiddleware`` — browser rendering with auto-routing
- ``CalypriumTraceMiddleware`` — one trace per request (downloader and spider middleware)
  (httpcloak TLS fingerprinting + Spectre device fingerprints handled internally)
- ``StealthFetchMiddleware`` — drop-in TLS-fingerprinted HTTP client
  (curl_cffi / httpcloak, no browser sessions or cookie solves)
- ``PaceMiddleware`` — spider-wide pages-per-minute pace (CALYPRIUM_PACE_PPM)
- ``PoolBusyMiddleware`` — requeues what the proxy gateway refused for a busy
  pool (``pool_busy``) and backs the concurrency off
"""

from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.middleware.spectre import SpectreMiddleware
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.tracing import CalypriumTraceMiddleware
from scrapy_calyprium.middleware.stealth_fetch import StealthFetchMiddleware
from scrapy_calyprium.middleware.portfolio import PaceMiddleware, PoolBusyMiddleware

__all__ = [
    "VeilProxyMiddleware",
    "SpectreMiddleware",
    "MimicBrowserMiddleware",
    "CalypriumTraceMiddleware",
    "StealthFetchMiddleware",
    "PaceMiddleware",
    "PoolBusyMiddleware",
]
