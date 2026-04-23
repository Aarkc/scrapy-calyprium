"""
Scrapy middleware components for anti-detection.

- ``VeilProxyMiddleware`` — routes requests through the Veil proxy gateway
- ``SpectreMiddleware`` — applies device fingerprints from the Spectre service
- ``MimicBrowserMiddleware`` — browser rendering with auto-routing
  (httpcloak TLS fingerprinting + Spectre device fingerprints handled internally)
- ``StealthFetchMiddleware`` — drop-in TLS-fingerprinted HTTP client
  (curl_cffi / httpcloak, no browser sessions or cookie solves)
"""

from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.middleware.spectre import SpectreMiddleware
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.stealth_fetch import StealthFetchMiddleware

__all__ = [
    "VeilProxyMiddleware",
    "SpectreMiddleware",
    "MimicBrowserMiddleware",
    "StealthFetchMiddleware",
]
