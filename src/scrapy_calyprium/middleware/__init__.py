"""
Scrapy middleware components for anti-detection.

- ``VeilProxyMiddleware`` — routes requests through the Veil proxy gateway
- ``MimicBrowserMiddleware`` — browser rendering with auto-routing
  (httpcloak TLS fingerprinting + Spectre device fingerprints handled internally)
"""

from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware

__all__ = [
    "VeilProxyMiddleware",
    "MimicBrowserMiddleware",
]
