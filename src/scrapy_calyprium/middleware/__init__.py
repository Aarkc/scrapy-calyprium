"""
Scrapy middleware components for anti-detection.

- ``VeilProxyMiddleware`` — routes requests through the Veil proxy gateway
- ``SpectreMiddleware`` — applies device fingerprints from the Spectre service
- ``MimicBrowserMiddleware`` — browser rendering with auto-routing
  (httpcloak TLS fingerprinting + Spectre device fingerprints handled internally)
"""

from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.middleware.spectre import SpectreMiddleware
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware

__all__ = [
    "VeilProxyMiddleware",
    "SpectreMiddleware",
    "MimicBrowserMiddleware",
]
