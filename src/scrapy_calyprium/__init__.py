"""
scrapy-calyprium — Anti-detection Scrapy middleware stack.

Provides proxy routing (Veil) and browser rendering (Mimic) for Scrapy spiders.
Mimic's auto-routing handles TLS fingerprinting (httpcloak) and device
fingerprints (Spectre) internally.

Quick start::

    # settings.py
    import scrapy_calyprium
    scrapy_calyprium.configure(api_key="caly_...")

    # Or manual setup:
    DOWNLOADER_MIDDLEWARES = {
        "scrapy_calyprium.VeilProxyMiddleware": 100,
        "scrapy_calyprium.MimicBrowserMiddleware": 200,
    }
"""

from scrapy_calyprium._version import __version__
from scrapy_calyprium._config import CalypriumConfig, configure, get_config
from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware

__all__ = [
    "__version__",
    "configure",
    "get_config",
    "CalypriumConfig",
    "VeilProxyMiddleware",
    "MimicBrowserMiddleware",
]
