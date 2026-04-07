"""
scrapy-calyprium — Anti-detection Scrapy middleware stack.

Provides proxy routing (Veil), device fingerprints (Spectre), and browser
rendering (Mimic) for Scrapy spiders. Includes an S3 batch pipeline for
durable item storage.

Quick start::

    # settings.py
    import scrapy_calyprium
    scrapy_calyprium.configure(api_key="caly_...")

    # Or manual setup:
    DOWNLOADER_MIDDLEWARES = {
        "scrapy_calyprium.VeilProxyMiddleware": 100,
        "scrapy_calyprium.SpectreMiddleware": 150,
        "scrapy_calyprium.MimicBrowserMiddleware": 200,
    }
    ITEM_PIPELINES = {
        "scrapy_calyprium.S3BatchPipeline": 100,
    }
"""

from scrapy_calyprium._version import __version__
from scrapy_calyprium._config import CalypriumConfig, configure, get_config
from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.middleware.spectre import SpectreMiddleware
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.pipelines.s3_batch import S3BatchPipeline

# AAR-15/17: optional local-first routing surfaces. Importable only if
# scrapy-calyprium[local] is installed.
try:
    from scrapy_calyprium.routing import (
        LocalFetcher,
        LocalFetchError,
        LocalFetchResult,
        is_local_fetch_available,
        is_blocked,
    )
    _HAS_LOCAL_ROUTING = True
except ImportError:
    _HAS_LOCAL_ROUTING = False

__all__ = [
    "__version__",
    "configure",
    "get_config",
    "CalypriumConfig",
    "VeilProxyMiddleware",
    "SpectreMiddleware",
    "MimicBrowserMiddleware",
    "S3BatchPipeline",
]
if _HAS_LOCAL_ROUTING:
    __all__ += [
        "LocalFetcher",
        "LocalFetchError",
        "LocalFetchResult",
        "is_local_fetch_available",
        "is_blocked",
    ]
