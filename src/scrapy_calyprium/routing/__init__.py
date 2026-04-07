"""Local-first auto-routing for scrapy-calyprium.

This package provides building blocks for performing TLS-fingerprinted HTTP
fetches inside the spider process instead of round-tripping every request
through the Mimic router service. The goal is to keep Mimic on the cold path
(challenge solving) while the spider handles the hot path (cookie replay).

See AAR-15 for the full design.
"""
from scrapy_calyprium.routing.block_detect import is_blocked
from scrapy_calyprium.routing.local_fetch import (
    LocalFetchError,
    LocalFetchResult,
    LocalFetcher,
    is_local_fetch_available,
)

__all__ = [
    "LocalFetchError",
    "LocalFetchResult",
    "LocalFetcher",
    "is_blocked",
    "is_local_fetch_available",
]
