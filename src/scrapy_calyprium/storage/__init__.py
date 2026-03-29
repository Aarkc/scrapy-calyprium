"""
Feed storage backends.

- ``ForgeFeedStorage`` — S3-compatible storage for spider output
"""

from scrapy_calyprium.storage.forge import ForgeFeedStorage

__all__ = ["ForgeFeedStorage"]
