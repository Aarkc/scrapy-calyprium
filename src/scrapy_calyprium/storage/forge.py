"""
Forge Feed Storage for Scrapy.

S3-compatible feed storage backend that writes spider output to
Calyprium's object storage.

URI format: ``forge://bucket/path/to/file.jl``

Settings are read from environment variables:
    S3_ENDPOINT: Storage endpoint
    S3_ACCESS_KEY: Access key
    S3_SECRET_KEY: Secret key
    S3_SECURE: Use HTTPS (default: true)
    S3_BUCKET_USER: Default bucket name (default: calyprium)
"""

import logging
import os
from datetime import datetime
from io import BytesIO
from urllib.parse import urlparse

from scrapy.extensions.feedexport import BlockingFeedStorage

logger = logging.getLogger(__name__)


class ForgeFeedStorage(BlockingFeedStorage):
    """
    S3-compatible storage backend for Scrapy feeds.

    Uploads spider output (JSONL, CSV, etc.) to object storage.
    Supports ``%(time)s``, ``%(spider)s``, and ``%(batch_id)d`` placeholders
    in the URI path.

    URI format: ``forge://bucket/path/to/%(time)s_%(batch_id)d.jl``
    """

    def __init__(self, uri, *, feed_options=None):
        parsed = urlparse(uri)
        default_bucket = os.getenv("S3_BUCKET_USER", "calyprium")
        self.bucket = parsed.hostname or default_bucket
        self.path = parsed.path.lstrip("/")
        self.feed_options = feed_options

        self.endpoint = os.getenv("S3_ENDPOINT")
        self.access_key = os.getenv("S3_ACCESS_KEY")
        self.secret_key = os.getenv("S3_SECRET_KEY")
        self.secure = os.getenv("S3_SECURE", "true").lower() == "true"

        if not self.endpoint or not self.access_key or not self.secret_key:
            raise ValueError(
                "ForgeFeedStorage requires S3_ENDPOINT, S3_ACCESS_KEY, "
                "and S3_SECRET_KEY environment variables."
            )

        from minio import Minio

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def _store_in_thread(self, file):
        """Store the feed file in object storage."""
        file.seek(0)
        data = file.read()

        user_id = os.getenv("SPIDER_USER_ID", "default")
        spider_name = os.getenv("SPIDER_NAME", "unknown")

        path = self.path
        path = path.replace("%(time)s", datetime.now().strftime("%Y%m%d_%H%M%S"))
        path = path.replace("%(spider)s", spider_name)
        path = path.replace("%(name)s", spider_name)

        # The FEED_URI already contains the full structured path
        # (e.g., {user_id}/{spider}/runs/{N}/batch_{id}.jl).
        # Only reconstruct if the path doesn't already start with user_id.
        if user_id != "default" and spider_name != "unknown":
            if not path.startswith(f"{user_id}/"):
                filename = os.path.basename(path)
                path = f"{user_id}/{spider_name}/{filename}"

        from minio.error import S3Error

        try:
            self.client.put_object(
                self.bucket, path, BytesIO(data), len(data)
            )
            logger.info(f"Stored feed: {self.bucket}/{path} ({len(data):,} bytes)")
        except S3Error as e:
            logger.error(f"Failed to store feed: {e}")
            raise
