"""
Forge Feed Storage for Scrapy.

S3-compatible feed storage backend that writes spider output to the
Calyprium Forge platform's object storage. Works with any S3-compatible
backend (MinIO, AWS S3, Hetzner Object Storage, etc.).

URI format: ``forge://bucket/path/to/file.jl``

Settings are read from environment variables:
    S3_ENDPOINT: Storage endpoint (e.g., "minio:9000")
    S3_ACCESS_KEY: Access key
    S3_SECRET_KEY: Secret key
    S3_SECURE: Use HTTPS (default: false)
    S3_BUCKET_USER: Default bucket name (default: calyprium)
    S3_SKIP_BUCKET_CREATION: Skip auto-creating buckets (default: false)
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

        # S3 credentials (S3_* takes precedence over MINIO_* for compat)
        self.endpoint = os.getenv(
            "S3_ENDPOINT", os.getenv("MINIO_ENDPOINT", "localhost:9000")
        )
        self.access_key = os.getenv(
            "S3_ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        )
        self.secret_key = os.getenv(
            "S3_SECRET_KEY", os.getenv("MINIO_SECRET_KEY", "minioadmin")
        )
        self.secure = (
            os.getenv("S3_SECURE", os.getenv("MINIO_SECURE", "false")).lower()
            == "true"
        )

        from minio import Minio

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

        if os.getenv("S3_SKIP_BUCKET_CREATION", "false").lower() != "true":
            try:
                if not self.client.bucket_exists(self.bucket):
                    self.client.make_bucket(self.bucket)
                    logger.info(f"Created bucket: {self.bucket}")
            except Exception as e:
                logger.warning(f"Error ensuring bucket {self.bucket}: {e}")

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
