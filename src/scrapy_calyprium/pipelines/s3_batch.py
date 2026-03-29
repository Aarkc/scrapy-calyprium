"""
S3 Batch Pipeline for Scrapy.

Writes items to S3-compatible storage incrementally in batches of N items.
Each batch is a separate JSONL file, written as soon as the batch is full.
This ensures data is durable mid-crawl -- if the spider dies, completed
batches are already safely stored.

Uses boto3 to upload through the Forge S3 gateway. Credentials are
auto-configured by ``scrapy_calyprium.configure()`` (which sets
``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, and ``AWS_ENDPOINT_URL``).

Settings:
    S3_BATCH_SIZE: Items per batch file (default: 100)
    S3_BATCH_PATH: Path template. Supports {user_id}, {spider},
        {run_number}, {batch_id}.
        Default: "{user_id}/{spider}/runs/{run_number}/batch_{batch_id}.jl"
    S3_BUCKET: Bucket name (default: calyprium)
    AWS_ACCESS_KEY_ID: S3 access key (auto-set by configure())
    AWS_SECRET_ACCESS_KEY: S3 secret key (auto-set by configure())
    AWS_ENDPOINT_URL: S3 endpoint (default: https://forge.calyprium.com/s3)
    AWS_REGION_NAME: S3 region (default: us-east-1)
    SPIDER_USER_ID: User ID for path construction
    SPIDER_NAME: Spider name for path construction
    SPIDER_RUN_NUMBER: Run number for path construction
"""

import json
import logging
import os
from io import BytesIO
from typing import List, Optional

from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


class S3BatchPipeline:
    """
    Scrapy item pipeline that writes items to S3 in batches.

    Every ``batch_size`` items, a JSONL file is uploaded to S3.
    Remaining items are flushed when the spider closes.
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        region_name: str,
        bucket: str,
        batch_size: int,
        path_template: str,
        user_id: str,
        spider_name: str,
        run_number: str,
    ):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name
        self.bucket = bucket
        self.batch_size = batch_size
        self.path_template = path_template
        self.user_id = user_id
        self.spider_name = spider_name
        self.run_number = run_number

        self._buffer: List[dict] = []
        self._batch_id = 0
        self._total_items = 0
        self._client = None

    @classmethod
    def from_crawler(cls, crawler):
        # Resolve credentials: crawler settings > env vars > CALYPRIUM_API_KEY fallback
        api_key = crawler.settings.get(
            "CALYPRIUM_API_KEY", os.getenv("CALYPRIUM_API_KEY", "")
        )

        access_key = (
            crawler.settings.get("AWS_ACCESS_KEY_ID")
            or os.getenv("AWS_ACCESS_KEY_ID")
            or api_key
        )
        secret_key = (
            crawler.settings.get("AWS_SECRET_ACCESS_KEY")
            or os.getenv("AWS_SECRET_ACCESS_KEY")
            or api_key
        )

        if not access_key or not secret_key:
            raise NotConfigured(
                "S3BatchPipeline requires S3 credentials. Set AWS_ACCESS_KEY_ID "
                "and AWS_SECRET_ACCESS_KEY, or use scrapy_calyprium.configure() "
                "with an API key."
            )

        endpoint_url = (
            crawler.settings.get("AWS_ENDPOINT_URL")
            or os.getenv("AWS_ENDPOINT_URL")
            or "https://forge.calyprium.com/s3"
        )

        return cls(
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            region_name=crawler.settings.get(
                "AWS_REGION_NAME", os.getenv("AWS_REGION_NAME", "us-east-1")
            ),
            bucket=crawler.settings.get(
                "S3_BUCKET", os.getenv("S3_BUCKET", "calyprium")
            ),
            batch_size=crawler.settings.getint("S3_BATCH_SIZE", 100),
            path_template=crawler.settings.get(
                "S3_BATCH_PATH",
                "{user_id}/{spider}/runs/{run_number}/batch_{batch_id}.jl",
            ),
            user_id=crawler.settings.get(
                "SPIDER_USER_ID", os.getenv("SPIDER_USER_ID", "default")
            ),
            spider_name=crawler.settings.get(
                "SPIDER_NAME", os.getenv("SPIDER_NAME", "unknown")
            ),
            run_number=crawler.settings.get(
                "SPIDER_RUN_NUMBER", os.getenv("SPIDER_RUN_NUMBER", "0")
            ),
        )

    def _get_client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name,
            )
        return self._client

    def open_spider(self, spider):
        logger.info(
            f"S3BatchPipeline: batch_size={self.batch_size}, "
            f"bucket={self.bucket}, endpoint={self.endpoint_url}"
        )

    def close_spider(self, spider):
        """Flush remaining items."""
        if self._buffer:
            self._flush()
        logger.info(
            f"S3BatchPipeline: wrote {self._total_items} items "
            f"in {self._batch_id} batches"
        )

    def process_item(self, item, spider):
        self._buffer.append(dict(item))
        if len(self._buffer) >= self.batch_size:
            self._flush()
        return item

    def _flush(self):
        """Write buffered items to S3 as a JSONL file."""
        if not self._buffer:
            return

        self._batch_id += 1
        path = self.path_template.format(
            user_id=self.user_id,
            spider=self.spider_name,
            run_number=self.run_number,
            batch_id=self._batch_id,
        )

        # Build JSONL content
        lines = [json.dumps(item, ensure_ascii=False) for item in self._buffer]
        content = ("\n".join(lines) + "\n").encode("utf-8")

        try:
            client = self._get_client()
            client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=BytesIO(content),
                ContentLength=len(content),
                ContentType="application/x-jsonlines",
            )
            self._total_items += len(self._buffer)
            logger.info(
                f"S3BatchPipeline: wrote batch {self._batch_id} "
                f"({len(self._buffer)} items, {len(content):,} bytes) "
                f"-> {self.bucket}/{path}"
            )
        except Exception as e:
            logger.error(
                f"S3BatchPipeline: failed to write batch {self._batch_id}: {e}"
            )

        self._buffer.clear()
