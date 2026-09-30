"""
S3 Batch Pipeline for Scrapy.

Writes items to S3-compatible storage incrementally in batches of N items.
Each batch is a separate JSONL file, written as soon as the batch is full.
This ensures data is durable mid-crawl -- if the spider dies, completed
batches are already safely stored.

Uses boto3 to upload through the Forge S3 gateway. Credentials are
auto-configured by ``scrapy_calyprium.configure()`` (which sets
``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, and ``AWS_ENDPOINT_URL``).

Reliability (AAR-59):

* Items are serialized when they arrive, with Scrapy's ``ScrapyJSONEncoder``
  over ``ItemAdapter`` (dates, Decimals, sets, nested Items, bytes...). An item
  that still can't be serialized is dropped (``DropItem``) and counted in
  ``s3batch/dropped_items``; it never poisons the rest of the run.
* Uploads run off the reactor thread and are retried with exponential
  backoff. A batch that still fails is spilled to disk and retried after the
  next successful upload and again at close. Whatever is still unpersisted at
  close is counted in ``s3batch/lost_items`` and the process exits non-zero.
* ``on_items_persisted`` listeners run only after a batch is stored, so freshness
  tracking (``RecrawlTrackingPipeline``) never marks a URL crawled whose data
  was lost.

Settings:
    S3_BATCH_SIZE: Items per batch file (default: 100)
    S3_BATCH_PATH: Path template. Supports {user_id}, {spider},
        {run_number}, {batch_id}.
        Default: "{user_id}/{spider}/runs/{run_number}/batch_{batch_id}.jl"
    S3_BUCKET: Bucket name (default: calyprium)
    S3_BATCH_UPLOAD_ATTEMPTS: Upload attempts per batch (default: 5)
    S3_BATCH_RETRY_BACKOFF: First retry delay in seconds, doubling (default: 1.0)
    S3_BATCH_SPILL_DIR: Where failed batches are spilled (default: a temp dir)
    S3_BATCH_FAIL_ON_LOSS: Exit the process non-zero if items were lost
        (default: True)
    AWS_ACCESS_KEY_ID: S3 access key (auto-set by configure())
    AWS_SECRET_ACCESS_KEY: S3 secret key (auto-set by configure())
    AWS_ENDPOINT_URL: S3 endpoint (default: https://forge.calyprium.com/s3)
    AWS_REGION_NAME: S3 region (default: us-east-1)
    SPIDER_USER_ID: User ID for path construction
    SPIDER_NAME: Spider name for path construction
    SPIDER_RUN_NUMBER: Run number for path construction
"""

import atexit
import json
import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Callable, List, Optional, Tuple

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem, NotConfigured
from scrapy.utils.serialize import ScrapyJSONEncoder
from twisted.internet import defer
from twisted.internet.threads import deferToThread

logger = logging.getLogger(__name__)

_LISTENERS_ATTR = "_calyprium_s3batch_persist_listeners"


def on_items_persisted(crawler: Any, callback: Callable[..., Any]) -> None:
    """Register ``callback(entries, spider)`` to run (on the reactor thread)
    after each batch is durably stored. ``entries`` is a list of
    ``(item, fallback_url)``. A Deferred returned by the callback is awaited
    before the pipeline finishes closing.

    (A plain registry rather than a Scrapy signal: deferred-returning signal
    handlers are deprecated in Scrapy 2.14 and absent-as-coroutines in 2.11.)
    """
    listeners = getattr(crawler, _LISTENERS_ATTR, None)
    if listeners is None:
        listeners = []
        setattr(crawler, _LISTENERS_ATTR, listeners)
    listeners.append(callback)

#: Process exit code used when items were lost (see ``_exit_nonzero_at_exit``).
LOST_ITEMS_EXIT_CODE = 3


class _ItemEncoder(ScrapyJSONEncoder):
    """ScrapyJSONEncoder plus bytes (Scrapy's exporters decode them too)."""

    def default(self, o: Any) -> Any:
        if isinstance(o, (bytes, bytearray)):
            return bytes(o).decode("utf-8", "replace")
        return super().default(o)


def serialize_item(item: Any) -> str:
    """One JSONL line for ``item``. Raises on unserializable items."""
    return json.dumps(ItemAdapter(item).asdict(), cls=_ItemEncoder, ensure_ascii=False)


def _exit_nonzero_at_exit(code: int = LOST_ITEMS_EXIT_CODE) -> None:
    """``scrapy crawl`` exits 0 unless bootstrap failed, so a run that lost data
    would look successful. Force a non-zero exit once shutdown finishes."""

    def _exit() -> None:
        logging.shutdown()
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except Exception:
                pass
        os._exit(code)

    atexit.register(_exit)


@dataclass
class _Batch:
    batch_id: int
    key: str
    lines: List[str]
    entries: List[Tuple[Any, Optional[str]]] = field(default_factory=list)

    def body(self) -> bytes:
        return ("\n".join(self.lines) + "\n").encode("utf-8")


class S3BatchPipeline:
    """
    Scrapy item pipeline that writes items to S3 in batches.

    Every ``batch_size`` items, a JSONL file is uploaded to S3.
    Remaining items are flushed when the spider closes.
    """

    #: Runs a blocking callable off the reactor; overridable in tests.
    _run_blocking = staticmethod(deferToThread)

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
        upload_attempts: int = 5,
        retry_backoff: float = 1.0,
        spill_dir: Optional[str] = None,
        fail_on_loss: bool = True,
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
        self.upload_attempts = max(1, upload_attempts)
        self.retry_backoff = retry_backoff
        self.spill_dir = spill_dir or os.path.join(
            tempfile.gettempdir(), "scrapy_calyprium_s3_spill",
            f"{spider_name}_{run_number}_{os.getpid()}",
        )
        self.fail_on_loss = fail_on_loss

        self.crawler = None
        self._buffer: List[str] = []
        self._entries: List[Tuple[Any, Optional[str]]] = []
        self._batch_id = 0
        self._total_items = 0
        self._dropped_items = 0
        self._lost_items = 0
        self._spilled: List[Tuple[str, _Batch]] = []  # (path on disk, batch)
        self._in_flight: List[defer.Deferred] = []
        self._draining = False
        self._client = None
        self._sleep = time.sleep

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

        pipeline = cls(
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
            upload_attempts=crawler.settings.getint("S3_BATCH_UPLOAD_ATTEMPTS", 5),
            retry_backoff=crawler.settings.getfloat("S3_BATCH_RETRY_BACKOFF", 1.0),
            spill_dir=crawler.settings.get("S3_BATCH_SPILL_DIR"),
            fail_on_loss=crawler.settings.getbool("S3_BATCH_FAIL_ON_LOSS", True),
        )
        pipeline.crawler = crawler
        return pipeline

    # -- helpers -----------------------------------------------------------

    def _stats_inc(self, key: str, count: int = 1) -> None:
        stats = getattr(self.crawler, "stats", None)
        if stats is not None and count:
            stats.inc_value(key, count)

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

    def _put(self, key: str, body: bytes) -> None:
        self._get_client().put_object(
            Bucket=self.bucket,
            Key=key,
            Body=BytesIO(body),
            ContentLength=len(body),
            ContentType="application/x-jsonlines",
        )

    def _put_with_retry(self, batch: _Batch, attempts: Optional[int] = None) -> bool:
        """Blocking: upload with exponential backoff. Runs off the reactor."""
        attempts = attempts or self.upload_attempts
        body = batch.body()
        delay = self.retry_backoff
        for attempt in range(1, attempts + 1):
            try:
                self._put(batch.key, body)
                return True
            except Exception as exc:
                logger.warning(
                    "S3BatchPipeline: upload of batch %d failed (attempt %d/%d): %s",
                    batch.batch_id, attempt, attempts, exc,
                )
                if attempt < attempts:
                    self._sleep(min(delay, 30.0))
                    delay *= 2
        return False

    def _spill(self, batch: _Batch) -> bool:
        try:
            os.makedirs(self.spill_dir, exist_ok=True)
            path = os.path.join(self.spill_dir, f"batch_{batch.batch_id}.jl")
            with open(path, "wb") as fh:
                fh.write(batch.body())
            self._spilled.append((path, batch))
            logger.error(
                "S3BatchPipeline: batch %d (%d items) spilled to %s; will retry",
                batch.batch_id, len(batch.lines), path,
            )
            return True
        except OSError as exc:
            logger.error("S3BatchPipeline: could not spill batch %d: %s", batch.batch_id, exc)
            return False

    def _upload_blocking(self, batch: _Batch) -> Tuple[_Batch, bool]:
        return batch, self._put_with_retry(batch)

    @staticmethod
    def _retry_spilled_blocking(
        pipeline: "S3BatchPipeline", spilled: List[Tuple[str, _Batch]], attempts: int,
    ) -> List[Tuple[str, _Batch, bool]]:
        return [(path, batch, pipeline._put_with_retry(batch, attempts))
                for path, batch in spilled]

    # -- reactor-thread bookkeeping ----------------------------------------

    def _persisted(self, batch: _Batch, spider) -> defer.Deferred:
        self._total_items += len(batch.lines)
        self._stats_inc("s3batch/items_written", len(batch.lines))
        self._stats_inc("s3batch/batches_written")
        logger.info(
            "S3BatchPipeline: wrote batch %d (%d items) -> %s/%s",
            batch.batch_id, len(batch.lines), self.bucket, batch.key,
        )
        waits = []
        for callback in getattr(self.crawler, _LISTENERS_ATTR, None) or []:
            try:
                waits.append(defer.maybeDeferred(callback, batch.entries, spider))
            except Exception:  # pragma: no cover - maybeDeferred catches
                logger.exception("S3BatchPipeline: persist listener failed")
        return defer.DeferredList(waits, consumeErrors=True)

    def _after_upload(self, result: Tuple[_Batch, bool], spider):
        batch, ok = result
        if not ok:
            self._stats_inc("s3batch/upload_failures")
            if not self._spill(batch):
                self._record_loss(len(batch.lines))
            return None
        d = self._persisted(batch, spider)
        if self._spilled and not self._draining:
            # S3 is reachable again: retry earlier spills now, not only at close.
            d.addCallback(lambda _: self._drain_spilled(spider, attempts=1))
        return d

    def _drain_spilled(self, spider, attempts: int) -> defer.Deferred:
        if self._draining or not self._spilled:
            return defer.succeed(None)
        self._draining = True
        d = self._run_blocking(
            self._retry_spilled_blocking, self, list(self._spilled), attempts,
        )

        def _done(results):
            persisted = []
            for path, batch, ok in results:
                if not ok:
                    continue
                self._spilled = [(p, b) for p, b in self._spilled if p != path]
                try:
                    os.remove(path)
                except OSError:
                    pass
                persisted.append(self._persisted(batch, spider))
            return defer.DeferredList(persisted)

        def _clear(result):
            self._draining = False
            return result

        d.addCallback(_done)
        d.addBoth(_clear)
        return d

    def _record_loss(self, count: int) -> None:
        self._lost_items += count
        self._stats_inc("s3batch/lost_items", count)

    def _track(self, d: defer.Deferred) -> defer.Deferred:
        self._in_flight.append(d)

        def _untrack(result):
            if d in self._in_flight:
                self._in_flight.remove(d)
            return result

        d.addBoth(_untrack)
        return d

    # -- pipeline API ------------------------------------------------------

    def open_spider(self, spider):
        logger.info(
            f"S3BatchPipeline: batch_size={self.batch_size}, "
            f"bucket={self.bucket}, endpoint={self.endpoint_url}"
        )

    def process_item(self, item, spider):
        try:
            line = serialize_item(item)
        except Exception as exc:
            self._dropped_items += 1
            self._stats_inc("s3batch/dropped_items")
            raise DropItem(f"S3BatchPipeline: item is not JSON-serializable: {exc!r}")
        self._buffer.append(line)
        self._entries.append((item, getattr(spider, "_current_url", None)))
        if len(self._buffer) >= self.batch_size:
            d = self._flush(spider)
            if d is not None:
                # Backpressure: this item completes once its batch is handled.
                d.addCallback(lambda _: item)
                return d
        return item

    def _flush(self, spider=None) -> Optional[defer.Deferred]:
        """Hand the buffered items to an off-reactor upload."""
        if not self._buffer:
            return None
        self._batch_id += 1
        batch = _Batch(
            batch_id=self._batch_id,
            key=self.path_template.format(
                user_id=self.user_id,
                spider=self.spider_name,
                run_number=self.run_number,
                batch_id=self._batch_id,
            ),
            lines=self._buffer,
            entries=self._entries,
        )
        self._buffer, self._entries = [], []
        d = self._run_blocking(self._upload_blocking, batch)
        d.addCallback(self._after_upload, spider)
        d.addErrback(self._upload_crashed, batch)
        return self._track(d)

    def _upload_crashed(self, failure, batch: _Batch):
        logger.error("S3BatchPipeline: batch %d upload crashed: %s", batch.batch_id, failure.value)
        if not self._spill(batch):
            self._record_loss(len(batch.lines))

    @defer.inlineCallbacks
    def close_spider(self, spider):
        """Flush remaining items, wait for uploads, retry spills, account loss."""
        self._flush(spider)
        while self._in_flight:
            yield defer.DeferredList(list(self._in_flight))
        if self._spilled:
            yield self._drain_spilled(spider, attempts=self.upload_attempts)
        for path, batch in self._spilled:
            logger.error(
                "S3BatchPipeline: batch %d (%d items) could not be stored; left at %s",
                batch.batch_id, len(batch.lines), path,
            )
            self._record_loss(len(batch.lines))
        self._spilled = []

        logger.info(
            "S3BatchPipeline: wrote %d items in %d batches (dropped=%d, lost=%d)",
            self._total_items, self._batch_id, self._dropped_items, self._lost_items,
        )
        if self._lost_items and self.fail_on_loss:
            logger.error(
                "S3BatchPipeline: %d items were lost; the run will exit with code %d",
                self._lost_items, LOST_ITEMS_EXIT_CODE,
            )
            _exit_nonzero_at_exit()
