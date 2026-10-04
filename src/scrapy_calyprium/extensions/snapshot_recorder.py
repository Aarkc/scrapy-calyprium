"""SnapshotRecorder — save a few raw pages per run for run health.

Keeps a small, bounded sample of what the spider actually saw:

* ``sample`` — the first ``SNAPSHOTS_SAMPLE`` successful HTML responses
  (content-type html, status < 400);
* ``error`` / ``blocked`` — up to ``SNAPSHOTS_ERRORS`` responses with status
  >= 400 or flagged by the SDK's block detection (``routing.block_detect``).
  ``blocked`` when ``is_blocked()`` says so, ``error`` otherwise.

Each page is gzipped and uploaded through the same forge ``/s3`` gateway, with
the same run credentials, as ``S3BatchPipeline``, next to the run's batches::

    {user}/{storage_id}/runs/{n}/snapshots/{sha256}.html.gz

(the run prefix is derived from ``S3_BATCH_PATH``). At spider close the list is
reported to forge::

    POST {FORGE_API_URL}/jobs/spiders/{slug}/runs/{n}/snapshots
    {"snapshots": [{"url", "status_code", "content_hash", "storage_key",
                    "kind", "captured_at"}]}

authenticated like ``CalypriumRunStats`` (``_forge.ForgeAuth``).

Best effort throughout: capture is a few comparisons on the reactor thread;
gzip, hashing and uploads run on a 2-thread pool with short timeouts; every
failure is logged and swallowed. Memory is bounded by
``(SNAPSHOTS_SAMPLE + SNAPSHOTS_ERRORS) * SNAPSHOTS_MAX_BYTES``.

Enabled via settings (forge's platform settings template)::

    EXTENSIONS = {
        "scrapy_calyprium.extensions.snapshot_recorder.SnapshotRecorder": 520,
    }

It disables itself (``NotConfigured``) outside platform runs — without a run
number or S3 credentials — and when ``SNAPSHOTS_ENABLED`` is false.

Settings:
    SNAPSHOTS_ENABLED        (default True)
    SNAPSHOTS_SAMPLE         (default 5)
    SNAPSHOTS_ERRORS         (default 5)
    SNAPSHOTS_MAX_BYTES      (default 2 MB; larger bodies are skipped)
    SNAPSHOTS_CLOSE_TIMEOUT  (seconds to wait for pending uploads at close,
                              default 30)
"""
from __future__ import annotations

import gzip
import hashlib
import logging
import os
import threading
from concurrent import futures
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional

import httpx
from scrapy import signals
from scrapy.exceptions import NotConfigured

from scrapy_calyprium._forge import ForgeAuth
from scrapy_calyprium.pipelines.s3_batch import S3Config, make_s3_client, resolve_s3_config
from scrapy_calyprium.routing.block_detect import is_blocked

logger = logging.getLogger(__name__)

DEFAULT_SAMPLE = 5
DEFAULT_ERRORS = 5
DEFAULT_MAX_BYTES = 2 * 1024 * 1024
DEFAULT_CLOSE_TIMEOUT = 30.0
REPORT_TIMEOUT = 10.0


@dataclass
class _Pending:
    url: str
    status_code: int
    kind: str
    captured_at: str
    body: bytes


def _header(headers: Any, name: str) -> str:
    try:
        value = headers.get(name)
    except Exception:
        return ""
    if isinstance(value, bytes):
        return value.decode("latin-1", "replace")
    return value or ""


def _headers_dict(headers: Any) -> Optional[Dict[str, str]]:
    """Scrapy ``Headers`` -> ``{str: str}`` (joined) for ``is_blocked``."""
    try:
        out = {}
        for key, values in headers.items():
            k = key.decode("latin-1") if isinstance(key, bytes) else str(key)
            vals = values if isinstance(values, (list, tuple)) else [values]
            out[k] = "; ".join(
                v.decode("latin-1", "replace") if isinstance(v, bytes) else str(v)
                for v in vals
            )
        return out
    except Exception:
        return None


def snapshot_key(run_prefix: str, content_hash: str) -> str:
    name = f"snapshots/{content_hash}.html.gz"
    return f"{run_prefix}/{name}" if run_prefix else name


class SnapshotRecorder:
    def __init__(
        self,
        s3: S3Config,
        forge_url: str,
        auth: ForgeAuth,
        spider_slug: str,
        run_number: int,
        sample_limit: int = DEFAULT_SAMPLE,
        error_limit: int = DEFAULT_ERRORS,
        max_bytes: int = DEFAULT_MAX_BYTES,
        close_timeout: float = DEFAULT_CLOSE_TIMEOUT,
        crawler: Any = None,
    ):
        self.s3 = s3
        self.forge_url = forge_url.rstrip("/")
        self.auth = auth
        self.spider_slug = spider_slug
        self.run_number = run_number
        self.sample_limit = max(0, sample_limit)
        self.error_limit = max(0, error_limit)
        self.max_bytes = max_bytes
        self.close_timeout = close_timeout
        self.crawler = crawler
        self.run_prefix = s3.run_prefix()

        self._lock = threading.Lock()
        self._samples = 0
        self._errors = 0
        self._snapshots: List[Dict[str, Any]] = []
        self._uploaded: set = set()
        self._futures: List[futures.Future] = []
        self._executor: Optional[futures.ThreadPoolExecutor] = None
        self._client = None
        self._closed = False

    @classmethod
    def from_crawler(cls, crawler: Any) -> "SnapshotRecorder":
        settings = crawler.settings
        if not settings.getbool("SNAPSHOTS_ENABLED", True):
            raise NotConfigured("SnapshotRecorder: SNAPSHOTS_ENABLED is false")

        run_number_raw = (
            settings.get("SPIDER_RUN_NUMBER")
            or settings.get("CALYPRIUM_RUN_NUMBER")
            or os.getenv("CALYPRIUM_RUN_NUMBER")
        )
        try:
            run_number = int(run_number_raw) if run_number_raw else 0
        except (TypeError, ValueError):
            run_number = 0
        if run_number <= 0:
            raise NotConfigured("SnapshotRecorder: no SPIDER_RUN_NUMBER (not a platform run)")

        s3 = resolve_s3_config(settings)
        if not s3.access_key or not s3.secret_key:
            raise NotConfigured("SnapshotRecorder: no S3 credentials")

        spider = getattr(crawler, "spider", None)
        slug = (
            settings.get("RECRAWL_SPIDER_SLUG")
            or settings.get("SPIDER_SLUG")
            or (getattr(spider, "name", "") if spider is not None else "")
            or ""
        )
        ext = cls(
            s3=s3,
            forge_url=settings.get("FORGE_API_URL", "http://calyprium-backend:8000"),
            auth=ForgeAuth.from_settings(settings),
            spider_slug=slug,
            run_number=run_number,
            sample_limit=settings.getint("SNAPSHOTS_SAMPLE", DEFAULT_SAMPLE),
            error_limit=settings.getint("SNAPSHOTS_ERRORS", DEFAULT_ERRORS),
            max_bytes=settings.getint("SNAPSHOTS_MAX_BYTES", DEFAULT_MAX_BYTES),
            close_timeout=settings.getfloat("SNAPSHOTS_CLOSE_TIMEOUT", DEFAULT_CLOSE_TIMEOUT),
            crawler=crawler,
        )
        crawler.signals.connect(ext.response_received, signals.response_received)
        crawler.signals.connect(ext.spider_closed, signals.spider_closed)
        return ext

    # -- helpers -----------------------------------------------------------

    def _stats_inc(self, key: str, count: int = 1) -> None:
        stats = getattr(self.crawler, "stats", None)
        if stats is not None:
            try:
                stats.inc_value(key, count)
            except Exception:
                pass

    def _get_client(self):
        if self._client is None:
            from botocore.config import Config

            self._client = make_s3_client(
                self.s3.endpoint_url, self.s3.access_key, self.s3.secret_key,
                self.s3.region_name,
                config=Config(
                    connect_timeout=5, read_timeout=15,
                    retries={"max_attempts": 2, "mode": "standard"},
                ),
            )
        return self._client

    def _classify(self, response: Any) -> Optional[str]:
        """The snapshot kind for ``response``, or None. Reactor thread: cheap."""
        if self._samples >= self.sample_limit and self._errors >= self.error_limit:
            return None
        status = int(getattr(response, "status", 0) or 0)
        content_type = _header(response.headers, b"Content-Type")
        is_html = "html" in content_type.lower()
        if status < 400 and not is_html:
            return None
        if self._errors < self.error_limit:
            blocked = is_blocked(
                status, response.body or b"", content_type or None,
                _headers_dict(response.headers),
            )
            if blocked:
                return "blocked"
            if status >= 400:
                return "error"
        if status < 400 and is_html and self._samples < self.sample_limit:
            return "sample"
        return None

    # -- signals -------------------------------------------------------------

    def response_received(self, response, request, spider) -> None:
        try:
            self._capture(response, spider)
        except Exception as exc:
            logger.debug("SnapshotRecorder: capture failed for %s: %s",
                         getattr(response, "url", "?"), exc)

    def _capture(self, response: Any, spider: Any) -> None:
        if self._closed:
            return
        kind = self._classify(response)
        if kind is None:
            return
        body = response.body or b""
        if len(body) > self.max_bytes:
            self._stats_inc("snapshots/skipped_too_large")
            return
        with self._lock:
            if kind == "sample":
                if self._samples >= self.sample_limit:
                    return
                self._samples += 1
            else:
                if self._errors >= self.error_limit:
                    return
                self._errors += 1
            if not self.spider_slug and spider is not None:
                self.spider_slug = getattr(spider, "name", "") or ""
        pending = _Pending(
            url=response.url,
            status_code=int(response.status),
            kind=kind,
            captured_at=datetime.now(timezone.utc).isoformat(),
            body=bytes(body),
        )
        if self._executor is None:
            self._executor = futures.ThreadPoolExecutor(
                max_workers=2, thread_name_prefix="calyprium-snapshots",
            )
        self._futures.append(self._executor.submit(self._store, pending))

    # -- off-reactor work ------------------------------------------------------

    def _store(self, pending: _Pending) -> Optional[Dict[str, Any]]:
        """Gzip, hash and upload one page. Runs on the snapshot thread pool."""
        try:
            content_hash = hashlib.sha256(pending.body).hexdigest()
            key = snapshot_key(self.run_prefix, content_hash)
            with self._lock:
                already = content_hash in self._uploaded
            if not already:
                data = gzip.compress(pending.body, compresslevel=6, mtime=0)
                self._get_client().put_object(
                    Bucket=self.s3.bucket,
                    Key=key,
                    Body=BytesIO(data),
                    ContentLength=len(data),
                    ContentType="application/gzip",
                )
            entry = {
                "url": pending.url,
                "status_code": pending.status_code,
                "content_hash": content_hash,
                "storage_key": key,
                "kind": pending.kind,
                "captured_at": pending.captured_at,
            }
            with self._lock:
                self._uploaded.add(content_hash)
                self._snapshots.append(entry)
            self._stats_inc("snapshots/captured")
            return entry
        except Exception as exc:
            self._stats_inc("snapshots/upload_failed")
            logger.warning("SnapshotRecorder: upload of %s snapshot for %s failed: %s",
                           pending.kind, pending.url, exc)
            return None
        finally:
            pending.body = b""

    # -- close -------------------------------------------------------------------

    def spider_closed(self, spider, reason) -> None:
        self._closed = True
        try:
            self._finish()
        except Exception as exc:
            logger.warning("SnapshotRecorder: close failed: %s", exc)

    def _finish(self) -> None:
        if self._futures:
            done, not_done = futures.wait(self._futures, timeout=self.close_timeout)
            if not_done:
                logger.warning(
                    "SnapshotRecorder: %d snapshot upload(s) still pending after %.0fs; "
                    "reporting without them", len(not_done), self.close_timeout,
                )
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
        with self._lock:
            snapshots = list(self._snapshots)
        if snapshots:
            self.report(snapshots)

    def report(self, snapshots: List[Dict[str, Any]]) -> bool:
        """POST the snapshot list to forge. Never raises."""
        if not self.spider_slug:
            logger.warning("SnapshotRecorder: no spider slug; not reporting %d snapshots",
                           len(snapshots))
            return False
        url = (
            f"{self.forge_url}/jobs/spiders/{self.spider_slug}/runs/"
            f"{self.run_number}/snapshots"
        )
        try:
            response = self.auth.call(lambda h: httpx.post(
                url, json={"snapshots": snapshots}, headers=h, timeout=REPORT_TIMEOUT,
            ))
        except Exception as exc:
            logger.warning("SnapshotRecorder: reporting snapshots to forge failed: %s", exc)
            return False
        status = getattr(response, "status_code", 0)
        if status >= 400:
            logger.warning(
                "SnapshotRecorder: forge answered %s to the snapshot report%s",
                status, " (endpoint not available on this forge)" if status == 404 else "",
            )
            return False
        logger.info("SnapshotRecorder: reported %d snapshots for %s run %d",
                    len(snapshots), self.spider_slug, self.run_number)
        return True
