"""Real crawls through a gateway that refuses for a busy pool, and a paced one.

Scrapy's engine, scheduler and downloader run against a local site and a local gateway that answers a CONNECT
(or a plain-HTTP proxy request) with ``503 Service Unavailable (pool_busy)`` and ``X-Calyprium-Proxy-Error:
pool_busy`` a set number of times, then carries the request. The site is also Forge (traces and run stats) and
Mimic (a solve would be counted). Each crawl runs in a subprocess (a Twisted reactor cannot be restarted inside
pytest) on the asyncio reactor, the platform's.
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import pytest

from scrapy_calyprium.routing.local_fetch import is_local_fetch_available

PAGE = (b"<html><head><title>Widget | Shop</title></head><body><nav></nav><main>"
        + b"<a href='/p'>product</a>" * 5 + b"</main></body></html>")
BUSY_CONNECT = (b"HTTP/1.1 503 Service Unavailable (pool_busy)\r\n"
                b"X-Calyprium-Proxy-Error: pool_busy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
BUSY_PLAIN = (b"HTTP/1.1 503 Service Unavailable (pool_busy)\r\nX-Calyprium-Proxy-Error: pool_busy\r\n"
              b"Content-Type: text/plain\r\nContent-Length: 9\r\nConnection: close\r\n\r\npool_busy")

SPIDER = '''
import json
import sys
import time

import scrapy
from scrapy.crawler import CrawlerProcess

site, proxy, mode, out = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
seen, failed = [], []
RUN = {"FORGE_API_URL": site, "RECRAWL_SPIDER_SLUG": "shop", "RECRAWL_USER_ID": "u-1",
       "SPIDER_RUN_NUMBER": 7}
settings = {
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "LOG_LEVEL": "WARNING", "ROBOTSTXT_OBEY": False, "TELNETCONSOLE_ENABLED": False,
    "CALYPRIUM_API_KEY": "clp_test", "CONCURRENT_REQUESTS": 8,
    "CALYPRIUM_POOL_BUSY_DELAY": 0.05, "CALYPRIUM_POOL_BUSY_MAX_DELAY": 0.1,
    "DOWNLOADER_MIDDLEWARES": {
        "scrapy_calyprium.middleware.tracing.CalypriumTraceMiddleware": 50,
        "scrapy_calyprium.middleware.portfolio.PaceMiddleware": 120,
        "scrapy_calyprium.middleware.mimic.MimicBrowserMiddleware": 200,
        "scrapy_calyprium.middleware.portfolio.PoolBusyMiddleware": 950,
    },
    "EXTENSIONS": {
        "scrapy_calyprium.extensions.run_stats.CalypriumRunStats": 500,
        "scrapy_calyprium.extensions.request_tracer.CalypriumRequestTracer": 501,
    },
    "MIMIC_SERVICE_URL": site, "MIMIC_SLOT_STATS_INTERVAL": 0, **RUN,
}
urls, meta = [], {}
if mode == "local":  # Mimic's local route, through the gateway
    settings.update(MIMIC_LOCAL_FETCH=True, MIMIC_LOCAL_PROXY_URL=proxy)
    urls = [site + "/local"]
elif mode == "scrapy_http":  # Scrapy downloads, a plain-HTTP proxy request (Scrapy's retries on)
    urls, meta = [site + "/plain"], {"proxy": proxy}
elif mode == "scrapy_https_cap":  # Scrapy downloads over a CONNECT the gateway always refuses
    settings.update(CALYPRIUM_POOL_BUSY_MAX_REQUEUES=2)
    settings["DOWNLOADER_MIDDLEWARES"]["scrapy.downloadermiddlewares.retry.RetryMiddleware"] = None
    urls, meta = [site.replace("http://", "https://") + "/tls"], {"proxy": proxy}
elif mode == "pace":  # the local route, no proxy, 120 pages/min
    settings.update(MIMIC_LOCAL_FETCH=True, CALYPRIUM_PACE_PPM=120, MIMIC_PACING=False)
    urls = [site + "/p%d" % i for i in range(4)]


class Shop(scrapy.Spider):
    name = "shop"

    async def start(self):  # Scrapy >= 2.13 starts here
        for request in self.start_requests():
            yield request

    def start_requests(self):
        for url in urls:
            yield scrapy.Request(url, meta=dict(meta), errback=self.failed)

    def parse(self, response):
        seen.append({"url": response.url, "status": response.status,
                     "route": response.meta.get("mimic_local_route"),
                     "busy": response.meta.get("calyprium_pool_busy"),
                     "retry_times": response.meta.get("retry_times"),
                     "attempt": response.meta.get("calyprium_attempt_number"),
                     "class": response.meta.get("calyprium_diagnosis", {}).get("class")})

    def failed(self, failure):
        failed.append({"type": type(failure.value).__name__, "error": str(failure.value),
                       "busy": failure.request.meta.get("calyprium_pool_busy")})


process = CrawlerProcess(settings)
crawler = process.create_crawler(Shop)
process.crawl(crawler)
process.start()
stats = {k: v for k, v in crawler.stats.get_stats().items()
         if k.startswith(("calyprium/", "mimic/", "downloader/exception", "downloader/response_status"))}
with open(out, "w") as fh:
    json.dump({"seen": seen, "failed": failed, "stats": stats}, fh)
'''


class Site:
    """The shop (any path is a product page), Forge (spans, run stats) and Mimic (/api/solve, counted)."""

    def __init__(self):
        self.hits = []
        self.posts = []
        self.lock = threading.Lock()

    def handler(self):
        site = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, ctype="text/html"):
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                with site.lock:
                    site.hits.append((urlparse(self.path).path, time.monotonic()))
                self._send(200, PAGE)

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                with site.lock:
                    site.posts.append((self.path, json.loads(body or b"null")))
                if self.path.startswith("/api/"):
                    return self._send(500, b'{"detail": "no solves here"}', "application/json")
                self._send(200, b'{"status": "ok"}', "application/json")

        return Handler


class Gateway:
    """Refuses the first ``busy`` proxy requests for a busy pool, then carries a CONNECT to its target (a plain
    proxy request is answered with the page)."""

    def __init__(self, busy: int):
        self.busy = busy
        self.requests = Counter()
        self.lock = threading.Lock()
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.port = self.sock.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = conn.recv(65536)
            if not chunk:
                conn.close()
                return
            data += chunk
        head, _, rest = data.partition(b"\r\n\r\n")
        method, target = head.split(b" ")[:2]
        kind = "connect" if method == b"CONNECT" else "plain"
        with self.lock:
            self.requests[kind] += 1
            refuse = self.busy > 0
            self.busy -= 1 if refuse else 0
        if refuse:
            conn.sendall(BUSY_CONNECT if kind == "connect" else BUSY_PLAIN)
            conn.close()
            return
        if kind == "plain":
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: %d\r\n"
                         b"Connection: close\r\n\r\n" % len(PAGE) + PAGE)
            conn.close()
            return
        host, port = target.decode().rsplit(":", 1)
        upstream = socket.create_connection((host, int(port)))
        conn.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        if rest:
            upstream.sendall(rest)

        def pump(src, dst):
            try:
                while True:
                    chunk = src.recv(65536)
                    if not chunk:
                        break
                    dst.sendall(chunk)
            except OSError:
                pass
            finally:
                for s in (src, dst):
                    try:
                        s.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

        threading.Thread(target=pump, args=(upstream, conn), daemon=True).start()
        pump(conn, upstream)

    def close(self):
        self.sock.close()


@pytest.fixture
def site():
    s = Site()
    server = ThreadingHTTPServer(("127.0.0.1", 0), s.handler())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    s.url = "http://127.0.0.1:%d" % server.server_address[1]
    yield s
    server.shutdown()


def _crawl(site, tmp_path, mode, gateway=None):
    script, out = tmp_path / "crawl.py", tmp_path / "out.json"
    script.write_text(SPIDER, encoding="utf-8")
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("CALYPRIUM_", "SPIDER_", "VEIL_", "MIMIC_"))}
    proxy = "http://calyprium:clp_test@127.0.0.1:%d" % gateway.port if gateway else ""
    proc = subprocess.run([sys.executable, str(script), site.url, proxy, mode, str(out)],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-3000:]
    return json.loads(out.read_text())


def _spans(site):
    return [s for path, body in site.posts if path.endswith("/traces") for s in body["spans"]]


def _report(site):
    return [body for path, body in site.posts if path.endswith("/stats")][-1]


@pytest.mark.skipif(not is_local_fetch_available(), reason="needs a local fetch backend")
def test_the_local_route_requeues_refusals_without_solving_failing_or_counting_them(site, tmp_path):
    gateway = Gateway(busy=2)
    try:
        result = _crawl(site, tmp_path, "local", gateway)
    finally:
        gateway.close()
    (page,) = result["seen"]
    assert (page["status"], page["route"], page["class"]) == (200, "httpcloak_light", "ok")
    # httpcloak 1.7.2 dials a refused proxy twice before it gives up, so one fetch may meet both refusals
    busy = page["busy"]
    assert busy in (1, 2)
    assert page["retry_times"] is None and not page["attempt"]  # no retry was spent
    assert gateway.requests["connect"] == 3 and [p for p, _ in site.hits] == ["/local"]
    assert not [p for p, _ in site.posts if p.startswith("/api/")]  # nothing solved
    stats = result["stats"]
    assert (stats["calyprium/pool_busy"], stats["calyprium/pool_busy/requeued"]) == (busy, busy)
    assert "calyprium/retry/count" not in stats and "downloader/exception_count" not in stats
    assert stats["calyprium/pool_busy/concurrency"] == 6  # 8 x 3/4
    # One priced attempt, a success; the refusals are no attempts and no failures.
    assert [(s["url"].rsplit("/", 1)[1], s["status"]) for s in _spans(site)] == [("local", "success")]
    report = _report(site)
    assert (report["request_count"], report["response_count"], report["pool_busy_count"]) == (1, 1, busy)
    assert report["failure_class_counts"] == {"ok": 1}
    assert report["status_code_counts"] == {"200": 1}


def test_a_plain_http_refusal_is_requeued_without_spending_scrapys_retries(site, tmp_path):
    gateway = Gateway(busy=2)
    try:
        result = _crawl(site, tmp_path, "scrapy_http", gateway)
    finally:
        gateway.close()
    (page,) = result["seen"]
    assert (page["status"], page["busy"], page["retry_times"]) == (200, 2, None)
    assert gateway.requests["plain"] == 3
    stats = result["stats"]
    assert stats["calyprium/pool_busy/requeued"] == 2
    assert "downloader/response_status_count/503" not in stats  # the refusals never reach DownloaderStats
    assert _report(site)["status_code_counts"] == {"200": 1}


def test_a_refused_tunnel_is_requeued_up_to_the_cap_then_handled_as_usual(site, tmp_path):
    gateway = Gateway(busy=1000)
    try:
        result = _crawl(site, tmp_path, "scrapy_https_cap", gateway)
    finally:
        gateway.close()
    assert result["seen"] == []
    (failure,) = result["failed"]
    assert failure["type"] == "TunnelError" and "(pool_busy)" in failure["error"]
    assert failure["busy"] == 2
    # The first try and two requeues; then the usual handling: Mimic's two transport retries (Scrapy's
    # RetryMiddleware is off, as on the platform), refused again and handed on at once.
    assert gateway.requests["connect"] == 5
    stats = result["stats"]
    assert (stats["calyprium/pool_busy"], stats["calyprium/pool_busy/requeued"],
            stats["calyprium/pool_busy/max_reached"]) == (5, 2, 3)
    assert stats["mimic/transport_retry/count"] == 2
    assert stats["downloader/exception_count"] == 3  # only the refusals handed on


@pytest.mark.skipif(not is_local_fetch_available(), reason="needs a local fetch backend")
def test_pace_holds_the_local_route_to_its_pages_per_minute(site, tmp_path):
    result = _crawl(site, tmp_path, "pace")
    assert len(result["seen"]) == 4 and all(p["route"] == "httpcloak_light" for p in result["seen"])
    times = sorted(t for _, t in site.hits)
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert len(gaps) == 3 and min(gaps) >= 0.4, gaps  # 120/min: one every 0.5 s, CONCURRENT_REQUESTS 8
    assert result["stats"]["calyprium/pace/delayed"] == 3
