"""A real crawl against a local server: Scrapy's engine, scheduler and downloader run the
Calyprium retry, wait for it, trace every attempt once and count the final classes.

Runs in a subprocess (a Twisted reactor cannot be restarted inside pytest), once on the asyncio
reactor (the platform's) and once on Twisted's default reactor (the deferLater branch).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

PAGE = (b"<html><head><title>Widget | Shop</title></head><body><nav></nav><main>"
        + b"<a href='/p'>product</a>" * 5 + b"</main></body></html>")
CF_CHALLENGE = (b"<html><head><title>Just a moment...</title></head><body>"
                b"<script>window._cf_chl_opt={cvId:'3'}</script></body></html>")

SPIDER = '''
import json
import sys
import time

import scrapy
from scrapy.crawler import CrawlerProcess

port, reactor, out = sys.argv[1], sys.argv[2], sys.argv[3]
base = "http://127.0.0.1:" + port
seen = []


class Shop(scrapy.Spider):
    name = "shop"

    async def start(self):  # Scrapy >= 2.13 starts here (>= 2.16 never calls start_requests)
        for request in self.start_requests():
            yield request

    def start_requests(self):
        for path in ("/rate", "/challenge", "/gone", "/broken"):
            yield scrapy.Request(base + path, meta={"handle_httpstatus_all": True,
                                                    "t0": time.monotonic()})

    def parse(self, response):
        seen.append({
            "path": response.url[len(base):], "status": response.status,
            "class": response.meta["calyprium_diagnosis"]["class"],
            "attempt": response.meta.get("calyprium_attempt_number", 0),
            "waited": time.monotonic() - response.meta["t0"],
        })


process = CrawlerProcess({
    "TWISTED_REACTOR": reactor or None,
    "LOG_LEVEL": "WARNING", "ROBOTSTXT_OBEY": False, "TELNETCONSOLE_ENABLED": False,
    "DOWNLOADER_MIDDLEWARES": {
        "scrapy_calyprium.middleware.tracing.CalypriumTraceMiddleware": 50,
        "scrapy_calyprium.middleware.mimic.MimicBrowserMiddleware": 200,
        "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
    },
    "EXTENSIONS": {
        "scrapy_calyprium.extensions.run_stats.CalypriumRunStats": 500,
        "scrapy_calyprium.extensions.request_tracer.CalypriumRequestTracer": 501,
    },
    "MIMIC_SERVICE_URL": "http://127.0.0.1:9", "CALYPRIUM_API_KEY": "clp_test",
    "FORGE_API_URL": base, "RECRAWL_SPIDER_SLUG": "shop", "RECRAWL_USER_ID": "u-1",
    "SPIDER_RUN_NUMBER": 7, "CALYPRIUM_RETRY_DELAY": 0.2,
})
crawler = process.create_crawler(Shop)
process.crawl(crawler)
process.start()
stats = {k: v for k, v in crawler.stats.get_stats().items() if k.startswith("calyprium/")}
with open(out, "w") as fh:
    json.dump({"seen": seen, "stats": stats}, fh)
'''


class Site:
    """A shop that rate-limits once, challenges once, lacks a page and is broken on another;
    also Forge, to receive the tracer's spans and the run stats."""

    def __init__(self):
        self.hits: Counter = Counter()
        self.posts = []
        self.lock = threading.Lock()

    def handler(self):
        site = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, headers=None):
                self.send_response(status)
                for key, value in {"Content-Type": "text/html", **(headers or {})}.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                with site.lock:
                    site.hits[self.path] += 1
                    n = site.hits[self.path]
                if self.path == "/rate":
                    return self._send(429, b"<html>slow down</html>", {"Retry-After": "1"}) \
                        if n == 1 else self._send(200, PAGE)
                if self.path == "/challenge":
                    return self._send(403, CF_CHALLENGE, {"Server": "cloudflare"}) \
                        if n == 1 else self._send(200, PAGE)
                if self.path == "/gone":
                    return self._send(404, PAGE)
                if self.path == "/broken":
                    return self._send(500, b"<html>error</html>")
                return self._send(200, PAGE)

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                with site.lock:
                    site.posts.append((self.path, json.loads(body or b"null")))
                self._send(200, b'{"status": "ok"}', {"Content-Type": "application/json"})

        return Handler


@pytest.fixture
def site():
    s = Site()
    server = ThreadingHTTPServer(("127.0.0.1", 0), s.handler())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    s.port = server.server_address[1]
    yield s
    server.shutdown()


@pytest.mark.parametrize("reactor", [
    "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "",  # Twisted's default reactor
])
def test_a_real_crawl_retries_waits_traces_and_counts(site, tmp_path, reactor):
    script, out = tmp_path / "crawl.py", tmp_path / "out.json"
    script.write_text(SPIDER, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if not k.startswith(("CALYPRIUM_", "SPIDER_"))}
    proc = subprocess.run([sys.executable, str(script), str(site.port), reactor, str(out)],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-3000:]
    result = json.loads(out.read_text())
    seen = {s["path"]: s for s in result["seen"]}

    assert (seen["/rate"]["status"], seen["/rate"]["attempt"]) == (200, 1)
    assert seen["/rate"]["waited"] >= 1.0  # Retry-After: 1 was honoured
    assert (seen["/challenge"]["status"], seen["/challenge"]["attempt"]) == (200, 1)
    assert (seen["/gone"]["status"], seen["/gone"]["class"], seen["/gone"]["attempt"]) == (
        404, "not_found", 0)
    assert (seen["/broken"]["status"], seen["/broken"]["class"], seen["/broken"]["attempt"]) == (
        500, "origin_error", 2)
    assert dict(site.hits) == {"/rate": 2, "/challenge": 2, "/gone": 1, "/broken": 3}

    stats = result["stats"]
    assert stats["calyprium/retry/rate_limited"] == 1
    assert stats["calyprium/retry/challenge"] == 1
    assert stats["calyprium/retry/origin_error"] == 2
    assert stats["calyprium/retry/max_reached"] == 1
    assert (stats["calyprium/class/ok"], stats["calyprium/class/not_found"],
            stats["calyprium/class/origin_error"]) == (2, 1, 1)

    # One span per attempt that went out (8), each with its class and attempt number.
    spans = [s for path, body in site.posts if path.endswith("/traces") for s in body["spans"]]
    by_path = {}
    for span in spans:
        by_path.setdefault(span["url"].split(str(site.port))[1], []).append(
            (span["attempt"], span["status"], span["failure_class"]))
    assert sorted(by_path["/rate"]) == [(0, "blocked", "rate_limited"), (1, "success", "ok")]
    assert sorted(by_path["/challenge"]) == [(0, "blocked", "challenge"), (1, "success", "ok")]
    assert by_path["/gone"] == [(0, "error", "not_found")]
    assert sorted(by_path["/broken"]) == [(n, "error", "origin_error") for n in range(3)]

    (report, *_) = [body for path, body in reversed(site.posts) if path.endswith("/stats")]
    assert report["failure_class_counts"] == {"ok": 2, "not_found": 1, "origin_error": 1}
    assert report["routing_method_counts"] == {"direct": 4}
