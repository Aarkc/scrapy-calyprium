"""PrismSitemapSpider against a local fake Prism, in a real crawl (subprocess: one reactor each).

A domain Prism never indexes ends the run as ``prism_no_urls`` after a handful of requests (not
thousands), having asked Prism to scan it once with the run's key; a domain Prism knows only under
its other www form is crawled from that form.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

PAGE = (b"<html><head><title>Widget | Shop</title></head><body><nav></nav><main>"
        + b"<a href='/p'>product</a>" * 5 + b"</main></body></html>")

SPIDER = '''
import json
import sys

from scrapy.crawler import CrawlerProcess

from scrapy_calyprium.spiders import PrismSitemapSpider

port, reactor, out, domain = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]


class Products(PrismSitemapSpider):
    name = "products"
    prism_domain = domain

    async def start(self):  # Scrapy >= 2.13 starts here (>= 2.16 never calls start_requests)
        for request in self.start_requests():
            yield request

    def parse_item(self, response):
        yield {"url": response.url}


process = CrawlerProcess({
    "TWISTED_REACTOR": reactor or None,
    "LOG_LEVEL": "WARNING", "ROBOTSTXT_OBEY": False, "TELNETCONSOLE_ENABLED": False,
    "PRISM_URL": "http://127.0.0.1:" + port, "CALYPRIUM_API_KEY": "clp_test",
    "PRISM_WAIT_FOR_SCAN_S": 3,
})
crawler = process.create_crawler(Products)
process.crawl(crawler)
process.start()
stats = crawler.stats.get_stats()
with open(out, "w") as fh:
    json.dump({"finish_reason": stats.get("finish_reason"),
               "items": stats.get("item_scraped_count", 0),
               "prism": {k: v for k, v in stats.items() if k.startswith("prism/")}}, fh)
'''


class FakePrism:
    def __init__(self, indexed=()):
        self.indexed = set(indexed)
        self.url_calls = []
        self.scans = []
        self.lock = threading.Lock()

    def handler(self):
        prism = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, ctype="application/json"):
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parts = urlparse(self.path)
                if parts.path.startswith("/product/"):
                    return self._send(200, PAGE, "text/html")
                domain = parts.path.split("/")[3]
                with prism.lock:
                    prism.url_calls.append((time.monotonic(), domain))
                if domain not in prism.indexed:
                    return self._send(404, b'{"detail": "No sitemap found"}')
                offset = int(parse_qs(parts.query).get("offset", ["0"])[0])
                base = f"http://127.0.0.1:{self.server.server_address[1]}"
                urls = [f"{base}/product/1", f"{base}/product/2"] if offset == 0 else []
                return self._send(200, json.dumps({"urls": urls, "total": 2}).encode())

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                with prism.lock:
                    prism.scans.append((self.path, self.headers.get("Authorization"),
                                        json.loads(body or b"null")))
                self._send(202, b'{"status": "pending"}')

        return Handler


def _crawl(tmp_path, prism, reactor, domain):
    server = ThreadingHTTPServer(("127.0.0.1", 0), prism.handler())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        script, out = tmp_path / "crawl.py", tmp_path / "out.json"
        script.write_text(SPIDER, encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if not k.startswith(("CALYPRIUM_", "SPIDER_"))}
        started = time.monotonic()
        proc = subprocess.run(
            [sys.executable, str(script), str(server.server_address[1]), reactor, str(out), domain],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
        elapsed = time.monotonic() - started
    finally:
        server.shutdown()
    assert proc.returncode == 0, proc.stderr[-3000:]
    return json.loads(out.read_text()), elapsed


@pytest.mark.parametrize("reactor", [
    "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "",  # Twisted's default reactor
])
def test_an_unindexed_domain_ends_as_prism_no_urls_without_spinning(tmp_path, reactor):
    prism = FakePrism()
    result, elapsed = _crawl(tmp_path, prism, reactor, "nowhere.test")
    assert result["finish_reason"] == "prism_no_urls"
    assert result["items"] == 0
    assert result["prism"]["prism/no_urls"] == 1
    assert result["prism"]["prism/scan_requested"] == 1
    domains = [d for _, d in prism.url_calls]
    assert domains[:2] == ["nowhere.test", "www.nowhere.test"]   # the other www form, once
    assert set(domains[2:]) == {"nowhere.test"}
    assert len(domains) <= 6                                      # not thousands
    gaps = [b[0] - a[0] for a, b in zip(prism.url_calls[1:], prism.url_calls[2:])]
    assert all(g >= 1.0 for g in gaps)                            # polled with backoff
    assert elapsed >= 3
    ((path, auth, body),) = prism.scans
    assert path == "/api/sitemaps/fetch?domain=nowhere.test"
    assert auth == "Bearer clp_test"
    assert body == {"scan_type": "full", "force": False}


def test_a_domain_prism_knows_as_www_is_crawled_from_there(tmp_path):
    prism = FakePrism(indexed={"www.ti.test"})
    result, _ = _crawl(tmp_path, prism, "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
                       "ti.test")
    assert result["finish_reason"] == "finished"
    assert result["items"] == 2
    assert result["prism"]["prism/domain_switched"] == 1
    assert prism.scans == []
