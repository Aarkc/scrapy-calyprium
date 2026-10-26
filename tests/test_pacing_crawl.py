"""A real crawl on the local route keeps the spider's DOWNLOAD_DELAY between the site's hits.

Scrapy's engine, scheduler and downloader run with MimicBrowserMiddleware answering every request
on the local route (in-process fetch, no proxy) against a local server, which notes when each
request arrived. Runs in a subprocess (a Twisted reactor cannot be restarted inside pytest).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from scrapy_calyprium.routing.local_fetch import is_local_fetch_available

PAGE = (b"<html><head><title>Widget | Shop</title></head><body><nav></nav><main>"
        + b"<a href='/p'>product</a>" * 40 + b"</main><footer></footer></body></html>")

SPIDER = '''
import json
import sys

import scrapy
from scrapy.crawler import CrawlerProcess

port, pacing, out = sys.argv[1], sys.argv[2] == "1", sys.argv[3]
base = "http://127.0.0.1:" + port
routes = []


class Shop(scrapy.Spider):
    name = "shop"

    async def start(self):  # Scrapy >= 2.13 starts here
        for request in self.start_requests():
            yield request

    def start_requests(self):
        for i in range(4):
            yield scrapy.Request(base + "/p%d" % i)

    def parse(self, response):
        routes.append(response.meta.get("mimic_local_route"))


process = CrawlerProcess({
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "LOG_LEVEL": "WARNING", "ROBOTSTXT_OBEY": False, "TELNETCONSOLE_ENABLED": False,
    "DOWNLOADER_MIDDLEWARES": {"scrapy_calyprium.middleware.mimic.MimicBrowserMiddleware": 200},
    "MIMIC_SERVICE_URL": "http://127.0.0.1:9", "CALYPRIUM_API_KEY": "clp_test",
    "MIMIC_LOCAL_FETCH": True, "MIMIC_SLOT_STATS_INTERVAL": 0,
    "CONCURRENT_REQUESTS": 8, "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
    "DOWNLOAD_DELAY": 0.4, "RANDOMIZE_DOWNLOAD_DELAY": False,
    "MIMIC_PACING": pacing,
})
process.crawl(Shop)
process.start()
with open(out, "w") as fh:
    json.dump({"routes": routes}, fh)
'''


class Site:
    def __init__(self):
        self.hits = []
        self.lock = threading.Lock()

    def handler(self):
        site = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                with site.lock:
                    site.hits.append(time.monotonic())
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(PAGE)))
                self.end_headers()
                self.wfile.write(PAGE)

        return Handler


@pytest.fixture
def site():
    s = Site()
    server = ThreadingHTTPServer(("127.0.0.1", 0), s.handler())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    s.port = server.server_address[1]
    yield s
    server.shutdown()


def _crawl(site, tmp_path, pacing: bool):
    script, out = tmp_path / "crawl.py", tmp_path / "out.json"
    script.write_text(SPIDER, encoding="utf-8")
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("CALYPRIUM_", "SPIDER_", "VEIL_", "MIMIC_"))}
    proc = subprocess.run([sys.executable, str(script), str(site.port), "1" if pacing else "0", str(out)],
                          cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-3000:]
    result = json.loads(out.read_text())
    gaps = [b - a for a, b in zip(site.hits, site.hits[1:])]
    return result["routes"], gaps


@pytest.mark.skipif(not is_local_fetch_available(), reason="needs a local fetch backend")
def test_the_local_route_keeps_download_delay(site, tmp_path):
    routes, gaps = _crawl(site, tmp_path, pacing=True)
    assert len(routes) == 4 and all(r and r.startswith("httpcloak") for r in routes), routes
    assert len(gaps) == 3 and min(gaps) >= 0.35, gaps


@pytest.mark.skipif(not is_local_fetch_available(), reason="needs a local fetch backend")
def test_without_pacing_the_local_route_ignores_it(site, tmp_path):
    routes, gaps = _crawl(site, tmp_path, pacing=False)
    assert len(routes) == 4 and all(r and r.startswith("httpcloak") for r in routes), routes
    assert min(gaps) < 0.35, gaps  # what every spider got before
