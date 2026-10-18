"""PrismSitemapSpider link discovery in a real crawl (subprocess: one reactor each), against a
local server that is both Prism and the shop.

Prism has no URL under the spider's prefix (the sitemap lists none), so the run finds the product
pages from the shop's own links: never the cart, sign-in or news pages, and the spider closes as
``prism_no_matching_urls`` when no page anywhere matches.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

SPIDER = '''
import json
import sys

from scrapy.crawler import CrawlerProcess

from scrapy_calyprium.spiders import PrismSitemapSpider

port, out, prefix = sys.argv[1], sys.argv[2], sys.argv[3]


class Shop(PrismSitemapSpider):
    name = "shop"
    prism_domain = "shop.test"
    prism_path_prefix = prefix
    allowed_domains = ["127.0.0.1"]

    async def start(self):  # Scrapy >= 2.13 starts here (>= 2.16 never calls start_requests)
        for request in self.start_requests():
            yield request

    def parse_item(self, response):
        yield {"url": response.url, "name": response.css("h1::text").get()}


process = CrawlerProcess({
    "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    "LOG_LEVEL": "WARNING", "ROBOTSTXT_OBEY": False, "TELNETCONSOLE_ENABLED": False,
    "PRISM_URL": "http://127.0.0.1:" + port, "CALYPRIUM_API_KEY": "clp_test",
    "DNS_TIMEOUT": 2, "DOWNLOAD_TIMEOUT": 5,
})
crawler = process.create_crawler(Shop)
process.crawl(crawler)
process.start()
stats = crawler.stats.get_stats()
with open(out, "w") as fh:
    json.dump({"finish_reason": stats.get("finish_reason"),
               "items": stats.get("item_scraped_count", 0),
               "prism": {k: v for k, v in stats.items() if k.startswith("prism/")}}, fh)
'''

SHOP = {
    "/catalog/": ["/catalog/page/2", "/item/1", "/item/2", "/cart/", "/login", "/news/launch",
                  "/catalog/?sort=price"],
    "/catalog/?sort=price": ["/item/2", "/item/1"],
    "/catalog/page/2": ["/item/3", "/catalog/"],
    "/about/": ["/careers/"],
    "/item/1": ["/item/2", "/catalog/"],
    "/item/2": ["/item/1"],
    "/item/3": [],
}


class FakePrismShop:
    def __init__(self):
        self.visited = []
        self.lock = threading.Lock()

    def handler(self):
        shop = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, ctype):
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parts = urlparse(self.path)
                base = f"http://127.0.0.1:{self.server.server_address[1]}"
                if parts.path.startswith("/api/domains/"):
                    query = parse_qs(parts.query)
                    if "path_prefix" in query:  # the sitemap lists no product page
                        body = {"urls": [], "total": 0}
                    else:
                        urls = [f"{base}/catalog/", f"{base}/about/"]
                        body = {"urls": urls, "total": len(urls)}
                    return self._send(200, json.dumps(body).encode(), "application/json")
                page = parts.path + (f"?{parts.query}" if parts.query else "")
                with shop.lock:
                    shop.visited.append(page)
                if page not in SHOP:
                    return self._send(404, b"<html>not here</html>", "text/html")
                links = "".join(f"<a href='{h}'>{h}</a>" for h in SHOP[page])
                title = f"<h1>Widget {page.rsplit('/', 1)[-1]}</h1>" if page.startswith("/item/") else ""
                body = f"<html><body>{title}<nav>{links}</nav></body></html>".encode()
                return self._send(200, body, "text/html; charset=utf-8")

        return Handler


def _crawl(tmp_path, prefix):
    site = FakePrismShop()
    server = ThreadingHTTPServer(("127.0.0.1", 0), site.handler())
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        script, out = tmp_path / "crawl.py", tmp_path / "out.json"
        script.write_text(SPIDER, encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if not k.startswith(("CALYPRIUM_", "SPIDER_"))}
        proc = subprocess.run(
            [sys.executable, str(script), str(server.server_address[1]), str(out), prefix],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
    finally:
        server.shutdown()
    assert proc.returncode == 0, proc.stderr[-3000:]
    return json.loads(out.read_text()), site.visited


def test_product_pages_missing_from_the_sitemap_are_found_from_the_shops_links(tmp_path):
    result, visited = _crawl(tmp_path, "/item/")
    assert result["finish_reason"] == "finished"
    assert result["items"] == 3
    assert result["prism"]["prism/discovery_started"] == 1
    assert result["prism"]["prism/discovery_targets"] == 3
    assert sorted(p for p in visited if p.startswith("/item/")) == ["/item/1", "/item/2", "/item/3"]
    assert not [p for p in visited if p.startswith(("/cart", "/login", "/news", "/careers"))]
    assert len(visited) == len(set(visited))  # each page once


def test_a_filter_no_page_matches_ends_as_prism_no_matching_urls(tmp_path):
    result, visited = _crawl(tmp_path, "/nothing-here/")
    assert result["finish_reason"] == "prism_no_matching_urls"
    assert result["items"] == 0
    assert result["prism"]["prism/discovery_found_nothing"] == 1
    assert "/catalog/" in visited
