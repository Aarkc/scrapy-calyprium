"""PrismSitemapSpider: link discovery when the spider's filter matches none of Prism's URLs, and
Prism URLs off the spider's domains.

Found in the 2026-10-18 fleet check: 64 Prism-fed spiders filter on a path the site's sitemap
never lists (hirose's /en/product/p/, nkk's /products/, eaton's skuPage.), so they ended with no
request; andersonpower.com's sitemap now lists idealelectricalinc.com, so every URL was dropped
as offsite; us.mersen.com's sitemap lists its pages on http://default/.
"""
from __future__ import annotations

import json
from unittest import mock
from urllib.parse import parse_qs, urlparse

import pytest
from scrapy.exceptions import CloseSpider, DontCloseSpider
from scrapy.http import HtmlResponse, TextResponse

from scrapy_calyprium.spiders.prism_sitemap import (
    _DISCOVERY_DENY,
    PrismSitemapSpider,
    _placeholder_host_fixed,
)
from tests._helpers import make_spider

PRISM = "http://prism.test"
SITE = "https://www.hirose.com"


class Hirose(PrismSitemapSpider):
    name = "hirose"
    prism_domain = "www.hirose.com"
    prism_path_prefix = "/en/product/p/"
    allowed_domains = ["www.hirose.com", "hirose.com"]

    def parse_item(self, response):
        yield {"url": response.url}


class Unfiltered(PrismSitemapSpider):
    name = "unfiltered"
    prism_domain = "www.hirose.com"

    def parse_item(self, response):
        yield {"url": response.url}


class Anderson(PrismSitemapSpider):
    name = "anderson"
    prism_domain = "www.andersonpower.com"
    allowed_domains = ["www.andersonpower.com", "andersonpower.com"]

    def parse_item(self, response):
        yield {"url": response.url}


def _spider(cls=Hirose, **settings):
    sp = make_spider(cls, {"PRISM_URL": PRISM, **settings}, batch_size=5)
    (refill,) = list(sp.start_requests())
    return sp, refill


def _page(request, urls, total=None):
    body = json.dumps({"urls": urls, "total": len(urls) if total is None else total}).encode()
    return TextResponse(request.url, body=body, request=request)


def _html(request, *hrefs):
    links = "".join(f"<a href='{h}'>x</a>" for h in hrefs)
    return HtmlResponse(request.url, body=f"<html><body>{links}</body></html>".encode(),
                        request=request, encoding="utf-8")


def _start(sp, refill, prism_urls=()):
    """The filter matches nothing; the unfiltered listing has ``prism_urls``."""
    (seeds,) = list(sp._handle_prism_page(_page(refill, [], total=0)))
    return seeds, list(sp._handle_discovery_seeds(_page(seeds, list(prism_urls))))


def _stat(sp, key):
    return sp.crawler.stats.get_value(key)


# -- starting ------------------------------------------------------------------------------

def test_a_filter_matching_nothing_reads_the_unfiltered_listing_for_seeds():
    sp, refill = _spider()
    assert parse_qs(urlparse(refill.url).query)["path_prefix"] == ["/en/product/p/"]
    (seeds,) = list(sp._handle_prism_page(_page(refill, [], total=0)))
    query = parse_qs(urlparse(seeds.url).query)
    assert "path_prefix" not in query and "pattern" not in query
    assert urlparse(seeds.url).path == "/api/domains/www.hirose.com/urls"
    assert seeds.meta["_internal"] and seeds.dont_filter
    assert sp._prism_exhausted
    assert _stat(sp, "prism/discovery_started") == 1


def test_seeds_are_the_pages_above_the_prefix_the_nearest_prism_urls_then_home():
    sp, refill = _spider()
    _, requests = _start(sp, refill, [f"{SITE}/corporate/en/about", f"{SITE}/en/product/series/DF13"])
    assert [r.url for r in requests] == [
        f"{SITE}/en/product/",            # the prefix's parent
        f"{SITE}/en/product/series/DF13",  # nearest Prism URL (shares /en/product/)
        f"{SITE}/en/",
        f"{SITE}/",                        # home last; /corporate/en/about shares nothing
    ]
    assert all(r.callback == sp._discovery_page for r in requests)
    assert _stat(sp, "prism/discovery_pages") == 4


def test_the_seed_listing_failing_still_starts_from_home():
    sp, refill = _spider()
    (seeds,) = list(sp._handle_prism_page(_page(refill, [], total=0)))
    requests = list(sp._discovery_seeds_failed(None))
    assert f"{SITE}/" in [r.url for r in requests]


def test_discovery_off_keeps_the_old_ending():
    sp, refill = _spider(PRISM_DISCOVERY_ENABLED=False)
    assert list(sp._handle_prism_page(_page(refill, [], total=0))) == []
    assert sp._prism_exhausted and not sp._discovery_on


def test_an_unfiltered_spider_never_discovers():
    sp, refill = _spider(Unfiltered)
    assert list(sp._handle_prism_page(_page(refill, [], total=0))) == []
    assert not sp._discovery_on


def test_a_filter_with_matches_never_discovers():
    sp, refill = _spider()
    out = list(sp._handle_prism_page(_page(refill, [f"{SITE}/en/product/p/DF13-2P"])))
    assert [r.url for r in out] == [f"{SITE}/en/product/p/DF13-2P"]
    assert not sp._discovery_on


# -- crawling ------------------------------------------------------------------------------

def test_matching_links_go_to_parse_item_and_the_rest_join_the_frontier():
    sp, refill = _spider()
    _, (listing, *_) = _start(sp, refill)
    out = list(sp._discovery_page(_html(
        listing,
        "/en/product/p/DF13-2P", "/en/product/p/DF13-2P#specs",   # a target, once
        "/en/product/series/DF14",                                # frontier
        "/cart/", "/en/login", "/en/news/2026", "/my-account/orders",  # never followed
        "https://other.example/en/product/p/X",                    # off the site
    )))
    targets = [r for r in out if r.callback == sp._discovery_parse]
    assert [r.url for r in targets] == [f"{SITE}/en/product/p/DF13-2P"]
    assert targets[0].priority > max(r.priority for r in out if r not in targets)
    frontier = {url for *_, url in sp._discovery_frontier} | {r.url for r in out}
    assert f"{SITE}/en/product/series/DF14" in frontier
    assert not [u for u in frontier if any(x in u for x in ("cart", "login", "news", "account"))]
    assert _stat(sp, "prism/discovery_targets") == 1


def test_a_target_page_is_parsed_and_its_links_followed():
    sp, refill = _spider()
    _, (listing, *_) = _start(sp, refill)
    (target,) = [r for r in sp._discovery_page(_html(listing, "/en/product/p/A"))
                 if r.callback == sp._discovery_parse]
    out = list(sp._discovery_parse(_html(target, "/en/product/p/B", "/en/product/p/A")))
    assert {"url": f"{SITE}/en/product/p/A"} in out
    assert [r.url for r in out if getattr(r, "callback", None) == sp._discovery_parse] == [
        f"{SITE}/en/product/p/B"]


def test_a_pattern_filter_matches_as_prism_does():
    class Panduit(Hirose):
        prism_path_prefix = None
        prism_pattern = r"/en/products/.+/p\d+\.html$"

    sp, refill = _spider(Panduit)
    assert sp._matches_filter(f"{SITE}/en/products/cable-ties/p123.html")
    assert not sp._matches_filter(f"{SITE}/en/products/cable-ties/")


@pytest.mark.parametrize("prefix, pattern", [
    (None, r"/p/[^/?#]+$"),                        # dwyer: "#" and "?" cut a raw query
    (None, r"/en/products/.+/p\d+\.html$"),       # panduit: "+" read as a space
    ("/en/product/p/", None),
    ("/a b&c=d/", r"x+y"),
])
def test_the_filter_reaches_prism_unchanged(prefix, pattern):
    class S(Hirose):
        prism_path_prefix = prefix
        prism_pattern = pattern

    sp, refill = _spider(S)
    assert sp._prism_filter() == (prefix, pattern)
    query = parse_qs(urlparse(refill.url).query)
    assert query.get("path_prefix", [None])[0] == prefix
    assert query.get("pattern", [None])[0] == pattern


def test_the_page_budget_holds_across_idle():
    sp, refill = _spider(PRISM_DISCOVERY_MAX_PAGES=3)
    _, requests = _start(sp, refill, [f"{SITE}/en/product/series/{n}" for n in range(10)])
    assert len(requests) == 3
    out = list(sp._discovery_page(_html(requests[0], *[f"/en/product/x{n}" for n in range(20)])))
    assert out == []                     # budget spent: links are kept, not fetched
    sp.crawler.engine = mock.Mock()
    with pytest.raises(CloseSpider):     # idle: nothing more to send, nothing found
        sp._on_idle()
    sp.crawler.engine.crawl.assert_not_called()
    assert _stat(sp, "prism/discovery_pages") == 3


def test_max_urls_caps_the_discovered_targets():
    sp = make_spider(Hirose, {"PRISM_URL": PRISM}, batch_size=5, max_urls=2)
    (refill,) = list(sp.start_requests())
    _, (listing, *_) = _start(sp, refill)
    out = list(sp._discovery_page(_html(listing, *[f"/en/product/p/{n}" for n in range(5)])))
    assert len([r for r in out if r.callback == sp._discovery_parse]) == 2


def test_idle_sends_the_frontier_and_keeps_the_spider_open():
    sp, refill = _spider()
    _, requests = _start(sp, refill)
    sp.crawler.engine = mock.Mock()
    sp._discovery_page(_html(requests[0], "/en/product/series/DF14"))  # never iterated:
    sp._discovery_inflight = len(requests)                              # requests "dropped"
    sp._discovery_frontier.clear()
    sp._discovery_push(f"{SITE}/en/product/series/DF14", 1)
    with pytest.raises(DontCloseSpider):
        sp._on_idle()
    (sent,), _ = sp.crawler.engine.crawl.call_args
    assert sent.url == f"{SITE}/en/product/series/DF14"


def test_discovery_finding_nothing_closes_as_prism_no_matching_urls():
    sp, refill = _spider()
    _start(sp, refill)
    sp._discovery_frontier.clear()
    sp.crawler.engine = mock.Mock()
    with pytest.raises(CloseSpider) as closed:
        sp._on_idle()
    assert closed.value.reason == "prism_no_matching_urls"
    assert _stat(sp, "prism/discovery_found_nothing") == 1


def test_discovery_that_found_pages_ends_quietly():
    sp, refill = _spider()
    _, (listing, *_) = _start(sp, refill)
    list(sp._discovery_page(_html(listing, "/en/product/p/A")))
    sp._discovery_frontier.clear()
    sp.crawler.engine = mock.Mock()
    assert sp._on_idle() is None
    assert sp._on_idle() is None  # and stays ended


def test_idle_without_discovery_does_nothing():
    sp, _ = _spider()
    assert sp._on_idle() is None


@pytest.mark.parametrize("path", [
    "/cart", "/cart/", "/en/Cart?x=1", "/checkout/step1", "/login", "/log-in", "/signin",
    "/sign-in/", "/account/orders", "/my-account", "/register", "/wishlist", "/auth/callback",
    "/en/news/2026", "/blog/", "/careers", "/en/tags/x",
])
def test_sign_in_cart_and_checkout_are_never_followed(path):
    assert _DISCOVERY_DENY.search(path)


@pytest.mark.parametrize("path", [
    "/", "/en/product/p/DF13", "/products/", "/category/rf-adapters", "/en/accessories/",
    "/cartridges/ink", "/newsletter-products/",
])
def test_catalogue_paths_are_followed(path):
    assert not _DISCOVERY_DENY.search(path)


# -- Prism URLs off the spider's domains -----------------------------------------------------

def test_a_listing_all_off_the_domain_closes_as_prism_urls_offsite():
    sp, refill = _spider(Anderson)
    moved = [f"https://idealelectricalinc.com/p/{n}" for n in range(3)]
    with pytest.raises(CloseSpider) as closed:
        list(sp._handle_prism_page(_page(refill, moved)))
    assert closed.value.reason == "prism_urls_offsite"
    assert _stat(sp, "prism/urls_offsite") == 3


def test_offsite_urls_are_dropped_and_onsite_ones_crawled():
    sp, refill = _spider(Anderson)
    out = list(sp._handle_prism_page(_page(refill, [
        "https://idealelectricalinc.com/p/1", "https://www.andersonpower.com/p/2",
        "https://andersonpower.com/p/3"])))
    assert [r.url for r in out] == ["https://www.andersonpower.com/p/2",
                                    "https://andersonpower.com/p/3"]
    assert _stat(sp, "prism/urls_offsite") == 1


def test_a_spider_without_allowed_domains_keeps_every_url():
    sp, refill = _spider(Unfiltered)
    out = list(sp._handle_prism_page(_page(refill, ["https://elsewhere.example/p/1"])))
    assert [r.url for r in out] == ["https://elsewhere.example/p/1"]


def test_placeholder_hosts_read_as_the_prism_domain():
    class Mersen(Anderson):
        prism_domain = "us.mersen.com"
        allowed_domains = ["us.mersen.com", "mersen.com"]

    sp, refill = _spider(Mersen)
    out = list(sp._handle_prism_page(_page(refill, ["http://default/de", "http://default/en/p?x=1"])))
    assert [r.url for r in out] == ["https://us.mersen.com/de", "https://us.mersen.com/en/p?x=1"]


@pytest.mark.parametrize("url", [
    "https://www.ti.com/product/X", "http://127.0.0.1:8000/p", "http://[::1]/p", "not a url",
])
def test_real_hosts_are_left_alone(url):
    assert _placeholder_host_fixed(url, "us.mersen.com") == url
