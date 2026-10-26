"""The local route sends the cookies a site set, as Scrapy's cookie middleware would.

MimicBrowserMiddleware (200) answers a local request before CookiesMiddleware (700) sees it, so its
process_request never put the jar's cookies on the request: an ASP.NET_SessionId or a viewstate session
a register set on the search page never came back on the next request, and spiders had to carry
Set-Cookie by hand. Its process_response still runs, so the jar fills; the local route now reads it.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from scrapy import Spider
from scrapy.downloadermiddlewares.cookies import CookiesMiddleware
from scrapy.http import Request
from scrapy.settings import Settings

from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware, _default_pool_size
from scrapy_calyprium.routing.auto import RouteResult
from scrapy_calyprium.routing.local_fetch import LocalFetchResult, _set_cookie_values, join_cookie_headers

from tests.test_middleware_local_integration import FakeRouter, _make_middleware

URL = "https://register.example.gov/search"
SPIDER = Spider(name="t")


def _page(set_cookies=()):
    headers = {"content-type": "text/html"}
    if set_cookies:
        headers["set-cookie"] = ", ".join(set_cookies)  # how the backends fold the header
    return RouteResult(
        fetch=LocalFetchResult(url=URL, final_url=URL, status_code=200, headers=headers,
                               body=b"<html><body><main>" + b"<p>row</p>" * 50 + b"</main></body></html>",
                               backend="httpcloak", set_cookies=list(set_cookies)),
        routing_method="httpcloak_light", blocked=False, domain_level="light",
    )


def _crawl(settings=None):
    """A middleware wired to a crawler whose downloader holds Scrapy's CookiesMiddleware."""
    router = FakeRouter()
    mw = _make_middleware(local_router=router)
    jar = CookiesMiddleware()
    mw.crawler = SimpleNamespace(
        settings=Settings(settings or {}),
        engine=SimpleNamespace(downloader=SimpleNamespace(middleware=SimpleNamespace(middlewares=[jar]))),
    )
    return mw, router, jar


async def _get(mw, router, jar, request, page):
    router.queue(page)
    response = await mw._try_local_route(request, spider=SPIDER)
    jar.process_response(request, response, SPIDER)  # Scrapy runs it on every response
    return response


@pytest.mark.asyncio
async def test_every_set_cookie_reaches_scrapy_and_comes_back_on_the_next_request():
    mw, router, jar = _crawl()
    response = await _get(mw, router, jar, Request(URL),
                          _page(["ASP.NET_SessionId=abc; path=/; HttpOnly",
                                 "pref=1; expires=Wed, 21 Oct 2037 07:28:00 GMT; path=/"]))
    assert response.headers.getlist("Set-Cookie") == [
        b"ASP.NET_SessionId=abc; path=/; HttpOnly", b"pref=1; expires=Wed, 21 Oct 2037 07:28:00 GMT; path=/"]

    await _get(mw, router, jar, Request(URL + "?page=2"), _page())
    sent = router.headers[1]["Cookie"]
    assert sorted(c.strip() for c in sent.split(";")) == ["ASP.NET_SessionId=abc", "pref=1"]


@pytest.mark.asyncio
async def test_a_cookie_the_spider_set_by_hand_wins_over_the_jar():
    mw, router, jar = _crawl()
    await _get(mw, router, jar, Request(URL), _page(["sid=jar; path=/", "pref=1; path=/"]))
    await _get(mw, router, jar, Request(URL, headers={"Cookie": "sid=mine"}), _page())
    assert router.headers[1]["Cookie"] == "sid=mine; pref=1"


@pytest.mark.asyncio
async def test_request_cookies_argument_is_sent():
    mw, router, jar = _crawl()
    await _get(mw, router, jar, Request(URL, cookies={"lang": "en"}), _page())
    assert router.headers[0]["Cookie"] == "lang=en"


@pytest.mark.asyncio
async def test_no_jar_cookies_when_asked_not_to():
    mw, router, jar = _crawl()
    await _get(mw, router, jar, Request(URL), _page(["sid=abc; path=/"]))
    await _get(mw, router, jar, Request(URL, meta={"dont_merge_cookies": True}), _page())
    assert "Cookie" not in router.headers[1]

    off, router2, jar2 = _crawl({"MIMIC_LOCAL_COOKIE_JAR": False})
    await _get(off, router2, jar2, Request(URL), _page(["sid=abc; path=/"]))
    await _get(off, router2, jar2, Request(URL), _page())
    assert "Cookie" not in router2.headers[1]


@pytest.mark.asyncio
async def test_without_a_cookie_middleware_nothing_changes():
    router = FakeRouter()
    mw = _make_middleware(local_router=router)  # no crawler at all
    router.queue(_page())
    await mw._try_local_route(Request(URL, headers={"Cookie": "a=1"}), spider=SPIDER)
    assert router.headers[0]["Cookie"] == "a=1"


def test_set_cookie_values_from_each_backend():
    # httpcloak: a list per header name
    assert _set_cookie_values({"Set-Cookie": ["a=1; path=/", "b=2"], "Server": ["x"]}) == ["a=1; path=/", "b=2"]

    class MultiDict:  # curl_cffi's Headers
        def get_list(self, name):
            return ["a=1", "b=2"] if name == "set-cookie" else []

    assert _set_cookie_values(MultiDict()) == ["a=1", "b=2"]
    assert _set_cookie_values({"set-cookie": "a=1"}) == ["a=1"]
    assert _set_cookie_values({}) == []


def test_join_cookie_headers():
    assert join_cookie_headers("sid=mine", "sid=jar; pref=1") == "sid=mine; pref=1"
    assert join_cookie_headers("", "pref=1") == "pref=1"


def test_default_pool_follows_the_spiders_concurrency():
    assert _default_pool_size(Settings()) == 8  # Scrapy's 8 per domain: the legacy pool
    assert _default_pool_size(Settings({"CONCURRENT_REQUESTS_PER_DOMAIN": 1})) == 2
    assert _default_pool_size(Settings({"CONCURRENT_REQUESTS_PER_DOMAIN": 2})) == 4
    assert _default_pool_size(Settings({"CONCURRENT_REQUESTS": 1})) == 2
    assert _default_pool_size(Settings({"CONCURRENT_REQUESTS_PER_DOMAIN": 32})) == 8
