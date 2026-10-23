"""The local fetch path sends the request the spider made, not just its URL.

With MIMIC_LOCAL_FETCH the router fetched every request as a bare GET of its URL: headers a
spider set on its Request (X-Api-Key, ``Authorization: SEARS``, Accept-Version, its own Referer,
a JSON Accept) never reached the site, and five spiders had to opt out with ``mimic_local_skip``
(metrocuadrado, heremaps, sears, reverb, asda). The fetcher keeps its browser identity: the
cookie slot's User-Agent, the preset's client hints and codings, and no proxy credentials.
"""
from __future__ import annotations

import importlib
import sys
import types
from typing import Any, Dict, List

import pytest
from scrapy.http import Request

from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.spectre import SpectreMiddleware
from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.routing.auto import SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.solve_client import SolveResult
from tests._helpers import make_crawler

PROXY = "http://calyprium:clp_k@gw:8080"
OK_HTML = (b"<!DOCTYPE html><html><head><title>Real Page</title></head><body><main>"
           + b"<a href='/x'>link</a>" * 10 + b"</main></body></html>")
CHALLENGE = (b"<html><head><title>Just a moment...</title></head>"
             b"<body>Checking your browser before accessing.</body></html>")
SPIDER_HEADERS = {
    "X-Api-Key": "k1",
    "Authorization": "SEARS",
    "Accept-Version": "3.0",
    "Referer": "https://wego.here.com/",
    "Accept": "application/json",
}
FP = {
    "fingerprint": {"id": "fp1", "name": "chrome-153"},
    "headers": {
        "User-Agent": "Spectre-UA",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "de-DE,de;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "sec-ch-ua": '"Chromium";v="153"',
        "Referer": "https://www.google.com/",
    },
}


class FakeResponse:
    def __init__(self, status_code=200, content=OK_HTML, headers=None, url="https://x/"):
        self.status_code = status_code
        self.content = content
        self.headers = headers if headers is not None else {"content-type": "text/html"}
        self.url = url


@pytest.fixture
def httpcloak(monkeypatch):
    """A fake httpcloak recording every call; local_fetch reloaded to pick it up."""
    module = types.ModuleType("httpcloak")

    class Session:
        calls: List[Dict[str, Any]] = []
        responses: List[FakeResponse] = []

        def __init__(self, preset=None, proxy=None, timeout=None):
            self.init = {"preset": preset, "proxy": proxy, "timeout": timeout}

        def _answer(self, call):
            Session.calls.append({**self.init, **call})
            return Session.responses.pop(0) if Session.responses else FakeResponse()

        def get(self, url, headers=None):
            return self._answer({"method": "GET", "url": url, "headers": headers, "body": None,
                                 "via": "get"})

        def request(self, method, url, data=None, headers=None):
            return self._answer({"method": method, "url": url, "headers": headers, "body": data,
                                 "via": "request"})

        def close(self):
            pass

    Session.calls = []
    Session.responses = []
    module.Session = Session
    monkeypatch.setitem(sys.modules, "httpcloak", module)
    import scrapy_calyprium.routing.local_fetch as lf
    importlib.reload(lf)
    yield Session
    monkeypatch.delitem(sys.modules, "httpcloak")
    importlib.reload(lf)


class FakeSolve:
    def __init__(self, *results):
        self.results = list(results)
        self.calls: List[Dict] = []

    async def solve(self, **kwargs):
        self.calls.append(kwargs)
        return self.results.pop(0)


def _solved(**overrides):
    fields = dict(success=True, cookies=[{"name": "cf_clearance", "value": "clear"}],
                  user_agent="Slot-UA", proxy_session_id="sess1", engine="camoufox",
                  preset="chrome-143", duration_ms=1000, provider="iproyal_static")
    fields.update(overrides)
    return SolveResult(**fields)


def _router(solve=None, cache=None):
    from scrapy_calyprium.routing.local_fetch import LocalFetcher
    return SpiderAutoRouter(
        fetcher=LocalFetcher(default_preset="chrome-143", timeout=30, backend="httpcloak"),
        cache=cache or DomainCache(), solve_client=solve or FakeSolve(), proxy_url=PROXY,
        target_pool_size=0)


def _stack(router):
    """Veil (100), Spectre (150) and Mimic (200), as the platform runs them."""
    crawler = make_crawler({
        "CALYPRIUM_API_KEY": "clp_k", "VEIL_USER_ID": "user-1",
        "VEIL_GATEWAY_URL": "http://gw:8080", "VEIL_PROFILE": "prof",
    })
    veil = VeilProxyMiddleware.from_crawler(crawler)
    spectre = SpectreMiddleware(service_url="http://spectre", api_key="k")
    spectre._cached_fingerprint = FP
    mimic = MimicBrowserMiddleware(service_url="http://mimic", api_key="clp_k")
    mimic._local_enabled = True
    mimic._local_router = router
    mimic._local_cache = router.cache

    async def download(request):
        veil.process_request(request, None)
        spectre.process_request(request, None)
        return await mimic.process_request(request, None)

    return download


def _sent(call) -> Dict[str, str]:
    return {k.lower(): v for k, v in (call["headers"] or {}).items()}


def _assert_spider_headers(sent):
    for name, value in SPIDER_HEADERS.items():
        assert sent[name.lower()] == value, name
    # never the gateway's credentials, never the fingerprint's identity or locale
    for name in ("proxy-authorization", "x-veil-profile", "sec-ch-ua", "accept-language"):
        assert name not in sent, name


@pytest.mark.asyncio
async def test_spider_headers_reach_httpcloak_on_the_light_path(httpcloak):
    download = _stack(_router())
    response = await download(Request("https://api.example.com/v1/items", headers=SPIDER_HEADERS))
    assert response.status == 200
    (call,) = httpcloak.calls
    sent = _sent(call)
    _assert_spider_headers(sent)
    assert "user-agent" not in sent  # the preset's own


@pytest.mark.asyncio
async def test_spider_headers_ride_on_a_cookie_replay(httpcloak):
    cache = DomainCache()
    cache.set_cookies_from_solve(domain="api.example.com", cookies=[{"name": "cf_clearance",
                                 "value": "clear"}], user_agent="Slot-UA", proxy_session_id="s1",
                                 preset="chrome-143", provider="iproyal_static")
    httpcloak.responses = [FakeResponse(403, CHALLENGE), FakeResponse()]
    download = _stack(_router(cache=cache))
    response = await download(Request("https://api.example.com/v1/items", headers=SPIDER_HEADERS))
    assert response.status == 200
    light, replay = httpcloak.calls
    _assert_spider_headers(_sent(light))
    sent = _sent(replay)
    _assert_spider_headers(sent)
    assert sent["user-agent"] == "Slot-UA"  # the clearance's UA, not Spectre's
    assert sent["cookie"] == "cf_clearance=clear"
    assert "-p_iproyal_static-session_s1:" in replay["proxy"]


@pytest.mark.asyncio
async def test_spider_headers_ride_on_the_replay_after_a_solve(httpcloak):
    httpcloak.responses = [FakeResponse(403, CHALLENGE), FakeResponse()]
    solve = FakeSolve(_solved())
    download = _stack(_router(solve=solve))
    response = await download(Request("https://api.example.com/v1/items", headers=SPIDER_HEADERS))
    assert response.status == 200
    assert len(solve.calls) == 1
    light, replay = httpcloak.calls
    _assert_spider_headers(_sent(replay))
    assert _sent(replay)["user-agent"] == "Slot-UA"


@pytest.mark.asyncio
async def test_a_plain_get_sends_no_headers_of_its_own(httpcloak):
    """Nothing the crawl added (fingerprint, Scrapy's Referer, proxy auth) goes out locally."""
    download = _stack(_router())
    request = Request("https://www.example.com/product/1")
    request.meta["calyprium_scrapy_referer"] = "https://www.example.com/category"
    request.headers["Referer"] = "https://www.example.com/category"  # Scrapy's RefererMiddleware
    await download(request)
    (call,) = httpcloak.calls
    assert call["headers"] is None and call["via"] == "get"


@pytest.mark.asyncio
async def test_spider_headers_cannot_replace_the_fetchers_identity(httpcloak):
    from scrapy_calyprium.routing.local_fetch import LocalFetcher
    fetcher = LocalFetcher(default_preset="chrome-143", timeout=30, backend="httpcloak")
    await fetcher.fetch(
        "https://example.com/", cookies=[{"name": "cf_clearance", "value": "clear"}],
        user_agent="Slot-UA", extra_headers={
            "User-Agent": "MyBot/1.0", "Accept-Encoding": "br", "sec-ch-ua-platform": '"Linux"',
            "Connection": "close", "Host": "evil.example", "Content-Length": "5",
            "Proxy-Authorization": "Basic eA==", "X-Veil-Profile": "p",
            "Cookie": "cf_clearance=stale; lang=de", "X-Api-Key": "k1"})
    sent = _sent(httpcloak.calls[0])
    assert sent == {
        "user-agent": "Slot-UA", "accept-encoding": "identity", "x-api-key": "k1",
        "cookie": "cf_clearance=clear; lang=de",  # the slot's clearance wins its name
    }


@pytest.mark.asyncio
async def test_curl_cffi_sends_the_spider_headers_and_one_cookie_jar(monkeypatch):
    import scrapy_calyprium.routing.local_fetch as lf
    calls = []

    class Response:
        status_code = 200
        content = OK_HTML
        headers = {"content-type": "text/html"}
        url = "https://example.com/"

    class AsyncSession:
        def __init__(self, impersonate=None):
            self.impersonate = impersonate

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, **kwargs):
            calls.append({"method": "GET", "url": url, **kwargs})
            return Response()

        async def request(self, method, url, **kwargs):
            calls.append({"method": method, "url": url, **kwargs})
            return Response()

    monkeypatch.setattr(lf, "_CURL_CFFI_ASYNC", AsyncSession)
    fetcher = lf.LocalFetcher(default_preset="firefox-135", timeout=30, backend="curl_cffi")
    await fetcher.fetch(
        "https://example.com/", preset="firefox-135", user_agent="Slot-UA",
        cookies=[{"name": "cf_clearance", "value": "clear"}],
        extra_headers={**SPIDER_HEADERS, "User-Agent": "MyBot/1.0", "Cookie": "lang=de"})
    (call,) = calls
    headers = {k.lower(): v for k, v in call["headers"].items()}
    assert headers["user-agent"] == "Slot-UA"
    assert headers["x-api-key"] == "k1" and headers["accept"] == "application/json"
    assert "cookie" not in headers  # curl_cffi would send it beside its cookie jar
    assert call["cookies"] == {"cf_clearance": "clear", "lang": "de"}


# ---------------------------------------------------------------------------
# A POST (a storefront's read-only GraphQL or search API) goes the same way.
# Express, Home Depot, Levi's, O'Reilly, Sigma-Aldrich and Tokopedia serve product data only from
# a POST API behind Akamai or Cloudflare: the local route fetched its URL with a GET, so the
# POST was never sent, solved or replayed with the domain's cookies.
# ---------------------------------------------------------------------------

GRAPHQL = "https://www.example.com/graphql"
QUERY = b'{"operationName":"Product","variables":{"id":"\xc3\xa9-1"},"query":"{ product { name } }"}'


def _graphql_post():
    from scrapy.http import JsonRequest
    request = JsonRequest(GRAPHQL, method="POST", headers={"X-Api-Key": "k1"})
    request = request.replace(body=QUERY)
    return request


def _assert_the_post(call):
    assert call["via"] == "request" and call["method"] == "POST"
    assert call["url"] == GRAPHQL
    assert call["body"] == QUERY
    sent = _sent(call)
    assert sent["content-type"] == "application/json"
    assert sent["x-api-key"] == "k1"


@pytest.mark.asyncio
async def test_a_post_goes_through_the_light_path(httpcloak):
    download = _stack(_router())
    response = await download(_graphql_post())
    assert response.status == 200
    (call,) = httpcloak.calls
    _assert_the_post(call)


@pytest.mark.asyncio
async def test_a_post_is_replayed_on_a_solved_slot(httpcloak):
    """The solve loads the site's homepage (a GET of an API path is no page); the POST is then
    replayed with the slot's cookies, User-Agent and preset."""
    httpcloak.responses = [FakeResponse(403, CHALLENGE), FakeResponse()]
    solve = FakeSolve(_solved(preset="chrome-146"))
    download = _stack(_router(solve=solve))
    response = await download(_graphql_post())
    assert response.status == 200
    assert solve.calls[0]["target_url"] == "https://www.example.com/"
    light, replay = httpcloak.calls
    _assert_the_post(light)
    _assert_the_post(replay)
    sent = _sent(replay)
    assert sent["user-agent"] == "Slot-UA" and sent["cookie"] == "cf_clearance=clear"
    assert replay["preset"] == "chrome-146"
    assert "-p_iproyal_static-session_sess1:" in replay["proxy"]


@pytest.mark.asyncio
async def test_a_post_is_replayed_on_a_cookie_slot(httpcloak):
    cache = DomainCache()
    cache.set_cookies_from_solve(domain="www.example.com", cookies=[{"name": "_abck",
                                 "value": "a"}], user_agent="Slot-UA", proxy_session_id="s1",
                                 preset="chrome-143")
    httpcloak.responses = [FakeResponse(403, CHALLENGE), FakeResponse()]
    download = _stack(_router(cache=cache))
    await download(_graphql_post())
    light, replay = httpcloak.calls
    _assert_the_post(replay)
    assert _sent(replay)["cookie"] == "_abck=a"


@pytest.mark.asyncio
async def test_post_body_and_headers_reach_httpcloak_unchanged(httpcloak):
    from scrapy_calyprium.routing.local_fetch import LocalFetcher
    body = b"\x00\xff" + QUERY  # bytes as made, never decoded
    headers = {"Content-Type": "application/json", "Accept": "application/json",
               "Origin": "https://www.example.com", "Referer": "https://www.example.com/p/1"}
    fetcher = LocalFetcher(default_preset="chrome-143", timeout=30, backend="httpcloak")
    await fetcher.fetch(GRAPHQL, method="post", body=body, extra_headers=headers)
    (call,) = httpcloak.calls
    assert call["method"] == "POST" and call["body"] == body
    assert call["headers"] == headers


def test_a_form_post_solves_on_its_own_page():
    from scrapy_calyprium.routing.auto import _solve_url
    form = {"Content-Type": "application/x-www-form-urlencoded"}
    assert _solve_url("https://shop.example/search", "POST", form) == "https://shop.example/search"
    assert _solve_url("https://shop.example/p/1?x=1", "GET", {}) == "https://shop.example/p/1?x=1"
    for url, headers in [
        ("https://shop.example/api/products", form),
        ("https://shop.example/federation-gateway/graphql?opname=x", {}),
        ("https://gql.shop.example/graphql/PDP", {}),
        ("https://shop.example/nextgen-webhooks/", {"content-type": "application/json"}),
    ]:
        host = url.split("/")[2]
        assert _solve_url(url, "POST", headers) == f"https://{host}/", url


@pytest.mark.asyncio
async def test_curl_cffi_sends_the_post(monkeypatch):
    import scrapy_calyprium.routing.local_fetch as lf
    calls = []

    class Response:
        status_code = 200
        content = OK_HTML
        headers = {"content-type": "application/json"}
        url = GRAPHQL

    class AsyncSession:
        def __init__(self, impersonate=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def get(self, url, **kwargs):
            calls.append({"method": "GET", "url": url, **kwargs})
            return Response()

        async def request(self, method, url, **kwargs):
            calls.append({"method": method, "url": url, **kwargs})
            return Response()

    monkeypatch.setattr(lf, "_CURL_CFFI_ASYNC", AsyncSession)
    fetcher = lf.LocalFetcher(default_preset="firefox-135", timeout=30, backend="curl_cffi")
    await fetcher.fetch(GRAPHQL, preset="firefox-135", method="POST", body=QUERY,
                        extra_headers={"Content-Type": "application/json"})
    (call,) = calls
    assert call["method"] == "POST" and call["data"] == QUERY
    assert call["headers"]["Content-Type"] == "application/json"
