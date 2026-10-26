"""A page's call to another host of its own site solves on that page.

businesssearch.ohiosos.gov's script calls businesssearchapi.ohiosos.gov (credentialed CORS GETs).
The API host blocks every request without the search page's Origin / Referer (a 403 page no solve
clears), and serves the zone's managed challenge to the rest; the clearance the search page earns
opens it (2026-10-26, measured on the platform: native solve of the search page, then the API
replayed with the page's Origin and Referer, 200 JSON). So the solve loads the calling page, and
the request is replayed as made.
"""
from __future__ import annotations

import asyncio
from typing import Dict, List

import pytest

from scrapy_calyprium.routing.auto import SpiderAutoRouter, _calling_page, _site, _solve_url
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import LocalFetchResult
from scrapy_calyprium.routing.solve_client import SolveResult

PAGE = "https://businesssearch.ohiosos.gov/"
API = "https://businesssearchapi.ohiosos.gov/NS_ACME_X"
XHR = {"Origin": "https://businesssearch.ohiosos.gov", "Referer": PAGE,
       "Accept": "application/json, text/javascript, */*; q=0.01"}


def test_the_registrable_domain_of_a_host():
    assert _site("businesssearchapi.ohiosos.gov") == "ohiosos.gov"
    assert _site("www.shop.co.uk") == "shop.co.uk"
    assert _site("a.b.example.com.") == "example.com"
    assert _site("localhost") == "localhost"


def test_a_page_calling_an_api_host_of_its_site_solves_on_that_page():
    assert _solve_url(API, "GET", XHR) == PAGE
    deep = {**XHR, "Referer": "https://businesssearch.ohiosos.gov/?=businessDetails/407276"}
    assert _solve_url(API, "GET", deep) == deep["Referer"]
    # header names in any case; the page is the origin's homepage when no Referer is on it
    assert _solve_url(API, "GET", {"origin": "https://businesssearch.ohiosos.gov"}) == PAGE
    other_referer = {**XHR, "Referer": "https://www.ohiosos.gov/businesses/"}
    assert _solve_url(API, "GET", other_referer) == PAGE
    # a POST from that page too, before the API rule would send it to the API host's homepage
    post = {**XHR, "Content-Type": "application/json"}
    assert _solve_url("https://api.shop.co.uk/graphql", "POST",
                      {**post, "Origin": "https://www.shop.co.uk",
                       "Referer": "https://www.shop.co.uk/p/1"}) == "https://www.shop.co.uk/p/1"


def test_no_calling_page_keeps_the_solve_where_it_was():
    url = "https://api.example.com/v1/items"
    for headers in [
        {},
        {"Referer": "https://www.example.com/"},             # no Origin: a navigation or Scrapy's own
        {"Origin": "https://wego.here.com", "Referer": "https://wego.here.com/"},  # another site
        {"Origin": "https://api.example.com"},                # the same host
        {"Origin": "null"},
        {"Origin": "chrome-extension://abc"},
        {"Origin": None},
    ]:
        assert _calling_page(url, headers) is None, headers
        assert _solve_url(url, "GET", headers) == url, headers
    # two sites under one public suffix are not one site
    assert _calling_page("https://a.co.uk/x", {"Origin": "https://b.co.uk"}) is None
    assert _calling_page("https://a.github.io/x", {"Origin": "https://b.github.io"}) is None
    # the API rule still holds for a same-host POST
    assert _solve_url("https://www.example.com/graphql", "POST",
                      {"Origin": "https://www.example.com",
                       "Content-Type": "application/json"}) == "https://www.example.com/"


# ---------------------------------------------------------------------------
# The route: solve on the page, replay the request as made, refill on the page
# ---------------------------------------------------------------------------

_CHALLENGE = (b"<html><head><title>Just a moment...</title></head>"
              b"<body>Checking your browser before accessing.</body></html>")


def _fetch(status: int, body: bytes, ctype: str = "text/html") -> LocalFetchResult:
    return LocalFetchResult(url=API, final_url=API, status_code=status,
                            headers={"content-type": ctype, "cf-mitigated": "challenge"} if status == 403
                            else {"content-type": ctype},
                            body=body, backend="httpcloak")


class FakeFetcher:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls: List[Dict] = []

    async def fetch(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        return self.responses.pop(0)


class FakeSolve:
    def __init__(self):
        self.calls: List[Dict] = []

    async def solve(self, **kwargs):
        self.calls.append(kwargs)
        n = len(self.calls)
        return SolveResult(success=True, cookies=[{"name": "cf_clearance", "value": f"c{n}"}],
                           user_agent="UA", proxy_session_id=f"s{n}", engine="tessera-native",
                           preset="chrome-155-windows", duration_ms=10, provider="evomi_rotating",
                           country="us")

    async def report_ip_outcome(self, **kwargs):
        pass


@pytest.mark.asyncio
async def test_the_api_request_solves_on_its_page_and_is_replayed_as_made():
    fetcher = FakeFetcher(_fetch(403, _CHALLENGE), _fetch(200, b'{"data": []}', "application/json"))
    solve = FakeSolve()
    router = SpiderAutoRouter(fetcher=fetcher, cache=DomainCache(), solve_client=solve,
                              proxy_url=None, target_pool_size=2, refill_interval=0.0,
                              solve_parallel_solves=1, cold_start_burst=1)
    result = await router.fetch(API, domain="businesssearchapi.ohiosos.gov", headers=XHR)
    assert not result.blocked and result.fetch.status_code == 200
    first = solve.calls[0]
    assert first["domain"] == "businesssearchapi.ohiosos.gov" and first["target_url"] == PAGE
    light, replay = fetcher.calls
    assert replay["url"] == API and replay["extra_headers"] == XHR  # the request as made
    assert replay["cookies"] == [{"name": "cf_clearance", "value": "c1"}]

    for _ in range(5):  # the cold-start refill, fired after the solve
        await asyncio.sleep(0)
    assert len(solve.calls) == 2
    assert solve.calls[1]["target_url"] == PAGE  # not the API host's homepage


@pytest.mark.asyncio
async def test_a_refill_of_a_domain_solved_on_its_own_pages_names_no_page():
    fetcher = FakeFetcher(_fetch(403, _CHALLENGE), _fetch(200, b"<html>" + b"<p>x</p>" * 40 + b"</html>"))
    solve = FakeSolve()
    router = SpiderAutoRouter(fetcher=fetcher, cache=DomainCache(), solve_client=solve,
                              proxy_url=None, target_pool_size=2, refill_interval=0.0,
                              solve_parallel_solves=1, cold_start_burst=1)
    url = "https://www.example.com/p/1"
    await router.fetch(url, domain="www.example.com")
    for _ in range(5):
        await asyncio.sleep(0)
    assert solve.calls[0]["target_url"] == url
    assert len(solve.calls) == 2 and "target_url" not in solve.calls[1]
