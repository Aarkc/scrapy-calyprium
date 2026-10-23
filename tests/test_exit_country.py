"""A spider pinned to a country exits through it on every hop.

The local path built the gateway username's country only from VEIL_COUNTRY, so a spider pinned
with MIMIC_PROXY_COUNTRY fetched, solved and replayed from anywhere; and a replay of a Mimic-solved
slot named the slot's provider and session but not the country the solve used (SolveResponse's
``country`` was never read), so the clearance met an IP of another country.
"""
from __future__ import annotations

import base64
from unittest import mock

import pytest
from scrapy.http import Request

from scrapy_calyprium._policy import POLICY_SETTINGS, RunPolicy
from scrapy_calyprium._veil import resolve_local_proxy_url
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.routing.domain_cache import CookieSlot, DomainCache
from scrapy_calyprium.routing.local_fetch import _inject_proxy_session
from scrapy_calyprium.routing.solve_client import SolveClient
from tests._helpers import make_crawler
from tests.test_local_request_forwarding import (  # noqa: F401 -- the httpcloak fixture
    CHALLENGE, FakeResponse, httpcloak,
)

BASE = {"CALYPRIUM_API_KEY": "clp_k", "VEIL_GATEWAY_URL": "http://gw:8080"}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in POLICY_SETTINGS:
        monkeypatch.delenv(key, raising=False)


def _countries(proxy_url: str):
    user = proxy_url.split("://", 1)[1].split(":", 1)[0]
    return [p for p in user.split("-")[1:] if p.startswith(("country_", "c_"))]


@pytest.mark.parametrize("settings,expected", [
    ({"MIMIC_PROXY_COUNTRY": "US"}, "us"),
    ({"VEIL_COUNTRY": "gb"}, "gb"),
    ({"VEIL_COUNTRY": "gb", "MIMIC_PROXY_COUNTRY": "us"}, "gb"),
    ({}, None),
])
def test_one_country_setting(settings, expected):
    assert RunPolicy.from_settings(make_crawler(settings).settings).country == expected


def test_mimic_proxy_country_pins_the_local_and_veil_paths():
    settings = {**BASE, "MIMIC_PROXY_COUNTRY": "us", "VEIL_USER_ID": "user-1"}
    assert resolve_local_proxy_url(make_crawler(settings).settings) == (
        "http://calyprium-country_us:clp_k@gw:8080")
    veil = VeilProxyMiddleware.from_crawler(make_crawler(settings))
    req = Request("https://example.com")
    veil.process_request(req, None)
    assert base64.b64decode(req.headers["Proxy-Authorization"][6:]).decode() == (
        "user-1-country_us:clp_k")


@pytest.mark.asyncio
async def test_veil_country_reaches_mimic_fetch_and_session():
    mw = MimicBrowserMiddleware.from_crawler(make_crawler({
        "MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "clp_k", "VEIL_COUNTRY": "us"}))
    assert mw._fetch_policy_fields()["proxy_country"] == "us"
    resp = mock.Mock()
    resp.json.return_value = {"session_id": "s1"}
    resp.raise_for_status = mock.Mock()
    client = mock.AsyncMock()
    client.post.return_value = resp
    mw._client = client
    client.is_closed = False
    await mw._create_session(None)
    assert client.post.call_args.kwargs["json"]["proxy_country"] == "us"


@pytest.mark.asyncio
async def test_solve_client_keeps_the_solves_country():
    client = SolveClient("http://mimic")
    resp = mock.Mock(status_code=200)
    resp.json.return_value = {"success": True, "cookies": [], "provider": "iproyal_static",
                              "proxy_session_id": "s1", "country": "US"}
    fake = mock.AsyncMock()
    fake.post.return_value = resp
    client._client = fake
    result = await client.solve(domain="example.com")
    assert result.country == "us"
    resp.json.return_value = {"success": True, "cookies": [], "country": "us-x:@"}
    assert (await client.solve(domain="example.com")).country is None  # not a username param


def test_replay_username_names_the_solves_country_once():
    base = "http://calyprium-spider_42-country_gb:clp_k@gw:8080"
    url = _inject_proxy_session(base, "s1", provider="iproyal_static", country="us")
    assert url == "http://calyprium-spider_42-p_iproyal_static-country_us-session_s1:clp_k@gw:8080"
    # no solve country: the run's stays
    assert _inject_proxy_session(base, "s1", provider="evomi") == (
        "http://calyprium-spider_42-country_gb-p_evomi-session_s1:clp_k@gw:8080")


def test_slot_keeps_its_provider_and_country():
    cache = DomainCache()
    slot = cache.set_cookies_from_solve(domain="example.com", cookies=[], user_agent="UA",
                                        proxy_session_id="s1", provider="iproyal_static",
                                        country="us")
    again = CookieSlot.from_dict(slot.to_dict())
    assert (again.provider, again.country) == ("iproyal_static", "us")


@pytest.mark.asyncio
async def test_a_pinned_spider_exits_us_on_every_hop(httpcloak):
    """MIMIC_PROXY_COUNTRY=us: the light fetch, the solve and the replay all leave through the US."""
    mw = MimicBrowserMiddleware.from_crawler(make_crawler({
        **BASE, "MIMIC_SERVICE_URL": "http://mimic", "MIMIC_LOCAL_FETCH": True,
        "MIMIC_SLOT_STATS_INTERVAL": 0, "MIMIC_LOCAL_POOL_SIZE": 0, "MIMIC_PROXY_COUNTRY": "us"}))
    mw._local_enabled = True
    mw._init_local_router(spider=None)
    resp = mock.Mock(status_code=200)
    resp.json.return_value = {
        "success": True, "cookies": [{"name": "cf_clearance", "value": "clear"}],
        "user_agent": "Slot-UA", "proxy_session_id": "s1", "provider": "iproyal_static",
        "country": "us", "preset": "chrome-143"}
    fake = mock.AsyncMock()
    fake.post.return_value = resp
    mw._solve_client._client = fake
    httpcloak.responses = [FakeResponse(403, CHALLENGE), FakeResponse()]

    response = await mw._try_local_route(Request("https://www.example.com/p/1"), None)

    assert response is not None and response.status == 200
    assert fake.post.call_args.kwargs["json"]["country"] == "us"
    light, replay = httpcloak.calls
    assert _countries(light["proxy"]) == ["country_us"]
    assert _countries(replay["proxy"]) == ["country_us"]
    assert "-p_iproyal_static-country_us-session_s1:" in replay["proxy"]


@pytest.mark.asyncio
async def test_a_replay_keeps_the_country_mimic_solved_in(httpcloak):
    """An unpinned run: Mimic solves through its own exit (BROWSER_SOLVE_COUNTRY) and the replay
    must leave through that country, or the clearance meets another IP."""
    mw = MimicBrowserMiddleware.from_crawler(make_crawler({
        **BASE, "MIMIC_SERVICE_URL": "http://mimic", "MIMIC_LOCAL_FETCH": True,
        "MIMIC_SLOT_STATS_INTERVAL": 0, "MIMIC_LOCAL_POOL_SIZE": 0}))
    mw._local_enabled = True
    mw._init_local_router(spider=None)
    resp = mock.Mock(status_code=200)
    resp.json.return_value = {
        "success": True, "cookies": [{"name": "cf_clearance", "value": "clear"}],
        "user_agent": "Slot-UA", "proxy_session_id": "s9", "provider": "iproyal_static",
        "country": "dk"}
    fake = mock.AsyncMock()
    fake.post.return_value = resp
    mw._solve_client._client = fake
    httpcloak.responses = [FakeResponse(403, CHALLENGE), FakeResponse(), FakeResponse(403, CHALLENGE),
                           FakeResponse()]

    await mw._try_local_route(Request("https://www.example.com/p/1"), None)
    await mw._try_local_route(Request("https://www.example.com/p/2"), None)  # cookie replay

    light, replay, light2, cookie_replay = httpcloak.calls
    assert _countries(light["proxy"]) == []
    assert _countries(replay["proxy"]) == ["country_dk"]
    assert _countries(cookie_replay["proxy"]) == ["country_dk"]
    assert "-session_s9:" in cookie_replay["proxy"]
