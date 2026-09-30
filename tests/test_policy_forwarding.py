"""AAR-62: profile policy settings reach Veil and Mimic.

Forge's build_run_settings emits SPIDER_ID, MIMIC_ALLOW_PAID_SOLVE,
MIMIC_PAID_SOLVE_MODE, MIMIC_ALLOWED_ENGINES, VEIL_ALLOWED_PROVIDERS and
VEIL_COUNTRY; before this fix the SDK read none of them, so per-spider budgets,
paid-solve "off" and allow-lists were never enforced and Veil spend was never
attributed.
"""
from __future__ import annotations

import base64
import re
from pathlib import Path
from unittest import mock

import pytest
from scrapy.http import Request

from scrapy_calyprium._policy import POLICY_SETTINGS, RunPolicy
from scrapy_calyprium._veil import resolve_local_proxy_url
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from scrapy_calyprium.routing.solve_client import SolveClient
from tests._helpers import make_crawler

POLICY = {
    "SPIDER_ID": "42",
    "MIMIC_ALLOW_PAID_SOLVE": "False",
    "MIMIC_PAID_SOLVE_MODE": "off",
    "MIMIC_ALLOWED_ENGINES": "camoufox,nodriver",
    "VEIL_ALLOWED_PROVIDERS": "webshare_static,evomi",
    "VEIL_COUNTRY": "US",
}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in POLICY_SETTINGS:
        monkeypatch.delenv(key, raising=False)


def test_run_policy_parses_forge_settings():
    p = RunPolicy.from_settings(make_crawler(POLICY).settings)
    assert p.spider_id == "42"
    assert p.allow_paid_solve is False
    assert p.paid_solve_mode == "off"
    assert p.allowed_engines == ["camoufox", "nodriver"]
    assert p.allowed_providers == ["webshare_static", "evomi"]
    assert p.country == "us"
    assert p.paid_solve_denied


def test_unset_policy_is_a_noop():
    p = RunPolicy.from_settings(make_crawler({}).settings)
    assert p.veil_username("u") == "u"
    assert p.mimic_fetch_fields() == {}
    assert p.solve_fields() == {}
    assert not p.paid_solve_denied


def test_veil_middleware_encodes_policy_in_username():
    crawler = make_crawler({
        **POLICY, "CALYPRIUM_API_KEY": "clp_k", "VEIL_USER_ID": "user-1",
        "VEIL_GATEWAY_URL": "http://gw:8080", "VEIL_PROVIDER": "webshare_rotating",
    })
    mw = VeilProxyMiddleware.from_crawler(crawler)
    req = Request("https://example.com")
    mw.process_request(req, None)
    creds = base64.b64decode(req.headers["Proxy-Authorization"][6:]).decode()
    assert creds == (
        "user-1-p_webshare_rotating-spider_42-ap_webshare_static.evomi"
        "-country_us:clp_k"
    )


def test_local_proxy_url_carries_policy_params():
    crawler = make_crawler({
        **POLICY, "CALYPRIUM_API_KEY": "clp_k", "VEIL_GATEWAY_URL": "http://gw:8080",
    })
    assert resolve_local_proxy_url(crawler.settings) == (
        "http://calyprium-spider_42-ap_webshare_static.evomi-country_us:clp_k@gw:8080"
    )


def test_local_proxy_session_injection_keeps_policy_params():
    from scrapy_calyprium.routing.local_fetch import _inject_proxy_session

    base = resolve_local_proxy_url(make_crawler({
        "SPIDER_ID": "42", "CALYPRIUM_API_KEY": "clp_k", "VEIL_GATEWAY_URL": "http://gw:8080",
    }).settings)
    assert _inject_proxy_session(base, "abc", provider="evomi") == (
        "http://calyprium-spider_42-p_evomi-session_abc:clp_k@gw:8080"
    )


@pytest.mark.asyncio
async def test_solve_client_forwards_spider_id_country_and_clamps_engine():
    policy = RunPolicy.from_settings(make_crawler(POLICY).settings)
    client = SolveClient("http://tessera", api_key="clp_k", policy=policy)
    resp = mock.Mock(status_code=200)
    resp.json.return_value = {"success": True, "cookies": []}
    fake = mock.AsyncMock()
    fake.post.return_value = resp
    client._client = fake
    await client.solve(domain="example.com", engine_hint="playwright_chromium")
    body = fake.post.call_args.kwargs["json"]
    assert body["spider_id"] == "42"
    assert body["country"] == "us"
    assert body["engine_hint"] == "camoufox"  # clamped into the allow-list


@pytest.mark.asyncio
async def test_mimic_fetch_payload_carries_policy():
    mw = MimicBrowserMiddleware.from_crawler(make_crawler({
        **POLICY, "MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "clp_k",
    }))
    resp = mock.Mock()
    resp.json.return_value = {"html": "<html></html>", "status_code": 200}
    resp.raise_for_status = mock.Mock()
    client = mock.AsyncMock()
    client.post.return_value = resp
    await mw._fetch_auto(client, Request("https://example.com"))
    payload = client.post.call_args.kwargs["json"]
    assert payload["spider_id"] == "42"
    assert payload["allow_paid_solve"] is False
    assert payload["paid_solve_mode"] == "off"
    assert payload["allowed_engines"] == ["camoufox", "nodriver"]
    assert payload["proxy_country"] == "us"


@pytest.mark.parametrize("paid,expected", [("False", "mimic"), ("True", "tessera")])
def test_paid_solve_off_keeps_local_solves_off_tessera(paid, expected):
    settings = {
        "MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "clp_k",
        "TESSERA_SERVICE_URL": "http://tessera", "MIMIC_LOCAL_FETCH": True,
        "MIMIC_ALLOW_PAID_SOLVE": paid, "MIMIC_SLOT_STATS_INTERVAL": 0,
        "SPIDER_ID": "42",
    }
    mw = MimicBrowserMiddleware.from_crawler(make_crawler(settings))
    mw._local_enabled = True
    mw._init_local_router(spider=None)
    assert mw._solve_backend == expected
    assert mw._solve_client.service_url == f"http://{expected}"
    assert mw._solve_client.policy.spider_id == "42"


# ---------------------------------------------------------------------------
# Contract: every policy key the SDK reads is one forge actually emits. Runs
# against the monorepo checkout when it sits next to this repo (CI of the
# monorepo should run the mirror-image check); skipped otherwise.
# ---------------------------------------------------------------------------

_RUN_SETTINGS = (
    Path(__file__).resolve().parents[2]
    / "calyprium" / "forge" / "api" / "services" / "run_settings.py"
)


@pytest.mark.skipif(not _RUN_SETTINGS.exists(), reason="calyprium monorepo not checked out")
def test_policy_settings_match_forge_build_run_settings():
    emitted = set(re.findall(r'f?"([A-Z][A-Z0-9_]+)=', _RUN_SETTINGS.read_text()))
    assert set(POLICY_SETTINGS) <= emitted, set(POLICY_SETTINGS) - emitted
