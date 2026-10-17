"""Solves name the vendor the block showed; Kasada's block is solvable; derived Chrome presets replay.

A live check on 2026-10-17 found every spider solve reaching Tessera as a Cloudflare solve (no
challenge type, Tessera's default), chewy's Kasada 429 read as a rate limit, and a native
solver's chrome-153-windows preset refused by the spider's httpcloak ("unknown preset").
"""
import json
import sys
import types
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest

from scrapy_calyprium.routing import chrome_preset
from scrapy_calyprium.routing.block_detect import challenge_family, is_blocked
from scrapy_calyprium.routing.chrome_preset import ensure_chrome_preset, replay_preset, sec_ch_ua
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import LocalFetchResult
from scrapy_calyprium.routing.solve_client import SolveClient, SolveResult

from tests.test_local_routing import FakeFetcher, FakeSolveClient, _make_router, _ok

# Kasada's blocking page (chewy.com, 429, 669 bytes): one script from Kasada's fixed path.
KASADA = (b'<html><head><script src="/149e9513-01fa-4fb0-aad4-566afd725d1b/2d206a39-8ed7-437e-a3be-'
          b'862e0f06eea3/ips.js?KP_UIDz=x&x-kpsdk-v=j-1.2.864"></script></head><body></body></html>')
KASADA_HEADERS = {"content-type": "text/html", "x-kpsdk-ct": "abc", "x-kpsdk-r": "1-0",
                  "set-cookie": "KP_UIDz=abc; Path=/"}
CF = b"<html><title>Just a moment...</title><script src='/cdn-cgi/challenge-platform/h/b/orchestrate'></script></html>"
DD = b"<html><script>var dd={'rt':'c','cid':'A','hsh':'B','t':'fe'}</script></html>"
SBSD = (b'<html><body><script type="text/javascript" '
        b'src="/a9Dq2dxJ/cbYL6Vu1uz/x?v=83615c8c-7d18-e5ce-d2b9-ee8938740214&amp;t=67893359"></script>'
        b'<div id="sec-if-cpt-container"></div></body></html>')
UTMVC = (b'<html><head><script src="/_Incapsula_Resource?SWJIYLWA=719d34d31c8e3a6e6fffd425f7e032f3"></script>'
         b'</head><body>Request unsuccessful. Incapsula incident ID: 0-1</body></html>')
INCIDENT = (b'<html><iframe src="/_Incapsula_Resource?CWUDNSAI=24&incident_id=0-1&edet=12"></iframe>'
            b'Request unsuccessful. Incapsula incident ID: 0-1</html>')
PX = b"<html><div id='px-captcha'></div><script>window._pxAppId='PXabc123';</script></html>"
DENY = b"<html><title>Access Denied</title>Reference #18.2b3c4d5e.1700000000.6f7a8b9c</html>"


class TestChallengeFamily:
    def test_each_vendor(self):
        assert challenge_family(403, CF) == "cloudflare_managed"
        assert challenge_family(403, b"x", {"cf-mitigated": "challenge"}) == "cloudflare_managed"
        assert challenge_family(403, DD) == "datadome"
        assert challenge_family(200, SBSD) == "akamai"
        assert challenge_family(429, KASADA, KASADA_HEADERS) == "kasada"
        assert challenge_family(403, UTMVC) == "incapsula"
        assert challenge_family(403, PX) == "perimeterx"
        assert challenge_family(403, DENY) == "akamai"
        assert challenge_family(403, b"nope", {"server": "AkamaiGHost"}) == "akamai"
        assert challenge_family(403, b"nope", {"set-cookie": "datadome=abc"}) == "datadome"

    def test_kasada_behind_akamais_cdn_is_kasada(self):
        assert challenge_family(429, KASADA, {**KASADA_HEADERS, "server": "AkamaiGHost"}) == "kasada"

    def test_nothing_to_name(self):
        assert challenge_family(403, INCIDENT) is None  # hard block: no ___utmvc script
        assert challenge_family(403, b"Forbidden", {"server": "nginx"}) is None
        assert challenge_family(200, KASADA) is None     # Kasada's SDK path on a served page
        assert challenge_family(200, b"<html><title>Parts</title></html>") is None

    def test_kasada_block_is_solvable(self):
        # Before: no "kasada" text, no known header or cookie -> read as a plain rate limit.
        assert is_blocked(429, KASADA, content_type="text/html", headers=KASADA_HEADERS) is True
        assert is_blocked(429, KASADA, content_type="text/html", headers={"content-type": "text/html"}) is True
        assert is_blocked(429, b"slow down", content_type="text/html", headers={"server": "nginx"}) is False


@pytest.mark.asyncio
async def test_solve_client_sends_the_challenge_type():
    client = SolveClient("http://tessera", api_key="clp_k")
    resp = mock.Mock(status_code=200)
    resp.json.return_value = {"success": True, "cookies": []}
    fake = mock.AsyncMock()
    fake.post.return_value = resp
    client._client = fake
    await client.solve(domain="www.chewy.com", challenge_type="kasada")
    assert fake.post.call_args.kwargs["json"]["challenge_type"] == "kasada"
    await client.solve(domain="example.com")
    assert "challenge_type" not in fake.post.call_args.kwargs["json"]  # Tessera's default stays


def _blocked(body, headers, status=429):
    return LocalFetchResult(url="https://www.chewy.com/", final_url="https://www.chewy.com/",
                            status_code=status, headers=headers, body=body, backend="httpcloak")


@pytest.mark.asyncio
async def test_router_names_the_vendor_in_its_solve():
    f = FakeFetcher()
    f.queue(_blocked(KASADA, KASADA_HEADERS), _ok())
    s = FakeSolveClient()
    s.queue(SolveResult(success=True, cookies=[{"name": "KP_UIDz", "value": "x"}], user_agent="UA/153",
                        proxy_session_id="sess-1", engine="native", preset="chrome-153-windows",
                        duration_ms=8000))
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())

    result = await router.fetch("https://www.chewy.com/", domain="www.chewy.com")

    assert result.blocked is False
    assert s.calls[0]["challenge_type"] == "kasada"
    assert f.calls[1]["preset"] == "chrome-153-windows"  # replayed on the solver's preset


@pytest.mark.asyncio
async def test_router_without_a_vendor_leaves_the_type_unset():
    f = FakeFetcher()
    f.queue(_blocked(b"<html>Access Denied</html>", {"content-type": "text/html"}, status=403), _ok())
    s = FakeSolveClient()
    s.queue(SolveResult(success=True, cookies=[{"name": "a", "value": "x"}], user_agent="UA",
                        proxy_session_id="sess-1", engine="camoufox", preset="chrome-latest",
                        duration_ms=8000))
    router = _make_router(fetcher=f, solve=s, cache=DomainCache())
    await router.fetch("https://example.com/", domain="example.com")
    assert s.calls[0]["challenge_type"] is None


# --- the derived Chrome preset ------------------------------------------------------------------

def _fake_httpcloak(monkeypatch):
    """Like httpcloak 1.7.2: a JSON preset is registered once per process and not listed by available_presets()."""
    registered = {}
    base = {"version": 1, "preset": {"name": "chrome-152-windows", "tls": {"client_hello": "x"},
            "headers": {"user_agent": "Mozilla/5.0 (Windows NT 10.0) Chrome/152.0.0.0 Safari/537.36",
                        "values": {"sec-ch-ua": sec_ch_ua(152)},
                        "order": [{"key": "sec-ch-ua", "value": sec_ch_ua(152)}, {"key": "user-agent", "value": ""}]}}}

    def load_preset_from_json(blob):
        name = json.loads(blob)["preset"]["name"]
        if name in registered:
            raise RuntimeError(f'preset name "{name}" already registered (call Unregister first to replace)')
        registered[name] = json.loads(blob)
        return name

    monkeypatch.setattr(chrome_preset, "_DERIVED", set())
    monkeypatch.setitem(sys.modules, "httpcloak", types.SimpleNamespace(
        available_presets=lambda: {"chrome-latest-windows": {}, "chrome-150-windows": {}, "chrome-152-windows": {}},
        describe_preset=lambda name: json.dumps(base),
        load_preset_from_json=load_preset_from_json,
    ))
    return registered


def test_sec_ch_ua_matches_real_chrome():
    assert sec_ch_ua(153) == '"Google Chrome";v="153", "Not_A Brand";v="8", "Chromium";v="153"'
    assert sec_ch_ua(152) == '"Chromium";v="152", "Not?A_Brand";v="24", "Google Chrome";v="152"'


def test_replay_derives_the_solvers_preset_once(monkeypatch):
    registered = _fake_httpcloak(monkeypatch)
    with ThreadPoolExecutor(8) as pool:
        names = list(pool.map(replay_preset, ["chrome-153-windows"] * 16))
    assert names == ["chrome-153-windows"] * 16
    assert list(registered) == ["chrome-153-windows"]
    p = registered["chrome-153-windows"]["preset"]
    assert "Chrome/153.0.0.0" in p["headers"]["user_agent"]
    assert p["headers"]["values"]["sec-ch-ua"] == sec_ch_ua(153)
    assert p["tls"] == {"client_hello": "x"}  # fingerprint untouched


def test_replay_passes_other_presets_through(monkeypatch):
    registered = _fake_httpcloak(monkeypatch)
    for name in ("chrome-latest-windows", "chrome-152-windows", "firefox-135", None):
        assert replay_preset(name) == name
    assert not registered


def test_replay_keeps_the_name_when_httpcloak_cannot_derive(monkeypatch):
    monkeypatch.setattr(chrome_preset, "_DERIVED", set())
    monkeypatch.setitem(sys.modules, "httpcloak", types.SimpleNamespace(
        available_presets=lambda: {"chrome-143-windows": {}}))  # an httpcloak without JSON presets
    assert replay_preset("chrome-153-windows") == "chrome-153-windows"


def test_real_httpcloak_accepts_the_derived_preset():
    httpcloak = pytest.importorskip("httpcloak")
    try:
        httpcloak.available_presets()
    except OSError:
        pytest.skip("httpcloak's native library is not loadable here")
    name = ensure_chrome_preset(153)
    assert ensure_chrome_preset(153) == name  # a second caller in the process
    # An unknown name only fails at the first request; a registered one is described.
    spec = json.loads(httpcloak.describe_preset(name))["preset"]
    assert spec["name"] == "chrome-153-windows" and "Chrome/153." in spec["headers"]["user_agent"]
