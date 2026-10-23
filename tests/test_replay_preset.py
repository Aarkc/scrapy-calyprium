"""A solve's clearance is replayed as the Chrome its user agent names (Mimic PR #241, 2026-10-23).

9 of 11 native Cloudflare clearances were refused on their first replay: Tessera names the bundled
``chrome-latest-windows`` for its Chrome 155 user agent, and in httpcloak 1.7.2 that preset is Chrome
152, whose sec-ch-ua (and an Accept without image/jxl) went out beside the Chrome/155 user agent.
``chrome_preset.replay_preset(preset, user_agent)`` replaces a Chrome preset of another major with
``chrome-<N>-windows`` for a Windows Chrome N user agent, as Mimic's does, and the local fetcher
replays every slot on it.
"""
import json
import sys
import types

import pytest

from scrapy_calyprium.routing import chrome_preset
from scrapy_calyprium.routing.chrome_preset import accept_document, ensure_chrome_preset, replay_preset, sec_ch_ua
from tests.test_local_fetch import fake_httpcloak  # noqa: F401 — the fixture

UA155 = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
         "Chrome/155.0.0.0 Safari/537.36")
ACCEPT_152 = ("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
              "*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")


@pytest.fixture(autouse=True)
def _fresh_answers(monkeypatch):
    """replay_preset remembers its answers for the process: each test starts without them."""
    monkeypatch.setattr(chrome_preset, "_REPLAY_PRESETS", {})


@pytest.fixture
def derive(monkeypatch):
    """httpcloak as shipped (its newest bundled Windows preset is Chrome 152), deriving chrome-<N>-windows."""
    derived = []

    def ensure(major):
        derived.append(major)
        return f"chrome-{major}-windows"

    monkeypatch.setitem(sys.modules, "httpcloak", types.SimpleNamespace())
    monkeypatch.setattr(chrome_preset, "_base_preset", lambda httpcloak: "chrome-152-windows")
    monkeypatch.setattr(chrome_preset, "ensure_chrome_preset", ensure)
    return derived


def _registry(monkeypatch, module):
    """``module`` (a fake httpcloak) bundles Chrome 152 as httpcloak 1.7.2 does and registers JSON presets."""
    registered = {}
    base = {"version": 1, "preset": {
        "name": "chrome-152-windows", "tls": {"client_hello": "x"},
        "headers": {"user_agent": UA155.replace("155.", "152."),
                    "values": {"sec-ch-ua": sec_ch_ua(152), "Accept": ACCEPT_152},
                    "order": [{"key": "sec-ch-ua", "value": sec_ch_ua(152)}, {"key": "Accept", "value": ACCEPT_152},
                              {"key": "user-agent", "value": ""}]}}}

    def load_preset_from_json(blob):
        name = json.loads(blob)["preset"]["name"]
        if name in registered:
            raise RuntimeError(f'preset name "{name}" already registered')
        registered[name] = json.loads(blob)["preset"]
        return name

    monkeypatch.setattr(chrome_preset, "_DERIVED", set())
    monkeypatch.setattr(module, "available_presets", lambda: {
        "chrome-latest-windows": {}, "chrome-latest": {}, "chrome-143": {}, "chrome-152-windows": {}}, raising=False)
    monkeypatch.setattr(module, "describe_preset", lambda name: json.dumps(base), raising=False)
    monkeypatch.setattr(module, "load_preset_from_json", load_preset_from_json, raising=False)
    return registered


def test_the_replay_preset_follows_the_user_agent(derive):
    firefox = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0"
    mac = UA155.replace("Windows NT 10.0; Win64; x64", "Macintosh; Intel Mac OS X 10_15_7")
    edge = UA155 + " Edg/155.0.0.0"
    assert replay_preset("chrome-latest-windows", UA155) == "chrome-155-windows"
    assert replay_preset("chrome-latest", UA155) == "chrome-155-windows"
    assert replay_preset("chrome-146", UA155) == "chrome-155-windows"
    assert replay_preset("chrome-155-windows", UA155) == "chrome-155-windows"  # registered here too
    # Same Chrome already, or nothing a Windows Chrome preset can present: unchanged.
    assert replay_preset("chrome-latest-windows", UA155.replace("155.", "152.")) == "chrome-latest-windows"
    assert replay_preset("chrome-latest-windows", mac) == "chrome-latest-windows"
    assert replay_preset("chrome-latest-windows", edge) == "chrome-latest-windows"
    assert replay_preset("chrome-146-linux", UA155) == "chrome-146-linux"
    assert replay_preset("firefox-135", firefox) == "firefox-135"
    assert replay_preset("chrome-latest-windows", "Mozilla/5.0 Test") == "chrome-latest-windows"
    assert replay_preset("chrome-latest-windows", None) == "chrome-latest-windows"
    assert replay_preset(None, UA155) is None
    # Without a user agent a solver's derived preset is still registered, as before.
    assert replay_preset("chrome-153-windows") == "chrome-153-windows"
    assert derive == [155, 155, 155, 155, 153]


def test_a_failed_derivation_replays_on_the_named_preset(monkeypatch):
    def broken(major):
        raise RuntimeError("httpcloak without JSON presets")

    monkeypatch.setitem(sys.modules, "httpcloak", types.SimpleNamespace())
    monkeypatch.setattr(chrome_preset, "_base_preset", lambda httpcloak: "chrome-152-windows")
    monkeypatch.setattr(chrome_preset, "ensure_chrome_preset", broken)
    assert replay_preset("chrome-latest-windows", UA155) == "chrome-latest-windows"
    assert chrome_preset._REPLAY_PRESETS == {}  # a failure is not remembered: the next replay asks again


def test_the_answer_is_remembered(derive):
    assert replay_preset("chrome-latest-windows", UA155) == "chrome-155-windows"
    assert replay_preset("chrome-latest-windows", UA155) == "chrome-155-windows"
    assert derive == [155]  # derived (or looked up) once


def test_a_derived_preset_sends_its_chromes_accept(monkeypatch):
    """Chrome 155 accepts image/jxl again; the derived preset says so, as Tessera's and Mimic's do."""
    module = types.SimpleNamespace()
    monkeypatch.setitem(sys.modules, "httpcloak", module)
    registered = _registry(monkeypatch, module)
    ensure_chrome_preset(155)
    ensure_chrome_preset(153)
    p155, p153 = registered["chrome-155-windows"], registered["chrome-153-windows"]
    assert p155["headers"]["values"]["Accept"] == accept_document(155) and "image/jxl" in accept_document(155)
    assert p153["headers"]["values"]["Accept"] == accept_document(153) == ACCEPT_152
    assert {e["key"]: e["value"] for e in p155["headers"]["order"]}["Accept"] == accept_document(155)
    assert p155["headers"]["values"]["sec-ch-ua"] == sec_ch_ua(155)
    assert "Chrome/155.0.0.0" in p155["headers"]["user_agent"]


@pytest.mark.asyncio
async def test_a_slot_replay_presents_its_user_agents_chrome(fake_httpcloak, monkeypatch):  # noqa: F811
    """The local fetcher replays a slot on the preset presenting its user agent's Chrome."""
    registered = _registry(monkeypatch, fake_httpcloak)
    from scrapy_calyprium.routing.local_fetch import LocalFetcher

    fetcher = LocalFetcher(default_preset="chrome-143", timeout=30, backend="httpcloak")
    await fetcher.fetch("https://shop.example/", cookies=[{"name": "cf_clearance", "value": "x"}],
                        user_agent=UA155, preset="chrome-latest-windows")
    assert fake_httpcloak.Session.last_init["preset"] == "chrome-155-windows"
    assert fake_httpcloak.Session.last_get["headers"]["User-Agent"] == UA155
    assert list(registered) == ["chrome-155-windows"]
    assert registered["chrome-155-windows"]["headers"]["values"]["sec-ch-ua"] == sec_ch_ua(155)

    # A fetch without a user agent (the light path) presents its preset's own Chrome.
    await fetcher.fetch("https://shop.example/")
    assert fake_httpcloak.Session.last_init["preset"] == "chrome-143"


def test_real_httpcloak_presents_the_user_agents_chrome():
    httpcloak = pytest.importorskip("httpcloak")
    try:
        bundled = httpcloak.available_presets()
    except OSError:
        pytest.skip("httpcloak's native library is not loadable here")
    if "chrome-155-windows" in bundled:
        pytest.skip("this httpcloak bundles Chrome 155 itself")
    name = replay_preset("chrome-latest-windows", UA155)
    assert name == "chrome-155-windows"
    spec = json.loads(httpcloak.describe_preset(name))["preset"]
    values = {k.lower(): v for k, v in spec["headers"]["values"].items()}
    assert values["sec-ch-ua"] == sec_ch_ua(155)
    assert values["accept"] == accept_document(155)
    assert "Chrome/155." in spec["headers"]["user_agent"]
