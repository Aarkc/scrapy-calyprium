"""A Firefox replay reaches a host whose TLS server prefers a finite-field group.

curl_cffi's Firefox ClientHello (every firefox* target) lists ffdhe2048 and ffdhe3072 after its
elliptic curves, as Firefox does, but its BoringSSL cannot compute either. wyobiz.wyo.gov's F5
BIG-IP answers a ClientHello listing ffdhe3072 with a HelloRetryRequest for it, so every replay of a
camoufox (Firefox) clearance there failed in the handshake: curl error 35, SSL_ERROR_SYSCALL, on
every exit, while Chrome and Safari identities (which list no ffdhe group) got the page. The fetcher
now tries such a handshake once more without those two groups and remembers the host.
"""
from __future__ import annotations

import socket
import struct
import threading
from typing import Any, Dict, List

import pytest

import scrapy_calyprium.routing.local_fetch as lf

HANDSHAKE_EOF = ("Failed to perform, curl: (35) BoringSSL SSL_connect: no transport error details "
                 "available (SSL_ERROR_SYSCALL; error queue empty) in connection to wyobiz.wyo.gov:443.")
TIMEOUT = "Failed to perform, curl: (28) Operation timed out after 30001 milliseconds"
URL = "https://wyobiz.wyo.gov/Business/FilingSearch.aspx"
PAGE = b"<!DOCTYPE html><html><head><title>Search</title></head><body><main>ok</main></body></html>"


class Response:
    status_code = 200
    content = PAGE
    headers = {"content-type": "text/html"}
    url = URL


def fake_session(outcomes: List[Any]):
    """A curl_cffi AsyncSession stand-in: each request takes the next outcome (an exception message
    raises, anything else answers the page) and records the session's options."""
    calls: List[Dict[str, Any]] = []

    class AsyncSession:
        def __init__(self, impersonate=None, curl_options=None):
            self.init = {"impersonate": impersonate, "curl_options": curl_options}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def _answer(self, method, url, kwargs):
            calls.append({**self.init, "method": method, "url": url, **kwargs})
            outcome = outcomes.pop(0) if outcomes else None
            if isinstance(outcome, str):
                raise RuntimeError(outcome)
            return Response()

        async def get(self, url, **kwargs):
            return await self._answer("GET", url, kwargs)

        async def request(self, method, url, **kwargs):
            return await self._answer(method, url, kwargs)

    return AsyncSession, calls


def _curves(call) -> Any:
    opts = call["curl_options"] or {}
    return next(iter(opts.values())) if opts else None


@pytest.fixture
def fetcher(monkeypatch):
    def make(outcomes):
        session, calls = fake_session(outcomes)
        monkeypatch.setattr(lf, "_CURL_CFFI_ASYNC", session)
        monkeypatch.setattr(lf, "_HTTPCLOAK", None)  # every preset to the fake curl_cffi, no network
        monkeypatch.setattr(lf, "_no_ffdhe_options", lambda: {"SSL_EC_CURVES": lf.FIREFOX_CURVES_NO_FFDHE})
        return lf.LocalFetcher(default_preset="firefox-135", timeout=30, backend="curl_cffi"), calls
    return make


@pytest.mark.asyncio
async def test_a_refused_firefox_handshake_is_sent_again_without_the_ffdhe_groups(fetcher):
    f, calls = fetcher([HANDSHAKE_EOF])
    result = await f.fetch(URL, preset="firefox-135", user_agent="Firefox-UA",
                           cookies=[{"name": "TS01ab71c3", "value": "v"}])
    assert result.status_code == 200 and result.body == PAGE
    assert [c["impersonate"] for c in calls] == ["firefox135", "firefox135"]
    assert [_curves(c) for c in calls] == [None, lf.FIREFOX_CURVES_NO_FFDHE]
    # the second attempt is the same request: its user agent and cookies ride on it
    assert calls[1]["headers"]["User-Agent"] == "Firefox-UA"
    assert calls[1]["cookies"] == {"TS01ab71c3": "v"}
    assert f.no_ffdhe_hosts == {"wyobiz.wyo.gov"}


@pytest.mark.asyncio
async def test_the_host_is_remembered_and_other_hosts_keep_firefoxs_groups(fetcher):
    f, calls = fetcher([HANDSHAKE_EOF])
    await f.fetch(URL, preset="firefox-135")
    await f.fetch(URL + "?page=2", preset="firefox-135")
    await f.fetch("https://example.com/", preset="firefox-135")
    assert [_curves(c) for c in calls] == [None, lf.FIREFOX_CURVES_NO_FFDHE,
                                           lf.FIREFOX_CURVES_NO_FFDHE, None]


@pytest.mark.asyncio
async def test_a_post_is_sent_again_as_made(fetcher):
    f, calls = fetcher([HANDSHAKE_EOF])
    await f.fetch(URL, preset="firefox-135", method="POST", body=b"a=1",
                  extra_headers={"Content-Type": "application/x-www-form-urlencoded"})
    assert [(c["method"], c["data"]) for c in calls] == [("POST", b"a=1"), ("POST", b"a=1")]


@pytest.mark.asyncio
async def test_when_the_second_handshake_fails_too_the_first_error_stands(fetcher):
    f, calls = fetcher([HANDSHAKE_EOF, "Failed to perform, curl: (7) CONNECT tunnel failed, response 502"])
    with pytest.raises(lf.LocalFetchError, match=r"curl: \(35\)"):
        await f.fetch(URL, preset="firefox-135")
    assert len(calls) == 2 and not f.no_ffdhe_hosts


@pytest.mark.asyncio
async def test_a_remembered_host_is_not_tried_a_third_way(fetcher):
    f, calls = fetcher([HANDSHAKE_EOF, None, HANDSHAKE_EOF])
    await f.fetch(URL, preset="firefox-135")
    with pytest.raises(lf.LocalFetchError):
        await f.fetch(URL, preset="firefox-135")
    assert [_curves(c) for c in calls] == [None, lf.FIREFOX_CURVES_NO_FFDHE, lf.FIREFOX_CURVES_NO_FFDHE]


@pytest.mark.asyncio
@pytest.mark.parametrize("preset,error", [
    ("chrome-146", HANDSHAKE_EOF),   # a Chrome ClientHello lists no ffdhe group
    ("firefox-135", TIMEOUT),        # not a handshake failure
])
async def test_other_failures_are_not_sent_again(fetcher, preset, error):
    f, calls = fetcher([error])
    with pytest.raises(lf.LocalFetchError):
        await f.fetch(URL, preset=preset)
    assert len(calls) == 1 and _curves(calls[0]) is None and not f.no_ffdhe_hosts


# ---------------------------------------------------------------------------
# The real curl_cffi: the option takes the two groups out and nothing else (no network: the
# ClientHello is read on a local socket).
# ---------------------------------------------------------------------------

GREASE = {0x0A0A + 0x1010 * i for i in range(16)}


def _client_hello(impersonate: str, curl_options=None) -> Dict[str, List[int]]:
    from curl_cffi import requests as cr

    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    received: List[bytes] = []

    def accept():
        conn, _ = server.accept()
        data = b""
        while len(data) < 5 or len(data) < 5 + struct.unpack(">H", data[3:5])[0]:
            chunk = conn.recv(65536)
            if not chunk:
                break
            data += chunk
        received.append(data)
        conn.close()

    thread = threading.Thread(target=accept)
    thread.start()
    try:
        cr.get(f"https://127.0.0.1:{server.getsockname()[1]}/", impersonate=impersonate,
               timeout=5, verify=False, curl_options=curl_options)
    except Exception:  # noqa: BLE001 -- the server hangs up after reading the hello
        pass
    thread.join(10)
    server.close()
    data = received[0]
    p = 5 + 4 + 2 + 32
    p += 1 + data[p]
    n = struct.unpack(">H", data[p:p + 2])[0]
    ciphers = [struct.unpack(">H", data[p + 2 + i:p + 4 + i])[0] for i in range(0, n, 2)]
    p += 2 + n
    p += 1 + data[p]
    end = p + 2 + struct.unpack(">H", data[p:p + 2])[0]
    p += 2
    out: Dict[str, List[int]] = {"ciphers": [c for c in ciphers if c not in GREASE], "extensions": [],
                                 "groups": [], "key_shares": []}
    while p < end:
        ext, length = struct.unpack(">HH", data[p:p + 4])
        body = data[p + 4:p + 4 + length]
        p += 4 + length
        if ext in GREASE:
            continue
        out["extensions"].append(ext)
        if ext == 10:
            out["groups"] = [g for g in (struct.unpack(">H", body[2 + i:4 + i])[0]
                                         for i in range(0, struct.unpack(">H", body[:2])[0], 2))
                             if g not in GREASE]
        if ext == 51:
            q = 2
            while q < len(body):
                group, size = struct.unpack(">HH", body[q:q + 4])
                if group not in GREASE:
                    out["key_shares"].append(group)
                q += 4 + size
    return out


FFDHE2048, FFDHE3072, X25519 = 256, 257, 29


@pytest.mark.parametrize("target", ["firefox133", "firefox135", "firefox144", "firefox147"])
def test_the_option_leaves_out_only_the_finite_field_groups(target):
    pytest.importorskip("curl_cffi")
    options = lf._no_ffdhe_options()
    assert options, "this curl_cffi has no SSL_EC_CURVES option"
    try:
        firefox = _client_hello(target)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"curl_cffi cannot impersonate {target}: {exc}")
    without = _client_hello(target, options)
    assert {FFDHE2048, FFDHE3072} <= set(firefox["groups"])
    assert without["groups"] == [g for g in firefox["groups"] if g not in (FFDHE2048, FFDHE3072)]
    assert X25519 in without["groups"]
    for part in ("ciphers", "extensions", "key_shares"):
        assert without[part] == firefox[part], part
