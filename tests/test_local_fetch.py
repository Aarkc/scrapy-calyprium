"""Tests for the spider-side LocalFetcher.

These tests are split into two groups:

1. **Unit tests** that don't need network. They mock the underlying
   httpcloak / curl_cffi backends and assert that:
     - the response body is preserved as raw bytes (the AAR-12 fix point)
     - cookies, user-agent, proxy session injection work correctly
     - errors get wrapped as LocalFetchError

2. **Integration tests** marked with `network` that fetch real URLs and
   verify the bytes match. These are skipped by default. Run them with
   `pytest -m network`.

AAR-15.
"""
from __future__ import annotations

import asyncio
import sys
import types
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

# We need to import the local_fetch module without triggering an
# ImportError when neither backend is installed locally. The module is
# defensive and detects backends at import time, so we can stub one in.


@pytest.fixture
def fake_httpcloak(monkeypatch):
    """Inject a fake `httpcloak` module before importing local_fetch.

    Lets us test the httpcloak backend code path without the Rust extension
    actually being installed.
    """
    fake_module = types.ModuleType("httpcloak")

    class FakeResponse:
        def __init__(self, status_code=200, content=b"", headers=None, url="https://x"):
            self.status_code = status_code
            self.content = content
            self.body = content
            self.text = content.decode("latin-1", errors="replace") if content else ""
            self.headers = headers or {}
            self.url = url

    class FakeSession:
        last_init: Dict[str, Any] = {}
        last_get: Dict[str, Any] = {}
        next_response: Any = None

        def __init__(self, preset=None, proxy=None, timeout=None):
            FakeSession.last_init = {
                "preset": preset, "proxy": proxy, "timeout": timeout,
            }

        def get(self, url, headers=None):
            FakeSession.last_get = {"url": url, "headers": headers}
            if isinstance(FakeSession.next_response, Exception):
                raise FakeSession.next_response
            return FakeSession.next_response or FakeResponse()

        def close(self):
            pass

    fake_module.Session = FakeSession
    fake_module.FakeResponse = FakeResponse  # for tests to construct
    monkeypatch.setitem(sys.modules, "httpcloak", fake_module)

    # Force re-detection in the local_fetch module
    import importlib
    import scrapy_calyprium.routing.local_fetch as lf
    importlib.reload(lf)

    return fake_module


@pytest.fixture
def fetcher(fake_httpcloak):
    from scrapy_calyprium.routing.local_fetch import LocalFetcher
    return LocalFetcher(default_preset="chrome-143", timeout=30, backend="httpcloak")


# ---------------------------------------------------------------------------
# Backend availability
# ---------------------------------------------------------------------------


class TestBackendAvailability:
    def test_local_fetch_available_when_httpcloak_present(self, fake_httpcloak):
        from scrapy_calyprium.routing.local_fetch import is_local_fetch_available
        assert is_local_fetch_available() is True

    def test_local_fetch_picks_httpcloak_first(self, fake_httpcloak):
        from scrapy_calyprium.routing.local_fetch import LocalFetcher
        fetcher = LocalFetcher()
        assert fetcher.backend == "httpcloak"


# ---------------------------------------------------------------------------
# Binary response preservation (the AAR-12 regression test)
# ---------------------------------------------------------------------------


class TestBinaryPreservation:
    @pytest.mark.asyncio
    async def test_pdf_bytes_survive_intact(self, fake_httpcloak, fetcher):
        # A PDF with bytes that would be replaced with U+FFFD if any text
        # decoding was applied along the way. We use the exact byte pattern
        # from the AAR-12 reproduction.
        pdf_bytes = (
            b"%PDF-1.7\n%\xb5\xed\xae\xfb\n"
            b"1 0 obj\n<<>>\nendobj\n"
            + b"\x00\x01\x02\x03\x80\x90\xa0\xb0\xc0\xd0\xe0\xf0\xff" * 50
        )
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=pdf_bytes,
            headers={"Content-Type": "application/pdf"},
        )

        result = await fetcher.fetch("https://example.com/file.pdf")

        # Byte-identical
        assert result.body == pdf_bytes
        # No replacement chars
        assert b"\xef\xbf\xbd" not in result.body
        # Header still readable
        assert result.headers.get("Content-Type") == "application/pdf"

    @pytest.mark.asyncio
    async def test_jpeg_bytes_survive_intact(self, fake_httpcloak, fetcher):
        jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF" + bytes(range(256))
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=jpeg,
            headers={"Content-Type": "image/jpeg"},
        )

        result = await fetcher.fetch("https://example.com/img.jpg")
        assert result.body == jpeg
        assert b"\xef\xbf\xbd" not in result.body

    @pytest.mark.asyncio
    async def test_content_encoding_stripped(self, fake_httpcloak, fetcher):
        """httpcloak decompresses internally, so Content-Encoding/Content-Length
        from the wire response no longer apply to the bytes we hand back. They
        MUST be stripped or downstream Scrapy middlewares will try to gunzip
        the already-decompressed body. AAR-17 second corruption bug."""
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=b"%PDF-1.7",
            headers={
                "Content-Type": ["application/pdf"],
                "Content-Encoding": ["gzip"],
                "Content-Length": ["12345"],
                "Vary": ["Accept-Encoding"],
            },
        )
        result = await fetcher.fetch("https://example.com/file.pdf")
        assert "Content-Encoding" not in result.headers
        assert "content-encoding" not in {k.lower() for k in result.headers}
        assert "Content-Length" not in result.headers
        assert "content-length" not in {k.lower() for k in result.headers}
        # Other headers preserved
        assert result.headers["Content-Type"] == "application/pdf"

    @pytest.mark.asyncio
    async def test_list_valued_headers_are_flattened(
        self, fake_httpcloak, fetcher,
    ):
        """httpcloak returns headers as lists. They MUST be flattened to plain
        strings before reaching downstream Scrapy middlewares — otherwise
        Content-Encoding becomes the literal string "['gzip']" which trips
        HttpCompressionMiddleware and corrupts the body. This was the root
        cause of the AAR-17 production rollout failure on 2026-04-08.
        """
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=b"%PDF-1.7",
            headers={
                "Content-Type": ["application/pdf"],
                "Content-Encoding": ["gzip"],
                "Vary": ["Accept-Encoding", "Cookie"],
            },
        )

        result = await fetcher.fetch("https://example.com/file.pdf")

        assert result.headers["Content-Type"] == "application/pdf"
        assert result.headers["Vary"] == "Accept-Encoding, Cookie"
        # Content-Encoding is stripped (httpcloak decompresses internally),
        # so the existence test is the regression for that AAR-17 bug.
        assert "Content-Encoding" not in result.headers

    @pytest.mark.asyncio
    async def test_html_response_decodes_via_text_helper(
        self, fake_httpcloak, fetcher,
    ):
        html = "<html><body>hello</body></html>".encode("utf-8")
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=html,
            headers={"Content-Type": "text/html; charset=utf-8"},
        )

        result = await fetcher.fetch("https://example.com/")
        assert result.body == html
        assert result.text() == "<html><body>hello</body></html>"


# ---------------------------------------------------------------------------
# Header / cookie / proxy injection
# ---------------------------------------------------------------------------


class TestHeaderInjection:
    @pytest.mark.asyncio
    async def test_cookies_serialized_into_cookie_header(
        self, fake_httpcloak, fetcher,
    ):
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=b"ok",
        )

        await fetcher.fetch(
            "https://example.com/",
            cookies=[
                {"name": "cf_clearance", "value": "abc"},
                {"name": "__cf_bm", "value": "xyz"},
            ],
        )

        sent = fake_httpcloak.Session.last_get["headers"]
        assert sent["Cookie"] == "cf_clearance=abc; __cf_bm=xyz"
        # Compression must be disabled when injecting cookies
        assert sent["Accept-Encoding"] == "identity"

    @pytest.mark.asyncio
    async def test_user_agent_passed_through(self, fake_httpcloak, fetcher):
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=b"ok",
        )

        await fetcher.fetch(
            "https://example.com/",
            user_agent="Mozilla/5.0 Test",
        )

        sent = fake_httpcloak.Session.last_get["headers"]
        assert sent["User-Agent"] == "Mozilla/5.0 Test"

    @pytest.mark.asyncio
    async def test_proxy_session_injection(self, fake_httpcloak, fetcher):
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=b"ok",
        )

        await fetcher.fetch(
            "https://example.com/",
            proxy_url="http://user:pass@proxy.example:8080",
            proxy_session_id="sess-abc",
        )

        proxy = fake_httpcloak.Session.last_init["proxy"]
        assert "session_sess-abc" in proxy
        assert "user-session_sess-abc" in proxy

    @pytest.mark.asyncio
    async def test_no_proxy_session_when_no_proxy(self, fake_httpcloak, fetcher):
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=200, content=b"ok",
        )
        await fetcher.fetch(
            "https://example.com/",
            proxy_session_id="sess-abc",
        )
        # No proxy URL was provided, so the session id has nowhere to be
        # injected. Should not crash.
        assert fake_httpcloak.Session.last_init["proxy"] is None


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_backend_exception_wrapped_as_local_fetch_error(
        self, fake_httpcloak, fetcher,
    ):
        from scrapy_calyprium.routing.local_fetch import LocalFetchError

        fake_httpcloak.Session.next_response = RuntimeError("connection refused")

        with pytest.raises(LocalFetchError) as exc_info:
            await fetcher.fetch("https://example.com/")

        assert "connection refused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_4xx_status_returned_not_raised(
        self, fake_httpcloak, fetcher,
    ):
        fake_httpcloak.Session.next_response = fake_httpcloak.FakeResponse(
            status_code=403, content=b"forbidden",
        )

        result = await fetcher.fetch("https://example.com/")
        assert result.status_code == 403
        assert result.body == b"forbidden"
