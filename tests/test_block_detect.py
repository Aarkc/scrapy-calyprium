"""Tests for the spider-side block detector.

These mirror the server-side `mimic.routing.httpcloak.is_blocked` tests so the
SDK and Mimic stay in sync. The SDK adds bytes-input handling for binary
content, which is the main thing the spider-side path needs to get right.

AAR-15.
"""
import pytest

from scrapy_calyprium.routing.block_detect import is_blocked


# ---------------------------------------------------------------------------
# Binary inputs (the AAR-12 fix point)
# ---------------------------------------------------------------------------


class TestBinaryInputs:
    def test_pdf_bytes_not_blocked(self):
        body = b"%PDF-1.7\n%\xb5\xed\xae\xfb\n1 0 obj\n<<>>\nendobj\n"
        assert is_blocked(200, body) is False

    def test_jpeg_bytes_not_blocked(self):
        body = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01"
        assert is_blocked(200, body) is False

    def test_png_bytes_not_blocked(self):
        body = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
        assert is_blocked(200, body) is False

    def test_zip_bytes_not_blocked(self):
        body = b"PK\x03\x04\x14\x00\x00\x00"
        assert is_blocked(200, body) is False

    def test_403_with_bytes_pdf_not_blocked_by_signature(self):
        # 403 + PDF body is unusual but should still be classified by status alone
        body = b"%PDF-1.7\n"
        # Small body + 403 status -> blocked by the < 20KB rule
        assert is_blocked(403, body) is True


# ---------------------------------------------------------------------------
# HTML / text challenges
# ---------------------------------------------------------------------------


class TestChallengeSignatures:
    @pytest.mark.parametrize(
        "snippet",
        [
            "<title>Just a moment...</title>",
            "<title>Attention Required! | Cloudflare</title>",
            "checking your browser before accessing",
            "cf-browser-verification",
            "datadome",
            "px-captcha",
        ],
    )
    def test_challenge_phrases_in_200_body(self, snippet):
        body = (
            "<!DOCTYPE html><html><head>" + snippet
            + "</head><body></body></html>"
        )
        assert is_blocked(200, body) is True

    def test_real_page_with_recaptcha_widget_is_still_blocked(self):
        # recaptcha is in CHALLENGE_SIGNATURES — even on a real page it would
        # currently flag. This documents existing behavior; if we want to
        # exempt sites that legitimately use recaptcha we'd need a softer rule.
        body = "<html><body>" + ("<a></a>" * 10) + "recaptcha</body></html>"
        assert is_blocked(200, body) is True


class TestStatusCodes:
    def test_403_small_body_blocked(self):
        body = "<html><body>nope</body></html>"
        assert is_blocked(403, body) is True

    def test_429_small_body_blocked(self):
        body = "<html></html>"
        assert is_blocked(429, body) is True

    def test_503_small_body_blocked(self):
        body = "<html></html>"
        assert is_blocked(503, body) is True

    def test_404_small_body_not_blocked(self):
        # 404 is a real not-found, not a bot block
        body = "<html><body>not found</body></html>"
        assert is_blocked(404, body) is False


class TestRealPagePassthrough:
    def test_200_real_page_with_structure_passes(self):
        body = (
            "<!DOCTYPE html><html><head><title>Real Product Page</title></head>"
            "<body><nav></nav><main>"
            + ("<a href='/x'>link</a>" * 10)
            + "</main><footer></footer></body></html>"
        )
        # Has real structure even though it's small
        assert is_blocked(200, body) is False

    def test_200_xml_sitemap_passes(self):
        body = '<?xml version="1.0"?><urlset><url><loc>https://x</loc></url></urlset>'
        assert is_blocked(200, body) is False

    def test_200_json_api_passes(self):
        body = '{"data": [1, 2, 3]}'
        assert is_blocked(200, body) is False

    def test_200_tiny_stub_blocked(self):
        body = "<html></html>"
        assert is_blocked(200, body) is True
