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


class TestContentTypeGate:
    """A 2xx non-HTML response is never a challenge — the content-type gate
    must suppress the HTML heuristics so API bodies aren't mis-flagged."""

    # A clean JSON body that legitimately contains challenge-marker words
    # (common in government/OData APIs). Without the content-type it false-fires.
    JSON_WITH_MARKERS = (
        '{"items":[{"note":"This portal provides automated access to records; '
        'access denied to unauthenticated bots. recaptcha not required."}]}'
    )

    def test_json_marker_false_positive_without_content_type(self):
        # Documents the pre-fix behavior: markers in a JSON body trip it.
        assert is_blocked(200, self.JSON_WITH_MARKERS) is True

    def test_json_marker_suppressed_with_json_content_type(self):
        assert (
            is_blocked(200, self.JSON_WITH_MARKERS, content_type="application/json")
            is False
        )

    def test_large_json_with_marker_suppressed(self):
        big = '{"x":"' + ("automated access " * 2000) + '"}'
        assert is_blocked(200, big, content_type="application/json; charset=utf-8") is False

    def test_xml_content_type_suppressed(self):
        assert is_blocked(200, "<a>access denied</a>", content_type="text/xml") is False

    def test_error_status_still_blocks_regardless_of_content_type(self):
        # A 403 is a block by status — the gate must NOT rescue error statuses.
        assert is_blocked(403, '{"error":"forbidden"}', content_type="application/json") is True
        assert is_blocked(429, b"", content_type="application/json") is True

    def test_html_content_type_still_runs_heuristics(self):
        body = "<html><body>just a moment...</body></html>"
        assert is_blocked(200, body, content_type="text/html") is True

    def test_missing_content_type_preserves_old_behavior(self):
        # Backward compatible: no content-type → unchanged heuristics.
        assert is_blocked(200, '{"data":[1,2,3]}') is False
        assert is_blocked(200, "<html></html>") is True


class TestWafHeaderGate:
    """A small-body 403/429/503 is only a *solvable* block when the headers
    carry a WAF/anti-bot signal. A plain IIS/nginx rate-limit 403 with no such
    signal must NOT be flagged (else the caller wastes a browser solve it can
    never clear — webapi.legistar.com's burst rate-limit)."""

    def test_iis_rate_limit_403_not_blocked(self):
        # The Legistar case: Microsoft-IIS 403, small body, no anti-bot markers.
        h = {"Server": "Microsoft-IIS/10.0", "Content-Type": "application/xml"}
        assert is_blocked(403, b"rate limited", headers=h) is False

    def test_nginx_429_not_blocked(self):
        h = {"Server": "nginx", "Content-Type": "text/plain"}
        assert is_blocked(429, b"too many requests", headers=h) is False

    def test_akamai_server_403_blocked(self):
        h = {"Server": "AkamaiGHost", "Content-Type": "text/html"}
        assert is_blocked(403, b"<html></html>", headers=h) is True

    def test_abck_set_cookie_403_blocked(self):
        h = {"Server": "Microsoft-IIS/10.0", "Set-Cookie": "_abck=xyz~-1~...; Path=/"}
        assert is_blocked(403, b"blocked", headers=h) is True

    def test_cf_clearance_cookie_blocked(self):
        h = {"Server": "cloudflare", "Set-Cookie": "cf_clearance=abc; Path=/"}
        assert is_blocked(429, b"", headers=h) is True

    def test_cf_mitigated_header_blocked(self):
        h = {"Server": "cloudflare", "cf-mitigated": "challenge"}
        assert is_blocked(403, b"<html>...</html>", headers=h) is True

    def test_datadome_header_blocked(self):
        h = {"Server": "nginx", "x-datadome": "protected"}
        assert is_blocked(403, b"blocked", headers=h) is True

    def test_body_challenge_signature_wins_over_headers(self):
        # Even a non-WAF Server header can't rescue a body that IS a challenge.
        h = {"Server": "Microsoft-IIS/10.0"}
        assert is_blocked(403, b"<html>Just a moment...</html>", headers=h) is True

    def test_no_headers_preserves_old_behavior(self):
        # Absent headers, a small-body 403 stays blocked (no regression).
        assert is_blocked(403, b"blocked") is True
        assert is_blocked(429, b"") is True

    def test_scrapy_style_list_of_bytes_headers(self):
        # Scrapy's Headers yields {bytes: [bytes]} — must normalise cleanly.
        h = {b"Server": [b"Microsoft-IIS/10.0"]}
        assert is_blocked(403, b"rate limited", headers=h) is False
        h2 = {b"Set-Cookie": [b"_abck=1~-1~2; Path=/"]}
        assert is_blocked(403, b"blocked", headers=h2) is True
