"""Detect bot-block / challenge responses from a fetched page.

Ported from `mimic.routing.httpcloak.is_blocked` so the spider can decide
locally whether a fetch needs to be escalated to a Mimic browser solve.

Decisions live in this single function so the SDK and the server-side router
can stay in sync. AAR-15.
"""
import re
from typing import Union

# Status codes that strongly indicate bot blocking
BLOCKED_STATUS_CODES = {403, 429, 503}

# HTML content signatures that indicate a challenge page (any status code).
# Each entry is checked against the first 8KB of lowercased HTML.
CHALLENGE_SIGNATURES = (
    # Cloudflare
    "cf-browser-verification",
    "cf_chl_opt",
    "_cf_chl",
    "managed challenge",
    "just a moment",
    "checking your browser",
    "attention required",
    # Anti-bot services
    "ddos-guard",
    "hcaptcha.com",
    "recaptcha",
    "perimeterx",
    "px-captcha",
    "datadome",
    "kasada",
    # Amazon
    "to discuss automated access",
    "robot check",
    "enter the characters you see below",
    "sorry, we just need to make sure you're not a robot",
    # Generic
    "bot detection",
    "automated access",
    "verify you are human",
    "please verify you are a human",
    "browser verification",
)

# Signatures that ONLY matter when combined with a block status code.
# These are too common in normal pages to use as standalone signals.
SOFT_BLOCK_SIGNATURES = (
    "access denied",
    "ray id",
    "continue shopping",
)

# Minimum HTML size (bytes) to consider a page "real" vs a challenge stub.
# Only applied to 200 responses that fail structural checks below.
MIN_CONTENT_SIZE = 10_000


def _has_real_page_structure(html: str) -> bool:
    """Check if HTML looks like a real page rather than a challenge stub.

    Real pages have a <title> with content and either meaningful text or
    structural tags (<nav>, <main>, <article>, <footer>, multiple <a> links).
    Challenge stubs typically have an empty or generic title and almost no
    real markup beyond a single <div> with a spinner or redirect script.
    """
    lower = html[:8000].lower()

    # Page has a non-empty <title>
    title_match = re.search(r"<title[^>]*>([^<]+)</title>", lower)
    if title_match:
        title_text = title_match.group(1).strip()
        challenge_titles = {
            "just a moment",
            "attention required",
            "access denied",
            "please wait",
        }
        if title_text and title_text not in challenge_titles:
            return True

    if lower.count("<a ") >= 3:
        return True

    for tag in ("<nav", "<main", "<article", "<footer", "<header"):
        if tag in lower:
            return True

    return False


def _is_binary_magic(prefix: bytes) -> bool:
    """Return True if the byte prefix looks like a known binary file format."""
    return (
        prefix.startswith(b"%PDF")
        or prefix.startswith(b"\xff\xd8")
        or prefix.startswith(b"\x89PNG")
        or prefix.startswith(b"GIF8")
        or prefix.startswith(b"RIFF")
        or prefix.startswith(b"PK\x03\x04")
    )


def is_blocked(status_code: int, body: Union[bytes, str]) -> bool:
    """Detect if a response indicates bot blocking or a challenge page.

    Accepts either bytes or str. For binary content, the HTML challenge
    signature checks are skipped (a PDF or JPEG can't contain
    "cf-browser-verification") but the status-code-based checks still apply,
    so a 403 response with a tiny binary body is still flagged as blocked.

    For HTML responses, decodes a small prefix as latin-1 (which never fails)
    and runs the signature checks. Latin-1 is safe because all the signatures
    are pure ASCII.
    """
    is_binary = False
    body_len = len(body)

    if isinstance(body, bytes):
        prefix = body[:8000]
        if _is_binary_magic(prefix):
            is_binary = True
            html = ""  # don't run HTML signature checks on binary
        else:
            try:
                html = prefix.decode("latin-1")
            except Exception:
                return False
    else:
        html = body[:8000]

    html_lower = html.lower()

    if not is_binary:
        for sig in CHALLENGE_SIGNATURES:
            if sig in html_lower:
                return True

        if status_code in BLOCKED_STATUS_CODES:
            for sig in SOFT_BLOCK_SIGNATURES:
                if sig in html_lower:
                    return True

    # Status-code based block: applies regardless of content type. A 403 with
    # a small body — binary or HTML — is virtually always a CDN block stub.
    if status_code in BLOCKED_STATUS_CODES and body_len < 20_000:
        return True

    # Binary content past the size guard is real. We don't need to look for
    # text signatures inside it.
    if is_binary:
        return False

    if status_code == 200 and len(html) < MIN_CONTENT_SIZE:
        stripped = html.strip()
        if (
            stripped.startswith("<?xml")
            or stripped.startswith("<sitemapindex")
            or stripped.startswith("<urlset")
        ):
            return False
        if stripped.startswith("{") or stripped.startswith("["):
            return False
        if _has_real_page_structure(html):
            return False
        text = re.sub(r"<[^>]+>", "", html)
        text = " ".join(text.split())
        if len(text) < 500:
            return True

    return False
