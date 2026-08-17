"""Detect bot-block / challenge responses from a fetched page.

Ported from `mimic.routing.httpcloak.is_blocked` so the spider can decide
locally whether a fetch needs to be escalated to a Mimic browser solve.

Decisions live in this single function so the SDK and the server-side router
can stay in sync. AAR-15.
"""
import re
from typing import Optional, Union

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

# Positive WAF / anti-bot signals carried in response headers. Their presence
# on a blocked-status response means a browser solve can actually clear it
# (mint _abck / cf_clearance / a DataDome cookie). Their ABSENCE — a plain
# Microsoft-IIS / nginx 403 with no anti-bot cookie or header — means a
# rate-limit or application error that no solve can fix, so the caller should
# back off and retry the cheap path instead of burning a solve. Matched
# case-insensitively.
_WAF_SERVER_MARKERS = ("akamaighost", "cloudflare", "ddos-guard", "incapsula", "imperva", "sucuri")
_WAF_HEADER_NAMES = ("cf-mitigated", "x-datadome", "x-iinfo", "x-sucuri-id", "x-akamai-transformed")
_WAF_COOKIE_MARKERS = (
    "_abck", "bm_sz", "ak_bmsc",     # Akamai
    "cf_clearance", "__cf_bm",       # Cloudflare
    "datadome",                      # DataDome
    "_px", "_pxhd", "_pxvid",        # PerimeterX / HUMAN
    "incap_ses", "visid_incap",      # Imperva / Incapsula
)


def _looks_like_waf(headers: Optional[dict], html_lower: str) -> bool:
    """True if the response carries a positive signal that it's a *solvable*
    WAF / anti-bot challenge (as opposed to a plain rate-limit or app error).

    Checks the response body for a known challenge signature and the response
    headers for anti-bot markers (Server: AkamaiGHost / Cloudflare / ..., a
    Set-Cookie like _abck / cf_clearance / datadome, or a dedicated header like
    cf-mitigated / x-datadome). A Microsoft-IIS / nginx 403 with none of these
    is a rate-limit, not a challenge.
    """
    for sig in CHALLENGE_SIGNATURES:
        if sig in html_lower:
            return True
    if not headers:
        return False
    # Normalise header keys/values to lowercase strings (handles str, bytes,
    # and Scrapy's list-of-bytes header values).
    norm: dict = {}
    for k, v in headers.items():
        kl = (k.decode("latin-1", "ignore") if isinstance(k, bytes) else str(k)).lower()
        if isinstance(v, (list, tuple)):
            vs = " ".join(
                x.decode("latin-1", "ignore") if isinstance(x, bytes) else str(x) for x in v
            )
        elif isinstance(v, bytes):
            vs = v.decode("latin-1", "ignore")
        else:
            vs = str(v)
        norm[kl] = vs.lower()
    if any(name in norm for name in _WAF_HEADER_NAMES):
        return True
    if any(m in norm.get("server", "") for m in _WAF_SERVER_MARKERS):
        return True
    if any(m in norm.get("set-cookie", "") for m in _WAF_COOKIE_MARKERS):
        return True
    return False


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


def is_blocked(
    status_code: int,
    body: Union[bytes, str],
    content_type: Optional[str] = None,
    headers: Optional[dict] = None,
) -> bool:
    """Detect if a response indicates bot blocking or a challenge page.

    Accepts either bytes or str. For binary content, the HTML challenge
    signature checks are skipped (a PDF or JPEG can't contain
    "cf-browser-verification") but the status-code-based checks still apply,
    so a 403 response with a tiny binary body is still flagged as blocked.

    For HTML responses, decodes a small prefix as latin-1 (which never fails)
    and runs the signature checks. Latin-1 is safe because all the signatures
    are pure ASCII.

    ``content_type`` (the response Content-Type header, if known) suppresses
    false positives on API responses: a non-error response whose content-type
    is not HTML (application/json, text/xml, images, ...) is never a bot
    challenge — challenge pages are always served as text/html. Without it, a
    clean 200 JSON body that merely contains a phrase like "access denied" or
    "automated access" (common in government/OData APIs) was mis-flagged,
    churning the fingerprint cache. Error statuses (403/429/503) still fall
    through to the status-based checks below regardless of content-type.
    """
    if (
        status_code not in BLOCKED_STATUS_CODES
        and content_type
        and "html" not in content_type.lower()
    ):
        return False

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

    # Status-code based block: a small-body 403/429/503 is usually a CDN block
    # stub — BUT only if there's a positive WAF/anti-bot signal a solve can
    # clear. When the caller passes headers and they show no such signal (a
    # plain Microsoft-IIS / nginx / OData 403 like webapi.legistar.com's burst
    # rate-limit), it's not a solvable challenge: return False so the caller
    # backs off and retries the cheap path instead of wasting a browser solve
    # and marking the domain heavy. Absent headers, preserve the old behavior.
    if status_code in BLOCKED_STATUS_CODES and body_len < 20_000:
        if headers is not None and not _looks_like_waf(headers, html_lower):
            return False
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
