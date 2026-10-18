"""Detect bot-block / challenge responses from a fetched page.

``is_blocked`` and ``diagnose`` delegate to the fetch layer's shared failure
classifier (``routing/diagnosis.py``, vendored verbatim from Mimic), so the
SDK, Mimic and Tessera hold one notion of "blocked": the diagnosis class is
one of ``BLOCKED_CLASSES`` (the site kept us from the page, as opposed to our
side failing or the page not existing). This module only adapts the SDK's
inputs (bytes bodies, Scrapy-style headers) to the classifier.

``challenge_family`` names the Tessera challenge type a block shows. AAR-15.
"""
import re
from typing import Any, Mapping, Optional, Union

from scrapy_calyprium.routing import diagnosis as dg

# Diagnosis classes that mean the site kept us from the page (diagnosis.Diagnosis.blocked).
BLOCKED_CLASSES = frozenset((
    dg.EDGE_DENY, dg.IDENTITY_DENY, dg.CHALLENGE, dg.CLEARANCE_REFUSED, dg.CAPTCHA,
    dg.SOFT_BLOCK, dg.RATE_LIMITED,
))

# Status codes that strongly indicate bot blocking (challenge_family's gate).
BLOCKED_STATUS_CODES = {403, 429, 503}

# Legacy signature lists: no longer used by is_blocked (the classifier decides), kept for
# code that imports them.
CHALLENGE_SIGNATURES = (
    "cf-browser-verification", "cf_chl_opt", "_cf_chl", "managed challenge", "just a moment",
    "checking your browser", "attention required", "ddos-guard", "hcaptcha.com", "recaptcha",
    "perimeterx", "px-captcha", "datadome", "kasada", "to discuss automated access",
    "robot check", "enter the characters you see below",
    "sorry, we just need to make sure you're not a robot", "bot detection", "automated access",
    "verify you are human", "please verify you are a human", "browser verification",
)
SOFT_BLOCK_SIGNATURES = ("access denied", "ray id", "continue shopping")
MIN_CONTENT_SIZE = 10_000

# Header-name prefixes of a vendor's own headers (Kasada's x-kpsdk-ct / -r / -c).
_WAF_HEADER_PREFIXES = ("x-kpsdk",)

# Content types for bodies that announce their format in their first bytes, used when the
# caller knows no Content-Type (a PDF or image is never a challenge page).
_BINARY_MAGIC = (
    (b"%PDF", "application/pdf"),
    (b"\xff\xd8", "image/jpeg"),
    (b"\x89PNG", "image/png"),
    (b"GIF8", "image/gif"),
    (b"RIFF", "application/octet-stream"),
    (b"PK\x03\x04", "application/zip"),
)
# Bytes of a non-error body handed to the classifier (it reads the first 65536 characters).
_SCAN_LIMIT = 262_144


def _normalise_headers(headers: Optional[dict]) -> dict:
    """Header names and values as lowercase strings (handles str, bytes, and
    Scrapy's list-of-bytes header values)."""
    norm: dict = {}
    for k, v in (headers or {}).items():
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
    return norm


def _str_headers(headers: Optional[Mapping[Any, Any]]) -> dict:
    """Headers as ``{str: str}`` for the classifier (which reads names case-insensitively)."""
    out: dict = {}
    for k, v in (headers or {}).items():
        name = k.decode("latin-1", "ignore") if isinstance(k, bytes) else str(k)
        values = v if isinstance(v, (list, tuple)) else [v]
        out[name] = ", ".join(
            x.decode("latin-1", "ignore") if isinstance(x, bytes) else str(x) for x in values
        )
    return out


def _binary_type(body: Union[bytes, str]) -> Optional[str]:
    if isinstance(body, (bytes, bytearray)):
        for magic, ctype in _BINARY_MAGIC:
            if bytes(body[:8]).startswith(magic):
                return ctype
    return None


def diagnose(
    status_code: int,
    body: Union[bytes, str],
    content_type: Optional[str] = None,
    headers: Optional[Mapping[Any, Any]] = None,
    url: Optional[str] = None,
    final_url: Optional[str] = None,
) -> dg.Diagnosis:
    """Classify one response with the shared classifier (``diagnosis.diagnose_response``).

    Two adaptations for what the SDK sees and Mimic does not: a non-error body whose first
    bytes name a binary format is classified as that format when no Content-Type is known,
    and a redirect (3xx) is never a stub page (its body is not the page)."""
    hdrs = _str_headers(headers)
    if body is None:
        body = b""
    if not content_type:
        content_type = next((v for k, v in hdrs.items() if k.lower() == "content-type"), None)
    if not content_type and int(status_code or 0) < 400:
        content_type = _binary_type(body)
    if int(status_code or 0) < 400 and len(body) > _SCAN_LIMIT:
        # Below 400 the classifier reads only the first 64K characters; don't decode a
        # multi-megabyte document to find that out.
        body = body[:_SCAN_LIMIT]
    d = dg.diagnose_response(status_code, body, content_type, hdrs, url, final_url)
    if 300 <= int(status_code or 0) < 400 and d.cls == dg.SOFT_BLOCK:
        return dg.Diagnosis(dg.OK, d.vendor, d.status, "redirect")
    return d


def is_blocked(
    status_code: int,
    body: Union[bytes, str],
    content_type: Optional[str] = None,
    headers: Optional[dict] = None,
) -> bool:
    """True when the shared classifier says the site kept us from the page: the diagnosis
    class is one of ``BLOCKED_CLASSES`` (edge deny, identity deny, challenge, refused
    clearance, captcha, soft block, rate limit).

    Accepts bytes or str bodies and plain or Scrapy-style headers. A 404, a site error, a
    sign-in / geo / consent wall or a clean API answer is not a block."""
    return diagnose(status_code, body, content_type, headers).cls in BLOCKED_CLASSES


# Cloudflare's challenge page (mimic.routing.challenge keeps the same list).
_CLOUDFLARE_MARKERS = (
    "challenge-platform", "cf-turnstile", "cf_chl_opt", "challenges.cloudflare.com",
    "just a moment", "turnstile/v0",
)
_PERIMETERX_MARKERS = ("px-captcha", "_pxappid", "human-challenge", "press & hold", "perimeterx")
# Akamai's blocking SBSD page (served 200): its only script is
# src="/<path>?v=<uuid>&t=<challenge id>"; the passive script on normal pages has no t=.
_SBSD_CHALLENGE_RE = re.compile(
    r'src="/[A-Za-z0-9/_\-.]+\?v=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    r'&(?:amp;)?t=[^"&]+"'
)
# Akamai's edge deny page: "Reference #18.x..." / "Reference-ID: #0.x...".
_AKAMAI_REFERENCE_RE = re.compile(r"reference(?:-id)?\s*:?\s*#\d+\.[0-9a-f]+\.\d+\.[0-9a-f]+", re.I)


def challenge_family(
    status_code: int,
    body: Union[bytes, str],
    headers: Optional[dict] = None,
) -> Optional[str]:
    """The Tessera challenge type a blocked response shows, or None when it names no vendor.

    A solve that names the vendor reaches that vendor's solvers; without one Tessera
    assumes a Cloudflare challenge. Cloudflare first (the most common, and its markup
    can sit beside others), then the sensor vendors; Kasada before Akamai, whose CDN it
    often sits behind. Incapsula only for its legacy ___utmvc page: the reese84 sensor
    has no solver, and a hard incident page nothing to solve.
    """
    if isinstance(body, bytes):
        text = body[:20_000].decode("latin-1")
    else:
        text = (body or "")[:20_000]
    low = text.lower()
    head = low[:4000]
    norm = _normalise_headers(headers)
    cookies = norm.get("set-cookie", "")

    if norm.get("cf-mitigated") == "challenge" or any(m in low for m in _CLOUDFLARE_MARKERS):
        return "cloudflare_managed"
    if "var dd=" in low or "geo.captcha-delivery.com" in low:
        return "datadome"
    if len(body) < 20_000 and _SBSD_CHALLENGE_RE.search(text):
        return "akamai"
    if status_code not in BLOCKED_STATUS_CODES:
        return None
    if (any(m in head for m in ("149e9513-01fa", "ips.js", "kpsdk"))
            or any(name.startswith(_WAF_HEADER_PREFIXES) for name in norm) or "kp_uidz" in cookies):
        return "kasada"
    if status_code == 403 and "_incapsula_resource" in head and "swjiylwa" in head:
        return "incapsula"
    if any(m in low for m in _PERIMETERX_MARKERS) or any(m in cookies for m in ("_pxhd", "_pxvid", "_px3")):
        return "perimeterx"
    if "x-datadome" in norm or "datadome=" in cookies:
        return "datadome"
    if ("bmak" in low or "_abck" in low or "akamaighost" in norm.get("server", "")
            or _AKAMAI_REFERENCE_RE.search(text) or any(m in cookies for m in ("_abck", "bm_sz", "ak_bmsc"))):
        return "akamai"
    return None
