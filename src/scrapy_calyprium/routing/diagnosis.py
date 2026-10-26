"""Why a fetch attempt failed: one of 16 classes, with the vendor and the evidence.

Every attempt in the fetch layer ends in exactly one class, and the class (not a blocked yes/no)
picks the next move: most failures are fixed by retrying elsewhere, slowing down or fixing our
own side, and only a reputation deny or a served challenge climbs to a dearer rung. The plan:
https://claude.ai/code/artifact/387fb48b-f75a-431e-804c-bf6ec1dd0cc0

Pure functions, standard library only, no I/O: the same file is vendored into Tessera and the
spider SDK (a test keeps the copies identical). Evidence is a short marker name, never page
content, cookies or credentials, so a diagnosis may go on spans, events and API responses.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, Optional
from urllib.parse import urlparse

VERSION = 5

OK = "ok"
PROXY_FAILURE = "proxy_failure"            # 1
CAPACITY = "capacity"                      # 2
ORIGIN_ERROR = "origin_error"              # 3
NOT_FOUND = "not_found"                    # 4
RATE_LIMITED = "rate_limited"              # 5
EDGE_DENY = "edge_deny"                    # 6
IDENTITY_DENY = "identity_deny"            # 7
CHALLENGE = "challenge"                    # 8
CLEARANCE_REFUSED = "clearance_refused"    # 9
CAPTCHA = "captcha"                        # 10
SOFT_BLOCK = "soft_block"                  # 11
GEO_OR_CONSENT = "geo_or_consent"          # 12
LOGIN_WALL = "login_wall"                  # 13
SOLVER_FAULT = "solver_fault"              # 14
POLICY_STOP = "policy_stop"                # 15
PROVIDER_REFUSED = "provider_refused_domain"  # 16

# Moves (what the platform does next).
RETRY_ELSEWHERE = "retry_elsewhere"
RETRY_LATER = "retry_later"
SLOW_DOWN = "slow_down"
CLIMB = "climb"
FIX_OURS = "fix_ours"
STOP = "stop"
NONE = "none"


@dataclass(frozen=True)
class ClassInfo:
    number: int
    side: str        # ours | site | owner | none
    move: str
    label: str       # a few plain words for people
    explain: str     # one sentence for people


CLASSES: Dict[str, ClassInfo] = {
    OK: ClassInfo(0, "none", NONE, "Fetched", "The page came back as asked."),
    PROXY_FAILURE: ClassInfo(1, "ours", RETRY_ELSEWHERE, "Proxy failure",
                             "The proxy could not carry the request; another exit is tried."),
    CAPACITY: ClassInfo(2, "ours", RETRY_LATER, "Our capacity",
                        "Our browsers, solvers or the time budget ran out; the request is retried."),
    ORIGIN_ERROR: ClassInfo(3, "site", RETRY_LATER, "Site error",
                            "The site answered with an error of its own; it is retried later."),
    NOT_FOUND: ClassInfo(4, "site", STOP, "Not found", "The page does not exist on the site."),
    RATE_LIMITED: ClassInfo(5, "ours", SLOW_DOWN, "Rate limited",
                            "The site is limiting how fast we fetch; the platform slows down."),
    EDGE_DENY: ClassInfo(6, "site", CLIMB, "Denied at the edge",
                         "The site's bot protection refused these exits without a challenge; "
                         "fresher or better exits are tried, then a cool-down."),
    IDENTITY_DENY: ClassInfo(7, "ours", FIX_OURS, "Denied by fingerprint",
                             "The site refuses our client identity on every exit; another "
                             "identity is tried and we are alerted."),
    CHALLENGE: ClassInfo(8, "site", CLIMB, "Bot challenge",
                         "The site served a bot challenge; a solver clears it and the clearance "
                         "is reused."),
    CLEARANCE_REFUSED: ClassInfo(9, "site", CLIMB, "Clearance refused",
                                 "A solved clearance was refused on replay; it is replayed on its "
                                 "own exit, then solved again."),
    CAPTCHA: ClassInfo(10, "site", CLIMB, "Captcha",
                       "The site asks for a captcha; a paid token is used if the policy allows."),
    SOFT_BLOCK: ClassInfo(11, "site", CLIMB, "Empty or decoy page",
                          "The page came back without its real content (a stub or decoy)."),
    GEO_OR_CONSENT: ClassInfo(12, "site", STOP, "Geo or consent wall",
                              "The site shows a region or consent wall instead of the page."),
    LOGIN_WALL: ClassInfo(13, "owner", STOP, "Sign-in required",
                          "The page needs an account; we never sign in."),
    SOLVER_FAULT: ClassInfo(14, "ours", FIX_OURS, "Solver fault",
                            "Our solver could not handle this challenge (a new script build or "
                            "a stale identity); we are alerted."),
    POLICY_STOP: ClassInfo(15, "owner", STOP, "Stopped by policy",
                           "The spider's policy (tier, paid solves or budget) stopped further "
                           "attempts."),
    PROVIDER_REFUSED: ClassInfo(16, "ours", RETRY_ELSEWHERE, "Provider refused the domain",
                                "A proxy or solver vendor does not serve this domain; the next "
                                "provider is used."),
}

# Which diagnosis best explains a request when several attempts failed: specific site-side
# findings first, then ours, generic last.
_PRECEDENCE = (
    LOGIN_WALL, GEO_OR_CONSENT, IDENTITY_DENY, CAPTCHA, CHALLENGE, CLEARANCE_REFUSED, EDGE_DENY,
    RATE_LIMITED, SOFT_BLOCK, SOLVER_FAULT, PROVIDER_REFUSED, POLICY_STOP, NOT_FOUND,
    ORIGIN_ERROR, PROXY_FAILURE, CAPACITY, OK,
)

# Vendors named in a diagnosis.
CLOUDFLARE = "cloudflare"
AKAMAI = "akamai"
DATADOME = "datadome"
KASADA = "kasada"
PERIMETERX = "perimeterx"
IMPERVA = "imperva"
AWS_WAF = "aws_waf"
AMAZON = "amazon"
GOOGLE = "google"
DDOS_GUARD = "ddos_guard"
SUCURI = "sucuri"
VERCEL = "vercel"
FASTLY = "fastly"


@dataclass(frozen=True)
class Diagnosis:
    cls: str
    vendor: str = ""
    status: int = 0
    evidence: str = ""          # the rule or marker that decided it (never content)
    confidence: str = "high"    # high | low
    retry_after: Optional[float] = None
    extra: Dict[str, Any] = field(default_factory=dict, compare=False)

    @property
    def ok(self) -> bool:
        return self.cls == OK

    @property
    def info(self) -> ClassInfo:
        return CLASSES[self.cls]

    @property
    def blocked(self) -> bool:
        """The site kept us from the page (as opposed to our side failing or the page not
        existing)."""
        return self.cls in (EDGE_DENY, IDENTITY_DENY, CHALLENGE, CLEARANCE_REFUSED, CAPTCHA,
                            SOFT_BLOCK, RATE_LIMITED)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"class": self.cls, "number": self.info.number,
                               "side": self.info.side, "move": self.info.move}
        if self.vendor:
            out["vendor"] = self.vendor
        if self.status:
            out["status"] = self.status
        if self.evidence:
            out["evidence"] = self.evidence
        if self.confidence != "high":
            out["confidence"] = self.confidence
        if self.retry_after is not None:
            out["retry_after"] = self.retry_after
        if self.extra:
            out.update(self.extra)
        return out


def most_informative(diagnoses: Iterable[Optional[Diagnosis]]) -> Optional[Diagnosis]:
    """The diagnosis that best explains a request whose attempts ended differently."""
    best: Optional[Diagnosis] = None
    for d in diagnoses:
        if d is None:
            continue
        if best is None or _PRECEDENCE.index(d.cls) < _PRECEDENCE.index(best.cls):
            best = d
    return best


# ---------------------------------------------------------------------------
# Markers
# ---------------------------------------------------------------------------

# Markers that only ever appear on a challenge page (never on a normal page carrying a tag).
_CF_CHALLENGE = ("cf_chl_opt", "_cf_chl", "cdn-cgi/challenge-platform/h/", "cf-browser-verification",
                 "challenges.cloudflare.com/cdn-cgi/challenge-platform")
_CF_TITLES = ("<title>just a moment", "<title>attention required", "<title>please wait")
_CF_ERROR_RE = re.compile(r"error(?:\s|&nbsp;)*(?:code)?[:\s#]*(10\d\d)\b", re.I)
_CF_IDENTITY_CODES = {"1010"}                       # banned by browser signature
_CF_RATE_CODES = {"1015"}                           # rate limited
_CF_DENY_CODES = {"1005", "1006", "1007", "1008", "1009", "1012", "1020", "1025"}

_DD_VAR_RE = re.compile(r"var\s+dd\s*=\s*\{[^}]*?['\"]?rt['\"]?\s*:\s*['\"](\w+)['\"]", re.I)
_DD_T_RE = re.compile(r"[{,\s]['\"]?t['\"]?\s*:\s*['\"](\w+)['\"]")
_SBSD_RE = re.compile(
    r'src="/[A-Za-z0-9/_\-.]+\?v=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    r'&(?:amp;)?t=[^"&]+"', re.I)
_AKAMAI_REFERENCE_RE = re.compile(
    r"reference(?:-id)?(?:\s|&#32;)*:?(?:\s|&#32;)*(?:#|&#35;)\d+(?:\.|&#46;)[0-9a-f]+", re.I)
# Amazon's robot check posts its form to /errors/validateCaptcha (or /errors_page/...): the path,
# not the word, which a site's own script may carry (musicstore.com's ValidateCaptchaInputs).
_AMAZON_CAPTCHA_FORM_RE = re.compile(r"/errors(?:_page)?/validatecaptcha\b", re.I)
_INTERACTIVE = (
    ("hcaptcha", ("hcaptcha.com", "h-captcha")),
    ("recaptcha", ("google.com/recaptcha", "g-recaptcha", "grecaptcha", "recaptcha/api.js")),
    ("funcaptcha", ("funcaptcha.com", "arkoselabs.com")),
    ("geetest", ("geetest.com", "gt_captcha")),
)
_GEO_PHRASES = (
    "not available in your country", "not available in your region",
    "not available in your location", "unavailable in your country", "unavailable in your region",
    "access from your country", "is not available in your area", "service is not available in",
    "geo-restricted", "geoblocked",
)
_CONSENT_PHRASES = ("before you continue to", "consent.google.", "consent.yahoo.", "consent.youtube.")
_LOGIN_PATH_RE = re.compile(
    r"/(?:log-?in|sign-?in|signin|auth|account/login|users?/sign_in|sso|oauth2?/authorize)(?:[/?#.]|$)",
    re.I)
_SOFT_DENY = ("access denied", "request blocked", "forbidden", "you have been blocked",
              "unusual traffic", "automated access", "bot detection", "verify you are human",
              "are you a robot", "not a robot", "security check", "checking your browser",
              "verifying your browser")
# Titles only a challenge, block or wall page carries (lowercased, end marks trimmed).
_CHALLENGE_TITLES = ("just a moment", "attention required", "access denied", "please wait",
                     "one moment, please", "403 forbidden", "forbidden", "security check",
                     "pardon our interruption", "access to this page has been denied",
                     "vercel security checkpoint", "verifying your browser", "checking your browser",
                     "client challenge", "robot or human")
# A JS interstitial of the site's own (a proof of work posted back, neobits.com) or a vendor's.
_JS_INTERSTITIAL = ("verifying your browser", "checking your browser", "you will be redirected shortly")
# PerimeterX's block page: its captcha container, its press-and-hold; and what names PerimeterX on a
# page titled as its block page (the container can sit past a large inline stylesheet).
_PX_BLOCK = ("px-captcha", "press & hold", "human-challenge")
_PX_NAMES = ("_pxappid", "pxcaptcha", "perimeterx", "px-cloud.net")
# Akamai Bot Manager's sensor script, as a page that has nothing else loads it: a path of four or
# more short random segments and no extension (the passive script on normal pages looks the same,
# so it only counts on a page with no content), or the akam pixel tag.
_AKAMAI_SENSOR_SRC_RE = re.compile(r'<script[^>]*\ssrc="/[\w-]+(?:/[\w-]+){3,}"', re.I)
_AKAMAI_SHELL = ("bazadebezolkohpepadr", "/akam/1")
# Fastly Next-Gen WAF's client challenge ("Client Challenge", ~3KB, served with 200): its loader inserts
# /_fs-ch-<token>/script.js. A served page of a Fastly site never names the path (mimic's
# challenge.fastly_challenge reads the same).
_FASTLY_SCRIPT_RE = re.compile(r"""["']/_fs-ch-[a-z0-9_-]+/script\.js""")
_WAF_HEADERS = ("cf-ray", "x-datadome", "x-amzn-waf-action", "x-iinfo", "x-sucuri-id", "x-kpsdk-ct",
                "x-px-", "server-timing")


def akamai_reference(text: str) -> bool:
    """Whether ``text`` carries the reference id of Akamai's edge deny ("Reference #18.x.y.z",
    "Reference-ID: #0.x...", also HTML-escaped as on the stock page)."""
    return bool(_AKAMAI_REFERENCE_RE.search((text or "")[:20000]))


def _hdr(headers: Optional[Mapping[str, Any]], name: str) -> str:
    if not headers:
        return ""
    for k, v in headers.items():
        if str(k).lower() == name:
            if isinstance(v, (list, tuple)):
                return ", ".join(str(x) for x in v)
            return str(v)
    return ""


def _header_names(headers: Optional[Mapping[str, Any]]) -> str:
    return " ".join(str(k).lower() for k in (headers or {}))


def _retry_after(headers: Optional[Mapping[str, Any]]) -> Optional[float]:
    raw = _hdr(headers, "retry-after").strip()
    if raw.isdigit():
        return float(raw)
    return None


def _title(lower: str) -> str:
    """The page's title, end marks trimmed; "" when it has none or only spaces and entities
    ("&nbsp;", Akamai's interstitial)."""
    m = re.search(r"<title[^>]*>([^<]*)</title>", lower[:8000])
    if not m:
        return ""
    text = re.sub(r"&(?:nbsp|#160|#xa0);", " ", m.group(1))
    return " ".join(text.split()).rstrip(".…! ").strip()


def _real_structure(lower: str) -> bool:
    """A real page rather than an interstitial: a title of its own, links, or layout tags. A
    challenge's own title is never a real page, whatever its layout (Imperva's block page sits in
    a <main>, Vercel's checkpoint in <main> and <footer>)."""
    head = lower[:8000]
    title = _title(head)
    if title in _CHALLENGE_TITLES:
        return False
    if title:
        if head.count("<a ") >= 3 or any(t in head for t in ("<nav", "<main", "<article",
                                                             "<footer", "<header")):
            return True
        # A titled page with substantial visible text is real too.
        text = re.sub(r"<[^>]+>", " ", lower[:40000])
        if len(" ".join(text.split())) >= 1500:
            return True
    if head.count("<a ") >= 5:
        return True
    return any(t in head for t in ("<nav", "<main", "<article"))


def _visible_text_len(lower: str) -> int:
    text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", lower[:60000], flags=re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    return len(" ".join(text.split()))


def _is_markup_fragment(stripped: str) -> bool:
    head = stripped[:2000].lower()
    return (stripped.startswith("<")
            and not any(t in head for t in ("<html", "<head", "<body", "<!doctype"))
            and "<script" not in stripped.lower())


def _vendor_hint(lower: str, headers: Optional[Mapping[str, Any]]) -> str:
    """The defence a response shows, from headers and passive tags (not proof of a block)."""
    server = _hdr(headers, "server").lower()
    names = _header_names(headers)
    cookies = _hdr(headers, "set-cookie").lower()
    if "x-kpsdk" in names or "kpsdk" in lower[:6000] or "149e9513-01fa" in lower[:6000]:
        return KASADA
    if "x-datadome" in names or "datadome=" in cookies or "captcha-delivery.com" in lower \
            or "js.datadome.co" in lower:
        return DATADOME
    if "_px" in cookies or "px-captcha" in lower or "perimeterx" in lower or "_pxappid" in lower:
        return PERIMETERX
    if "_incapsula_resource" in lower or "incap_ses" in cookies or "visid_incap" in cookies \
            or "x-iinfo" in names or "incapsula incident" in lower:
        return IMPERVA
    if "x-amzn-waf" in names or "awswaf" in lower or "aws-waf-token" in cookies:
        return AWS_WAF
    if "akamaighost" in server or "_abck=" in cookies or "bm_sz=" in cookies or "ak_bmsc=" in cookies \
            or _AKAMAI_REFERENCE_RE.search(lower[:20000]):
        return AKAMAI
    if "cloudflare" in server or "cf-ray" in names or "cf-mitigated" in names \
            or "__cf_bm" in cookies or "cf_clearance" in cookies:
        return CLOUDFLARE
    if "ddos-guard" in server or "ddos-guard" in lower[:6000]:
        return DDOS_GUARD
    if "sucuri" in server or "x-sucuri-id" in names:
        return SUCURI
    if "x-vercel-mitigated" in names or "vercel security checkpoint" in lower[:40000]:
        return VERCEL
    if "/_fs-ch-" in lower[:50000]:
        return FASTLY
    return ""


def _interactive(lower: str) -> str:
    for name, markers in _INTERACTIVE:
        if any(m in lower for m in markers):
            return name
    return ""


def _host(url: Optional[str]) -> str:
    try:
        return (urlparse(url or "").hostname or "").lower()
    except ValueError:
        return ""


def _path(url: Optional[str]) -> str:
    try:
        return urlparse(url or "").path or ""
    except ValueError:
        return ""


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------

def diagnose_response(
    status: int,
    body: Any,
    content_type: Optional[str] = None,
    headers: Optional[Mapping[str, Any]] = None,
    url: Optional[str] = None,
    final_url: Optional[str] = None,
) -> Diagnosis:
    """Classify one HTTP response (the page as the fetch layer received it)."""
    status = int(status or 0)
    if isinstance(body, (bytes, bytearray)):
        body = bytes(body).decode("utf-8", "replace")
    body = body or ""
    ctype = (content_type or _hdr(headers, "content-type") or "").lower()
    is_html = (not ctype) or "html" in ctype
    stripped = body.strip()
    block_status = status in (401, 403, 405, 429, 503)
    lower = (body if (block_status or status >= 400) else body[:65536]).lower()
    vendor = _vendor_hint(lower, headers)
    retry_after = _retry_after(headers)

    def d(cls: str, ev: str, v: Optional[str] = None, conf: str = "high") -> Diagnosis:
        return Diagnosis(cls, vendor if v is None else v, status, ev, conf,
                         retry_after if cls in (RATE_LIMITED, ORIGIN_ERROR, CAPACITY) else None)

    # The gateway names its own refusals (CONNECT-level answers that reached us as a response).
    proxy_reason = _hdr(headers, "x-calyprium-proxy-error").strip().lower()
    if proxy_reason:
        return _proxy_reason_diagnosis(proxy_reason, status)
    if status in (402, 407):
        return d(PROXY_FAILURE, f"proxy_{status}", "")

    # --- JSON / non-HTML bodies: vendors answer XHR calls with JSON blocks --------------------
    if not is_html:
        if "captcha-delivery.com" in lower[:4000]:
            return d(CHALLENGE, "datadome_json", DATADOME)
        # Cloudflare's challenge under a non-HTML type: its orchestrate script
        # (application/javascript, "window._cf_chl_opt.…") or a challenge page mislabelled.
        # No clean API, feed or script body carries these markers.
        if any(m in lower[:8000] for m in _CF_CHALLENGE) \
                or "challenge" in _hdr(headers, "cf-mitigated").lower():
            return d(CHALLENGE, "cloudflare_challenge", CLOUDFLARE)
        if ("blockscript" in lower[:4000] or "\"appid\"" in lower[:4000]) and "px" in lower[:4000]:
            return d(CHALLENGE, "perimeterx_json", PERIMETERX)
        if status == 429 and ("x-kpsdk" in _header_names(headers)):
            return d(CHALLENGE, "kasada_header", KASADA)

    # --- Challenge pages (any status) ---------------------------------------------------------
    if is_html or block_status:
        if any(m in lower for m in _CF_CHALLENGE) or "challenge" in _hdr(headers, "cf-mitigated").lower() \
                or (any(t in lower[:3000] for t in _CF_TITLES) and "challenge-platform" in lower):
            return d(CHALLENGE, "cloudflare_challenge", CLOUDFLARE)
        m = _DD_VAR_RE.search(lower[:20000])
        if m or "geo.captcha-delivery.com" in lower or "ct.captcha-delivery.com" in lower:
            t = _DD_T_RE.search(lower[m.start():m.start() + 400]) if m else None
            if t and t.group(1) == "bv":
                return d(EDGE_DENY, "datadome_banned", DATADOME)
            return d(CHALLENGE, "datadome_challenge", DATADOME)
        title = _title(lower)
        if any(p in lower for p in _PX_BLOCK) or (
                title == "access to this page has been denied" and any(p in lower for p in _PX_NAMES)):
            return d(CHALLENGE, "perimeterx_challenge", PERIMETERX)
        # Vercel's checkpoint (its Firewall's challenge action) answers 429: a challenge, not a rate limit.
        if "vercel security checkpoint" in lower[:40000] or "challenge" in _hdr(headers, "x-vercel-mitigated").lower():
            return d(CHALLENGE, "vercel_checkpoint", VERCEL)
        if len(body) < 50000 and _FASTLY_SCRIPT_RE.search(lower):
            return d(CHALLENGE, "fastly_challenge", FASTLY)
        if status in (403, 429) and (
                "ips.js" in lower[:6000] or "kpsdk" in lower[:6000] or "149e9513-01fa" in lower[:6000]
                or "x-kpsdk" in _header_names(headers)):
            return d(CHALLENGE, "kasada_challenge", KASADA)
        # Akamai's interstitial: a proof of work in the page, posted to /_sec/verify with its bm-verify token.
        if "bm-verify" in lower[:20000] and "/_sec/verify" in lower[:20000]:
            return d(CHALLENGE, "akamai_interstitial", AKAMAI)
        if len(body) < 20000 and _SBSD_RE.search(body):
            return d(CHALLENGE, "akamai_sbsd", AKAMAI)
        if "_incapsula_resource" in lower[:6000] and "swjiylwa" in lower[:6000]:
            return d(CHALLENGE, "imperva_utmvc", IMPERVA)
        if "reese84" in lower[:20000] and block_status:
            return d(CHALLENGE, "imperva_reese84", IMPERVA)
        # Imperva's block page and reese84 interstitial ("Pardon Our Interruption"), served with 200.
        if "pardon our interruption" in lower[:20000]:
            return d(CHALLENGE, "imperva_interstitial", IMPERVA)
        # window.gokuProps is only ever on AWS WAF's interstitial (a normal page's SDK, jsapi.js, never sets it), so it
        # is a challenge whatever the page around it looks like; the SDK markers alone need a page with no structure,
        # or AWS WAF's own challenge status (202; booking.com loads challenge.js from a first-party path)
        if "gokuprops" in lower or (("awswafintegration" in lower or "token.awswaf.com" in lower)
                                    and not _real_structure(lower)) or (status == 202 and "awswaf" in lower):
            return d(CHALLENGE, "aws_waf_challenge", AWS_WAF)
        if "enter the characters you see below" in lower or "to discuss automated access" in lower \
                or _AMAZON_CAPTCHA_FORM_RE.search(lower):
            return d(CAPTCHA, "amazon_robot_check", AMAZON)
        if "/sorry/index" in (final_url or "").lower() or ("our systems have detected unusual traffic"
                                                          in lower and "google" in lower):
            return d(CAPTCHA, "google_sorry", GOOGLE)
        if status in (403, 429, 503) and ("bmak" in lower or "_abck" in lower) \
                and not _AKAMAI_REFERENCE_RE.search(lower[:20000]):
            return d(CHALLENGE, "akamai_sensor", AKAMAI)
        widget = _interactive(lower)
        if widget and (block_status or not _real_structure(lower)):
            return d(CAPTCHA, f"{widget}_gate")
        if status < 400 and len(body) < 20000 and not _real_structure(lower):
            shown = _visible_text_len(lower)
            # A page whose only content is Akamai's sensor script (its pixel tag, or a lone script on a
            # random extensionless path): the sensor is the challenge.
            if shown < 200 and (any(m in lower for m in _AKAMAI_SHELL) or _AKAMAI_SENSOR_SRC_RE.search(lower)):
                return d(CHALLENGE, "akamai_sensor_shell", AKAMAI)
            # A JS interstitial: the page runs a script (a proof of work, a check) and reloads.
            if shown < 600 and "<script" in lower and any(p in lower for p in _JS_INTERSTITIAL):
                return d(CHALLENGE, "js_interstitial")

    # --- Walls -------------------------------------------------------------------------------
    if status == 451:
        return d(GEO_OR_CONSENT, "status_451")
    fpath = _path(final_url)
    if final_url and url and _LOGIN_PATH_RE.search(fpath) and not _LOGIN_PATH_RE.search(_path(url)):
        return d(LOGIN_WALL, "redirect_to_sign_in")
    if status == 401:
        return d(LOGIN_WALL, "status_401", conf="low" if vendor else "high")
    # A wall reads the same in any layout: office depot's geo wall is a 200 "Site Maintenance" page with
    # its own header, <main> and footer, so a small page with little text is read too.
    if is_html and stripped and (not _real_structure(lower)
                                 or (len(body) < 20000 and _visible_text_len(lower) < 1500)):
        if any(p in lower for p in _GEO_PHRASES):
            return d(GEO_OR_CONSENT, "geo_wall")
        if any(p in lower for p in _CONSENT_PHRASES) or _host(final_url).startswith("consent."):
            return d(GEO_OR_CONSENT, "consent_wall")
    elif _host(final_url).startswith("consent."):
        return d(GEO_OR_CONSENT, "consent_wall")

    # --- Status-led classes ------------------------------------------------------------------
    if status == 403 or status == 503 and is_html and any(s in lower for s in _SOFT_DENY):
        cf = _CF_ERROR_RE.search(lower[:20000]) if vendor == CLOUDFLARE or "cloudflare" in lower[:20000] \
            else None
        if cf:
            code = cf.group(1)
            if code in _CF_IDENTITY_CODES:
                return d(IDENTITY_DENY, f"cloudflare_{code}", CLOUDFLARE)
            if code in _CF_RATE_CODES:
                return d(RATE_LIMITED, f"cloudflare_{code}", CLOUDFLARE)
            if code in _CF_DENY_CODES:
                return d(EDGE_DENY, f"cloudflare_{code}", CLOUDFLARE)
        if _AKAMAI_REFERENCE_RE.search(lower[:20000]) or (vendor == AKAMAI and len(body) < 20000):
            return d(EDGE_DENY, "akamai_deny", AKAMAI)
        if "incapsula incident" in lower or "_incapsula_resource" in lower:
            return d(EDGE_DENY, "imperva_incident", IMPERVA)
        if vendor == AWS_WAF:
            return d(EDGE_DENY, "aws_waf_deny", AWS_WAF)
        if not is_html:
            waf = vendor or any(h in _header_names(headers) for h in _WAF_HEADERS)
            if waf:
                return d(EDGE_DENY, "waf_json_403", conf="low")
            return d(LOGIN_WALL, "api_403", conf="low")
        if len(body) < 20000 or any(s in lower for s in _SOFT_DENY) or not _real_structure(lower):
            return d(EDGE_DENY, "deny_page", conf="high" if vendor else "low")
        return d(EDGE_DENY, "large_403", conf="low")
    if status == 429:
        return d(RATE_LIMITED, "status_429")
    if status == 999:  # LinkedIn's "request denied": its answer to too many requests from an address
        return d(RATE_LIMITED, "status_999")
    if status in (404, 410):
        return d(NOT_FOUND, f"status_{status}")
    if status == 503:
        if retry_after is not None:
            return d(RATE_LIMITED, "503_retry_after")
        if is_html and len(body) < 20000 and vendor and not _real_structure(lower):
            return d(EDGE_DENY, "vendor_503", conf="low")
        return d(ORIGIN_ERROR, "status_503")
    if 520 <= status <= 530:
        return d(ORIGIN_ERROR, f"cloudflare_{status}", CLOUDFLARE)
    if status >= 500:
        return d(ORIGIN_ERROR, f"status_{status}")
    if status == 405 and vendor == AWS_WAF:
        return d(CHALLENGE, "aws_waf_405", AWS_WAF)
    if status >= 400:
        return d(ORIGIN_ERROR, f"status_{status}", conf="low")

    # --- 2xx / 3xx: real page, stub or decoy --------------------------------------------------
    if status == 204 or not is_html:
        return d(OK, "")
    if not stripped:
        return d(SOFT_BLOCK, "empty_body", conf="low")
    if stripped.startswith(("<?xml", "<urlset", "<sitemapindex", "{", "[")):
        return d(OK, "")
    # A bare answer with no markup at all ("True", "OK", a token, a count) under an HTML type: what a form's
    # verify call or a small endpoint returns (tncab.tnsos.gov's captcha verify answers "True"). No vendor's
    # challenge or block page comes without markup.
    if "<" not in stripped and len(stripped) < 2000 and not any(s in lower for s in _SOFT_DENY):
        return d(OK, "text_answer")
    # Akamai's edge deny served with 200 ("Something went wrong" and kin, with its reference id).
    if len(body) < 20000 and akamai_reference(lower) and (not _real_structure(lower)
                                                         or _visible_text_len(lower) < 1500):
        return d(EDGE_DENY, "akamai_deny", AKAMAI)
    if len(body) < 10000 and not _real_structure(lower):
        if _is_markup_fragment(stripped):
            return d(OK, "")
        if any(s in lower for s in _SOFT_DENY) or vendor:
            return d(SOFT_BLOCK, "stub_page")
        if _visible_text_len(lower) < 300:
            return d(SOFT_BLOCK, "stub_page", conf="low")
    return d(OK, "")


def _proxy_reason_diagnosis(reason: str, status: int = 0) -> Diagnosis:
    # A vendor refusing the domain, or holding a new domain to its daily ramp: the next
    # provider serves it (the gateway's X-Calyprium-Proxy-Error reasons).
    if "refused_domain" in reason or "ramp_limit" in reason:
        return Diagnosis(PROVIDER_REFUSED, "", status, reason)
    if "budget" in reason or "policy" in reason:
        return Diagnosis(POLICY_STOP, "", status, reason)
    return Diagnosis(PROXY_FAILURE, "", status, reason)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

_DEADLINE = ("deadlineexceeded", "deadline exceeded", "deadline spent")
_CAPACITY = ("engine unavailable", "unavailable. available", "no policy-allowed engine",
             "worker queue", "queue full", "too many requests to worker", "circuit open",
             "native solver busy", "no worker")
_PROXY = ("proxy", "connect tunnel", "tunnel connection", "407", "402", "bad gateway",
          "connection reset", "connection aborted", "remote end closed", "eof occurred",
          "ssl", "tls", "handshake", "timed out", "timeout", "connection refused",
          "unexpected eof", "socks", "curl: (5", "curl: (7", "curl: (28", "curl: (35",
          "curl: (56", "proxyerror")
_DNS = ("name or service not known", "nodename nor servname", "no such host",
        "getaddrinfo failed", "nxdomain", "could not resolve host", "temporary failure in name")


# The gateway refusing a destination for every provider the request may use, as a client relays it: httpcloak
# quotes the CONNECT reply ("403 Forbidden - destination blocked by proxy provider policy", connection.py's
# ProviderRefused), camoufox's Page.goto names a proxy that answered the CONNECT with 403.
_PROVIDER_POLICY = ("blocked by proxy provider policy", "ns_error_proxy_forbidden")


def diagnose_error(error: Any, gateway_reason: Optional[str] = None) -> Diagnosis:
    """Classify a transport-level failure (no HTTP response from the site)."""
    if gateway_reason:
        return _proxy_reason_diagnosis(gateway_reason.strip().lower())
    name = type(error).__name__.lower() if isinstance(error, BaseException) else ""
    msg = f"{name} {error}".lower()
    if "x-calyprium-proxy-error" in msg:
        m = re.search(r"x-calyprium-proxy-error[\"':=\s]+([a-z_0-9]+)", msg)
        if m:
            return _proxy_reason_diagnosis(m.group(1))
    if any(s in msg for s in _PROVIDER_POLICY):
        return Diagnosis(PROVIDER_REFUSED, "", 0, "provider_refused_domain")
    if any(s in msg for s in _DEADLINE):
        return Diagnosis(CAPACITY, "", 0, "deadline")
    if any(s in msg for s in _CAPACITY):
        return Diagnosis(CAPACITY, "", 0, "capacity")
    if any(s in msg for s in _DNS):
        return Diagnosis(ORIGIN_ERROR, "", 0, "dns")
    if "refused_domain" in msg or "refused the domain" in msg:
        return Diagnosis(PROVIDER_REFUSED, "", 0, "provider_refused_domain")
    if "ramp_limit" in msg:
        return Diagnosis(PROVIDER_REFUSED, "", 0, "ramp_limit")
    if "timed out" in msg or "timeout" in msg:
        return Diagnosis(PROXY_FAILURE, "", 0, "timeout")
    if any(s in msg for s in _PROXY):
        return Diagnosis(PROXY_FAILURE, "", 0, "transport")
    return Diagnosis(PROXY_FAILURE, "", 0, "transport", confidence="low")


# ---------------------------------------------------------------------------
# Solver outcomes (Tessera's failure_class vocabulary)
# ---------------------------------------------------------------------------

_SOLVER_MAP = {
    "edge_deny": EDGE_DENY,
    "not_challenged": OK,
    "challenge_unsolved": CHALLENGE,
    "clearance_refused": CLEARANCE_REFUSED,
    "solver_fault": SOLVER_FAULT,
    "proxy_failure": PROXY_FAILURE,
    "provider_refused_domain": PROVIDER_REFUSED,
    "deadline": CAPACITY,
    "busy": CAPACITY,
}


def from_solver(failure_class: Optional[str], vendor: str = "",
                skipped_reasons: Iterable[str] = ()) -> Diagnosis:
    """A plan class for a solver's failure class (``no_eligible_solver`` is a provider gap
    when allowlists skipped every solver, else a solver gap)."""
    fc = (failure_class or "").strip().lower()
    if fc == "no_eligible_solver":
        reasons = set(skipped_reasons)
        if "paid_cap" in reasons:
            return Diagnosis(POLICY_STOP, vendor, 0, "paid_cap")
        if reasons & {"not_allowlisted", "blocked_domain"}:
            return Diagnosis(PROVIDER_REFUSED, vendor, 0, "no_solver_serves_domain")
        return Diagnosis(SOLVER_FAULT, vendor, 0, "no_solver_for_challenge")
    cls = _SOLVER_MAP.get(fc)
    if cls is None:
        return Diagnosis(SOLVER_FAULT, vendor, 0, f"solver:{fc or 'unknown'}", confidence="low")
    ev = {CHALLENGE: "solver_rejected", CAPACITY: fc}.get(cls, fc)
    return Diagnosis(cls, vendor, 0, ev)
