"""Browser-made form submits through Mimic (``POST /api/browser/submit``).

A request with ``meta["mimic_browser_submit"]`` is not downloaded. Mimic loads its page in a real
browser on the run's exit, sets the form's fields and clicks the control that submits, so the
page's own script makes the request with whatever it adds to it (F5 BIG-IP's bot defence refuses
a search POST that its script in the page did not see, wyobiz.wyo.gov 2026-10-26). The spider gets
the rendered page after the submit as a normal response. One browser page is kept per run (or per
``session_key``): a follow-up postback (``navigate="never"``: the next result page, a row) acts on
the page the last submit left.

    yield scrapy.FormRequest(url, formdata=fields, callback=self.parse_results,
                             meta={"mimic_browser_submit": {"submit": "ctl00$MainContent$cmdSearch"}})
    yield scrapy.Request(url, callback=self.parse_results, dont_filter=True,
                         meta={"mimic_browser_submit": {"submit": "ctl00$MainContent$lbtnNextHeader",
                                                        "navigate": "never"}})

Options (``meta["mimic_browser_submit"]``: ``True`` or a dict):
    fields          form fields by name; default: a form-encoded POST body (the page's own state
                    fields such as __VIEWSTATE are left to the page, a non-empty __EVENTTARGET names
                    the postback, a submit button's name names the button)
    submit          the control that submits: a submit button's name or a postback event target
    submit_argument __EVENTARGUMENT for a postback target
    click           a CSS selector to click instead
    navigate        auto (load the page unless the browser is on it) | always | never (follow-up)
    url             the page to load (default: the request's URL)
    wait_for        a CSS selector the page must hold once the submit is done
    session_key     one browser page per key (default: the spider's name and run)
    engine, proxy_type, proxy_country, settle_ms, type_delay_ms, timeout (s), max_submits,
    max_age_s, probe_on_stall, return_cookies (the browser's cookies for the site, kept in
    ``meta["mimic_submit_cookies"]``; off by default)

Settings: MIMIC_SUBMIT_ENGINE (camoufox), MIMIC_SUBMIT_PROXY_TYPE, MIMIC_SUBMIT_TIMEOUT (120 s),
MIMIC_SUBMIT_MAX_SUBMITS, MIMIC_SUBMIT_MAX_AGE (the session's recycling; Mimic caps both). The exit
country is the run's (VEIL_COUNTRY / MIMIC_PROXY_COUNTRY).

``meta["mimic_submit"]`` says how it went: session_key, session_id, new_session, session_submits,
navigated, trigger (click | postback), outcome (async | navigation | not_applied | dropped | stalled
| no_postback | blocked) and session_closed. Mimic fails closed: a captcha or challenge page comes
back as it is (its session closed), a submit the page did not apply comes back empty, and the
middleware never re-sends a browser submit (the page may have posted it); the spider decides.
"""
from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import parse_qsl

META = "mimic_browser_submit"
INFO = "mimic_submit"
COOKIES = "mimic_submit_cookies"
PATH = "/api/browser/submit"
RELEASE_PATH = "/api/browser/submit/release"

_PASSTHROUGH = ("submit", "submit_argument", "click", "wait_for", "settle_ms", "type_delay_ms", "probe_on_stall",
                "max_age_s")
_INFO_KEYS = ("session_key", "session_id", "new_session", "session_submits", "navigated", "trigger", "outcome",
              "session_closed", "fetch_time_ms")


def options(request: Any) -> Optional[Dict[str, Any]]:
    """The request's browser-submit options, or None when it is not one."""
    value = (getattr(request, "meta", None) or {}).get(META)
    if not value:
        return None
    return dict(value) if isinstance(value, dict) else {}


def form_fields(request: Any) -> Dict[str, str]:
    """A form-encoded POST body's fields (the last value of a repeated name)."""
    if (getattr(request, "method", "GET") or "GET").upper() != "POST" or not request.body:
        return {}
    ctype = request.headers.get("Content-Type") or b""
    if isinstance(ctype, bytes):
        ctype = ctype.decode("latin-1", "ignore")
    if ctype and "x-www-form-urlencoded" not in ctype.lower():
        return {}
    encoding = getattr(request, "encoding", None) or "utf-8"
    return dict(parse_qsl(request.body.decode(encoding, "replace"), keep_blank_values=True))


def payload(request: Any, opts: Dict[str, Any], settings: Any, policy: Any, default_key: str) -> Dict[str, Any]:
    """The ``/api/browser/submit`` body for ``request``."""
    def setting(name: str, default: Any = None) -> Any:
        try:
            value = settings.get(name)
        except AttributeError:
            value = None
        return default if value in (None, "") else value

    engine = opts.get("engine") or setting("MIMIC_SUBMIT_ENGINE", "camoufox")
    if policy is not None:
        engine = policy.clamp_engine(engine) or engine
    timeout_s = float(opts.get("timeout") or setting("MIMIC_SUBMIT_TIMEOUT", 120))
    body: Dict[str, Any] = {
        "url": opts.get("url") or request.url,
        "fields": opts["fields"] if "fields" in opts else form_fields(request),
        "navigate": opts.get("navigate") or "auto",
        "session_key": str(opts.get("session_key") or default_key)[:200],
        "browser_engine": engine,
        "proxy_type": opts.get("proxy_type") or setting("MIMIC_SUBMIT_PROXY_TYPE"),
        "proxy_country": opts.get("proxy_country") or getattr(policy, "country", None),
        "timeout_ms": int(min(240.0, max(10.0, timeout_s)) * 1000),
        "return_cookies": bool(opts.get("return_cookies", False)),
    }
    max_submits = opts.get("max_submits") or setting("MIMIC_SUBMIT_MAX_SUBMITS")
    if max_submits:
        body["max_submits"] = int(max_submits)
    max_age = opts.get("max_age_s") or setting("MIMIC_SUBMIT_MAX_AGE")
    if max_age:
        body["max_age_s"] = int(max_age)
    for key in _PASSTHROUGH:
        if key in opts and opts[key] is not None and key not in body:
            body[key] = opts[key]
    if policy is not None:
        for key, value in policy.mimic_session_fields().items():
            body.setdefault(key, value)
    return {k: v for k, v in body.items() if v is not None}


def info(data: Dict[str, Any]) -> Dict[str, Any]:
    """How a submit went, for ``meta["mimic_submit"]`` (never cookies)."""
    return {k: data.get(k) for k in _INFO_KEYS if k in data}
