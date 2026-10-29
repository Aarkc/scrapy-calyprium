"""Local TLS-fingerprinted HTTP fetch for scrapy-calyprium spiders.

This is the spider-side replacement for round-tripping every request through
Mimic's `/api/fetch` endpoint. It performs the actual HTTP call inside the
spider process using either the `httpcloak` Rust extension (preferred) or
`curl_cffi` as a fallback for platforms without the Rust wheel.

Key correctness property: **the response body is preserved as raw bytes**.
The Mimic-side `/api/fetch` endpoint serializes the body as a JSON string,
which silently corrupts any non-text content (PDFs, images, fonts, archives)
because JSON strings must be valid UTF-8. See AAR-12.

Usage:

    fetcher = LocalFetcher(default_preset="chrome-143", timeout=30)
    result = await fetcher.fetch(
        url="https://example.com/file.pdf",
        proxy_url="http://user:pass@proxy.example:8080",
        cookies=[{"name": "cf_clearance", "value": "..."}],
        user_agent="Mozilla/5.0 ...",
    )
    if result.status_code == 200:
        with open("file.pdf", "wb") as f:
            f.write(result.body)  # raw bytes — never decoded

Block detection is delegated to `block_detect.is_blocked` so the spider can
escalate to a Mimic browser solve when the local fetch returns a challenge.

AAR-15.
"""
from __future__ import annotations

import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

from scrapy_calyprium.routing.block_detect import is_blocked
from scrapy_calyprium.routing.chrome_preset import replay_preset

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Backend availability detection
# ---------------------------------------------------------------------------


def _try_import_httpcloak():
    try:
        import httpcloak  # type: ignore

        return httpcloak
    except Exception:
        return None


def _try_import_curl_cffi():
    try:
        from curl_cffi.requests import AsyncSession  # type: ignore

        return AsyncSession
    except Exception:
        return None


_HTTPCLOAK = _try_import_httpcloak()
_CURL_CFFI_ASYNC = _try_import_curl_cffi()


def is_local_fetch_available() -> bool:
    """Return True if any local fetch backend is importable."""
    return _HTTPCLOAK is not None or _CURL_CFFI_ASYNC is not None


def available_backends() -> List[str]:
    backends = []
    if _HTTPCLOAK is not None:
        backends.append("httpcloak")
    if _CURL_CFFI_ASYNC is not None:
        backends.append("curl_cffi")
    return backends


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass
class LocalFetchResult:
    """The result of a local HTTP fetch.

    `body` is always raw bytes — callers that want text should decode using
    `headers.get('content-type')` to pick an encoding.
    """

    url: str
    final_url: str
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    elapsed_ms: int = 0
    backend: str = ""
    # Every Set-Cookie value the site sent, one per cookie (``headers`` folds a repeated header
    # into one value, which no cookie parser can split again).
    set_cookies: List[str] = field(default_factory=list)

    @property
    def content_type(self) -> str:
        return (self.headers.get("content-type") or self.headers.get("Content-Type") or "").lower()

    def text(self, errors: str = "replace") -> str:
        """Decode body as text. Use only when you know the response is text."""
        encoding = "utf-8"
        ct = self.content_type
        if "charset=" in ct:
            encoding = ct.split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
        return self.body.decode(encoding, errors=errors)


class LocalFetchError(Exception):
    """Raised when the local fetch backend cannot be used at all. ``proxy_error`` is the gateway's
    X-Calyprium-Proxy-Error when it refused the CONNECT and the backend kept its reply (``pool_busy``,
    ``refused_domain``, ...)."""

    def __init__(self, *args, proxy_error: Optional[str] = None):
        super().__init__(*args)
        self.proxy_error = proxy_error


#: httpcloak's error for a CONNECT the proxy refused, with the gateway's reason in the status phrase
_HTTPCLOAK_REFUSAL_RE = re.compile(r"proxy CONNECT failed: \d{3}[^()]*\(([A-Za-z0-9_]+)\)")


def _fetch_failed(url: str, exc: Exception) -> "LocalFetchError":
    """The error for a fetch that failed. A refused CONNECT names the gateway's reason, which the
    error text lacks (the shared classifier reads "X-Calyprium-Proxy-Error: <reason>" from it)."""
    reason, phrase = _proxy_refusal(exc)
    detail = f" (X-Calyprium-Proxy-Error: {reason}; {phrase or 'no status phrase'})" if reason else ""
    return LocalFetchError(f"curl_cffi fetch failed for {url}: {exc}{detail}", proxy_error=reason or None)


def _proxy_refusal(exc: BaseException) -> Tuple[str, str]:
    """The gateway's X-Calyprium-Proxy-Error and status phrase from a refused CONNECT, when the backend
    kept the reply: curl_cffi's error says only "CONNECT tunnel failed, response 503" and carries the
    proxy's reply as ``exc.response``. httpcloak puts the status line in its error text instead."""
    response = getattr(exc, "response", None)
    if response is None:
        return "", ""
    reason = ""
    headers = getattr(response, "headers", None)
    try:
        reason = str((headers.get("x-calyprium-proxy-error") if headers is not None else "") or "")
    except Exception:  # noqa: BLE001 -- odd header containers: no reason
        reason = ""
    phrase = str(getattr(response, "reason", "") or "")
    return reason.strip().lower(), phrase.strip()


# ---------------------------------------------------------------------------
# Proxy-session URL injection
# ---------------------------------------------------------------------------


def _inject_proxy_session(
    proxy_url: str,
    proxy_session_id: str,
    provider: Optional[str] = None,
    country: Optional[str] = None,
) -> str:
    """Inject sticky proxy session, provider and country into the proxy URL username.

    Encodes the provider (e.g. webshare_rotating), the exit country and the
    session id into the gateway proxy username, as the solve dialled them
    (``-p_<provider>[-country_<cc>]-session_<id>-strict_1``), so the Veil gateway routes
    through the same upstream and IP that earned the clearance. The solve's
    country replaces the run's (``-country_<cc>`` from the policy): one exit.
    ``strict_1`` (gateway 1.173.24+): the session never moves to another IP, since a
    clearance is bound to the address that earned it (an older gateway ignores it).
    """
    parsed = urlparse(proxy_url)
    if not parsed.username:
        return proxy_url
    parts = [parsed.username]
    if country:
        base, *params = parsed.username.split("-")
        parts = [base, *(p for p in params if not p.startswith(("country_", "c_")))]
    if provider:
        parts.append(f"p_{provider}")
    if country:
        parts.append(f"country_{country}")
    parts.append(f"session_{proxy_session_id}")
    if "strict_1" not in parts:
        parts.append("strict_1")
    new_user = "-".join(parts)
    netloc = f"{new_user}:{parsed.password}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(
        (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
    )


# ---------------------------------------------------------------------------
# Firefox ClientHellos without the finite-field groups
# ---------------------------------------------------------------------------

#: The groups of curl_cffi's Firefox ClientHello (every firefox* target: X25519MLKEM768, X25519,
#: P-256, P-384, P-521, ffdhe2048, ffdhe3072) without the two finite-field Diffie-Hellman groups.
#: curl_cffi's BoringSSL lists ffdhe2048 and ffdhe3072 so its Firefox fingerprint matches Firefox's,
#: but cannot compute either. A server that prefers one (wyobiz.wyo.gov's F5 BIG-IP answers any
#: ClientHello listing ffdhe3072 with a HelloRetryRequest for it) ends the handshake: curl error 35,
#: SSL_ERROR_SYSCALL, on every exit. Firefox itself (NSS) and OpenSSL complete it; Chrome and Safari
#: never list those groups. Without them the ClientHello is otherwise the same (its ciphers,
#: extensions and key shares; the JA4, which leaves the groups out, is unchanged).
FIREFOX_CURVES_NO_FFDHE = "X25519MLKEM768:X25519:P-256:P-384:P-521"


def _tls_handshake_failed(exc: BaseException) -> bool:
    """Whether a curl_cffi error is a failed TLS handshake (curl error 35, CURLE_SSL_CONNECT_ERROR)."""
    return "curl: (35)" in str(exc)


def _no_ffdhe_options() -> Optional[Dict]:
    """curl options presenting a Firefox ClientHello without the finite-field groups, or None when
    this curl_cffi has no SSL_EC_CURVES option."""
    try:
        from curl_cffi.const import CurlOpt  # type: ignore
    except Exception:  # noqa: BLE001
        return None
    option = getattr(CurlOpt, "SSL_EC_CURVES", None)
    return {option: FIREFOX_CURVES_NO_FFDHE} if option is not None else None


def _build_cookie_header(cookies: List[Dict]) -> str:
    return "; ".join(
        f"{c['name']}={c['value']}" for c in cookies if c.get("name") and "value" in c
    )


# ---------------------------------------------------------------------------
# A request's own headers
# ---------------------------------------------------------------------------

#: Headers the fetcher owns, never taken from ``extra_headers``: the browser identity the TLS and
#: HTTP/2 fingerprint belongs to (a cookie slot's User-Agent, the preset's client hints, the
#: codings the backend decodes), the connection itself (forbidden on HTTP/2), and the proxy
#: credentials, which are the gateway's and never the site's.
_OWNED_HEADERS = frozenset({
    "user-agent", "accept-encoding",
    "host", "connection", "keep-alive", "proxy-connection", "transfer-encoding", "te",
    "trailer", "upgrade", "http2-settings", "content-length",
    "proxy-authorization", "proxy-authenticate",
})
_OWNED_PREFIXES = ("sec-ch-ua", "x-veil-")


def _request_headers(extra_headers: Optional[Dict[str, str]]) -> Tuple[Dict[str, str], str]:
    """``extra_headers`` the fetch may send, and the request's own Cookie header (merged with
    the slot's cookies rather than replacing them). Both backends put a name the preset knows
    (Accept, Referer, ...) where the browser would and append the others, so the preset's
    header order holds."""
    own: Dict[str, str] = {}
    cookie = ""
    for name, value in (extra_headers or {}).items():
        key = str(name).lower()
        if value is None:
            continue
        if key == "cookie":
            cookie = str(value)
        elif key not in _OWNED_HEADERS and not key.startswith(_OWNED_PREFIXES):
            own[str(name)] = str(value)
    return own, cookie


def _cookie_pairs(header: str) -> List[Dict]:
    pairs = []
    for part in header.split(";"):
        name, sep, value = part.strip().partition("=")
        if sep and name.strip():
            pairs.append({"name": name.strip(), "value": value.strip()})
    return pairs


def _merge_cookies(cookies: Optional[List[Dict]], header: str) -> List[Dict]:
    """The slot's cookies, then the request's own (a name the slot has keeps the slot's value:
    the clearance is what the replay is for)."""
    merged = [c for c in (cookies or []) if c.get("name") and "value" in c]
    names = {c["name"] for c in merged}
    merged.extend(c for c in _cookie_pairs(header) if c["name"] not in names)
    return merged


def join_cookie_headers(first: str, second: str) -> str:
    """One Cookie header from two: ``first``'s cookies, then those of ``second`` whose names
    ``first`` lacks."""
    return _build_cookie_header(_merge_cookies(_cookie_pairs(first or ""), second or ""))


def _set_cookie_values(raw) -> List[str]:
    """Every Set-Cookie value of a backend's response headers, one per cookie: httpcloak hands a
    list per name, curl_cffi a multi-dict (whose plain items() keeps one value per name)."""
    get_list = getattr(raw, "get_list", None)
    if callable(get_list):
        try:
            return [str(v) for v in (get_list("set-cookie") or [])]
        except Exception:  # noqa: BLE001 — fall back to reading the items
            pass
    items = raw.multi_items() if hasattr(raw, "multi_items") else (
        raw.items() if hasattr(raw, "items") else ())
    out: List[str] = []
    for k, v in items:
        if str(k).lower() == "set-cookie":
            out.extend(str(x) for x in v) if isinstance(v, (list, tuple)) else out.append(str(v))
    return out


# ---------------------------------------------------------------------------
# LocalFetcher
# ---------------------------------------------------------------------------


class LocalFetcher:
    """Async TLS-fingerprinted HTTP fetcher for spider-side use.

    Picks the best available backend at construction time. httpcloak is
    preferred (faster, JA3/JA4 fingerprinting); curl_cffi is the fallback for
    macOS/Windows where the httpcloak wheel may not be available.
    """

    def __init__(
        self,
        default_preset: str = "chrome-143",
        timeout: int = 30,
        backend: Optional[str] = None,
        fetch_concurrency: int = 128,
    ):
        self.default_preset = default_preset
        self.timeout = timeout
        # httpcloak is a synchronous library, so each fetch is dispatched to a
        # thread pool. asyncio.to_thread() shares Python's *default* executor,
        # capped at min(32, cpu+4) workers — which silently throttles local
        # throughput to ~32 in-flight fetches regardless of the cookie-pool
        # size or CONCURRENT_REQUESTS. Give the httpcloak backend its own,
        # larger pool so the cookie pool (N slots x RPM cap) is the real limit.
        self._fetch_concurrency = max(1, int(fetch_concurrency))
        self._executor: Optional[ThreadPoolExecutor] = None
        # Hosts that end a Firefox handshake listing the finite-field groups: their curl_cffi
        # Firefox fetches leave those groups out (see FIREFOX_CURVES_NO_FFDHE).
        self.no_ffdhe_hosts: set = set()

        if backend == "httpcloak":
            if _HTTPCLOAK is None:
                raise LocalFetchError(
                    "httpcloak backend requested but the package isn't installed. "
                    "Install scrapy-calyprium[local] on a supported platform."
                )
            self.backend = "httpcloak"
        elif backend == "curl_cffi":
            if _CURL_CFFI_ASYNC is None:
                raise LocalFetchError(
                    "curl_cffi backend requested but the package isn't installed. "
                    "Install scrapy-calyprium[local]."
                )
            self.backend = "curl_cffi"
        elif backend is None:
            if _HTTPCLOAK is not None:
                self.backend = "httpcloak"
            elif _CURL_CFFI_ASYNC is not None:
                self.backend = "curl_cffi"
            else:
                raise LocalFetchError(
                    "No local fetch backend available. "
                    "Install scrapy-calyprium[local] (httpcloak on Linux, curl_cffi everywhere)."
                )
        else:
            raise LocalFetchError(f"Unknown backend: {backend!r}")

        # The httpcloak backend is synchronous; give it a dedicated thread
        # pool so concurrency isn't silently capped at the default executor's
        # ~32 workers. curl_cffi uses a native AsyncSession (no thread pool).
        if self.backend == "httpcloak":
            self._executor = ThreadPoolExecutor(
                max_workers=self._fetch_concurrency,
                thread_name_prefix="local-fetch",
            )

        logger.info(
            "LocalFetcher initialized with backend=%s (fetch_concurrency=%s)",
            self.backend,
            self._fetch_concurrency if self.backend == "httpcloak" else "n/a",
        )

    async def fetch(
        self,
        url: str,
        *,
        cookies: Optional[List[Dict]] = None,
        user_agent: Optional[str] = None,
        proxy_url: Optional[str] = None,
        proxy_session_id: Optional[str] = None,
        provider: Optional[str] = None,
        preset: Optional[str] = None,
        timeout: Optional[int] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        method: str = "GET",
        body: Optional[bytes] = None,
        country: Optional[str] = None,
    ) -> LocalFetchResult:
        """Perform a single HTTP request (a GET unless ``method`` says otherwise) and
        return raw bytes.

        Args:
            url: target URL
            cookies: clearance cookies to inject. Must be a list of
                ``{"name": ..., "value": ...}`` dicts.
            user_agent: User-Agent header to send. Should match the UA that
                earned the cookies, or Cloudflare will reject them.
            proxy_url: HTTP proxy URL with credentials embedded.
            proxy_session_id: optional sticky-session id, injected into the
                proxy URL so the upstream gateway routes to the same IP that
                earned the cookies.
            country: the exit country the cookies were earned through (a
                slot's), pinned with the session in place of the run's.
            preset: TLS fingerprint preset (httpcloak: chrome-143,
                firefox-latest; curl_cffi: chrome120, etc.). Defaults to the
                fetcher's default_preset.
            timeout: request timeout in seconds.
            extra_headers: the request's own headers (a spider's X-Api-Key, Referer,
                JSON Accept, ...). The browser identity, the codings, the connection
                and proxy headers stay the fetcher's (see ``_OWNED_HEADERS``); a
                Cookie header joins the slot's cookies.
            method: the request's method; anything but GET sends ``body`` (raw bytes,
                as the spider made them: a GraphQL query, a search form).
            body: the request body, for a method other than GET.

        Returns:
            LocalFetchResult with `body` as raw bytes.

        Raises:
            LocalFetchError on any transport-level failure. The caller should
            interpret a 4xx/5xx status code via `block_detect.is_blocked` to
            decide whether to escalate to Mimic.
        """
        effective_preset = preset or self.default_preset
        effective_timeout = timeout or self.timeout
        method = (method or "GET").upper()

        # Pick the best backend per-request. Firefox presets should use
        # curl_cffi because httpcloak's Firefox presets are fake (Chrome
        # internals with wrong H2/header fingerprints). Chrome presets
        # work best with httpcloak (faster, native Rust).
        backend = self._select_backend(effective_preset)

        if backend == "httpcloak":
            return await self._fetch_httpcloak(
                url=url,
                cookies=cookies,
                user_agent=user_agent,
                proxy_url=proxy_url,
                proxy_session_id=proxy_session_id,
                provider=provider,
                country=country,
                preset=effective_preset,
                timeout=effective_timeout,
                extra_headers=extra_headers,
                method=method,
                body=body,
            )
        elif backend == "curl_cffi":
            return await self._fetch_curl_cffi(
                url=url,
                cookies=cookies,
                user_agent=user_agent,
                proxy_url=proxy_url,
                proxy_session_id=proxy_session_id,
                provider=provider,
                country=country,
                preset=effective_preset,
                timeout=effective_timeout,
                extra_headers=extra_headers,
                method=method,
                body=body,
            )
        else:
            raise LocalFetchError(f"Unknown backend state: {backend}")

    def _select_backend(self, preset: str) -> str:
        """Pick the best backend for this preset.

        Firefox presets → curl_cffi (proper Firefox TLS fingerprints)
        Chrome presets → httpcloak (faster, proper Chrome TLS)
        Falls back to whatever is available if the preferred backend isn't.
        """
        is_firefox = preset.startswith("firefox-") or preset == "firefox-latest"
        if is_firefox and _CURL_CFFI_ASYNC is not None:
            return "curl_cffi"
        if not is_firefox and _HTTPCLOAK is not None:
            return "httpcloak"
        # Fallback to whatever is available
        return self.backend

    # ------------------------------------------------------------------
    # httpcloak backend
    # ------------------------------------------------------------------

    async def _fetch_httpcloak(
        self,
        *,
        url: str,
        cookies: Optional[List[Dict]],
        user_agent: Optional[str],
        proxy_url: Optional[str],
        proxy_session_id: Optional[str],
        provider: Optional[str] = None,
        country: Optional[str] = None,
        preset: str,
        timeout: int,
        extra_headers: Optional[Dict[str, str]],
        method: str = "GET",
        body: Optional[bytes] = None,
    ) -> LocalFetchResult:
        import time

        assert _HTTPCLOAK is not None  # narrowed by __init__
        httpcloak = _HTTPCLOAK

        effective_proxy = proxy_url
        if proxy_session_id and proxy_url:
            effective_proxy = _inject_proxy_session(
                proxy_url, proxy_session_id, provider=provider, country=country)

        # A replay presents the Chrome its user agent names (a solver's clearance may name a
        # preset of another Chrome release), and a derived chrome-<N>-windows preset, which
        # httpcloak refuses until it is registered, is derived here too.
        preset = replay_preset(preset, user_agent)

        own_headers, own_cookie = _request_headers(extra_headers)
        headers: Dict[str, str] = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        if cookies:
            headers["Cookie"] = _build_cookie_header(_merge_cookies(cookies, own_cookie))
            # Disable compression to avoid httpcloak decompression errors
            # (the server-side fetcher does the same — see AAR-12 sibling).
            headers["Accept-Encoding"] = "identity"
            logger.info(
                "httpcloak fetch %s: preset=%s, cookies=%s, "
                "ua=%s, proxy_session=%s",
                url, preset,
                [c.get("name") for c in cookies[:10]],
                (user_agent or "")[:60], proxy_session_id,
            )
        elif own_cookie:
            headers["Cookie"] = own_cookie
        headers.update(own_headers)

        start = time.time()

        def _do_sync_fetch():
            session = httpcloak.Session(
                preset=preset, proxy=effective_proxy, timeout=timeout
            )
            try:
                if method != "GET":
                    # the preset's headers as for a fetch(): known names in place, body as made
                    return session.request(method, url, data=body, headers=headers or None)
                if headers:
                    return session.get(url, headers=headers)
                return session.get(url)
            finally:
                session.close()

        try:
            # httpcloak is sync; run in our dedicated thread pool (not the
            # default asyncio executor, which is capped at ~32 workers and
            # would throttle local throughput well below the cookie-pool size).
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(self._executor, _do_sync_fetch)
        except Exception as exc:  # noqa: BLE001
            # A refused CONNECT reads "proxy CONNECT failed: 503 Service Unavailable (pool_busy)":
            # the gateway puts its reason in the status phrase.
            refused = _HTTPCLOAK_REFUSAL_RE.search(str(exc))
            raise LocalFetchError(f"httpcloak fetch failed for {url}: {exc}",
                                  proxy_error=refused.group(1).lower() if refused else None) from exc

        elapsed_ms = int((time.time() - start) * 1000)

        # Body MUST come from .content (bytes), not .text (str).
        # Using .text was the root cause of AAR-12.
        body: bytes = response.content if response.content is not None else b""

        # Sanitize headers. httpcloak returns header values as lists (since
        # HTTP headers can be multi-valued); flatten to single strings so
        # downstream code (e.g. Scrapy's HttpCompressionMiddleware) sees
        # plain values like "gzip" instead of the literal string "['gzip']".
        #
        # Two specific headers MUST be stripped: Content-Encoding and
        # Content-Length. httpcloak transparently decompresses the body
        # before returning it via .content, so the original Content-Encoding
        # (gzip / br / deflate) no longer applies and Content-Length no
        # longer matches. If we left them in place, downstream Scrapy
        # middlewares would try to decompress the already-decompressed body
        # — that was the second AAR-17 corruption bug surfaced in the
        # production rollout on 2026-04-08.
        clean_headers: Dict[str, str] = {}
        raw = response.headers if response.headers else {}
        if hasattr(raw, "items"):
            for k, v in raw.items():
                key_lower = str(k).lower()
                if key_lower in ("content-encoding", "content-length"):
                    continue
                if isinstance(v, (list, tuple)):
                    clean_headers[str(k)] = ", ".join(str(x) for x in v) if v else ""
                else:
                    clean_headers[str(k)] = str(v)

        # Log replay outcome for debugging cookie/fingerprint issues
        if cookies:
            cf_mitigated = clean_headers.get("cf-mitigated", "")
            cf_ray = clean_headers.get("cf-ray", "")
            # the shared classifier's verdict (a small page or an error status alone is no block)
            blocked = is_blocked(response.status_code, body, headers=clean_headers)
            body_hint = ""
            if blocked and body:
                text = body[:200].decode("utf-8", errors="replace")
                if "Just a moment" in text:
                    body_hint = "cf-challenge"
                elif "Attention Required" in text:
                    body_hint = "cf-block"
                else:
                    body_hint = text[:60].replace("\n", " ")
            logger.info(
                "httpcloak replay %s: status=%d size=%d elapsed=%dms "
                "blocked=%s cf_mitigated=%s body_hint=%s",
                url, response.status_code, len(body), elapsed_ms,
                blocked, cf_mitigated, body_hint,
            )

        return LocalFetchResult(
            url=url,
            final_url=str(response.url) if hasattr(response, "url") else url,
            status_code=int(response.status_code),
            headers=clean_headers,
            body=body,
            elapsed_ms=elapsed_ms,
            backend="httpcloak",
            set_cookies=_set_cookie_values(raw),
        )

    # ------------------------------------------------------------------
    # curl_cffi backend
    # ------------------------------------------------------------------

    async def _fetch_curl_cffi(
        self,
        *,
        url: str,
        cookies: Optional[List[Dict]],
        user_agent: Optional[str],
        proxy_url: Optional[str],
        proxy_session_id: Optional[str],
        provider: Optional[str] = None,
        country: Optional[str] = None,
        preset: str,
        timeout: int,
        extra_headers: Optional[Dict[str, str]],
        method: str = "GET",
        body: Optional[bytes] = None,
    ) -> LocalFetchResult:
        import time

        assert _CURL_CFFI_ASYNC is not None
        AsyncSession = _CURL_CFFI_ASYNC

        # Map preset names to curl_cffi impersonate values.
        # curl_cffi uses "firefoxNNN" / "chromeNNN" format (no dash).
        impersonate = preset
        if preset.startswith("firefox-"):
            version = preset.split("-", 1)[1]
            if version == "latest":
                impersonate = "firefox135"
            else:
                impersonate = f"firefox{version}"
        elif preset.startswith("chrome-"):
            version = preset.split("-", 1)[1]
            if version == "latest":
                impersonate = "chrome146"
            else:
                impersonate = f"chrome{version}"

        effective_proxy = proxy_url
        if proxy_session_id and proxy_url:
            effective_proxy = _inject_proxy_session(
                proxy_url, proxy_session_id, provider=provider, country=country)

        proxies = None
        if effective_proxy:
            proxies = {"http": effective_proxy, "https": effective_proxy}

        own_headers, own_cookie = _request_headers(extra_headers)
        headers: Dict[str, str] = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        headers.update(own_headers)

        cookies_dict = None
        cookie_names = []
        if cookies or own_cookie:
            # one Cookie header: curl_cffi would send a Cookie header beside its cookie jar's
            cookies_dict = {c["name"]: c["value"] for c in _merge_cookies(cookies, own_cookie)}
            cookie_names = list(cookies_dict.keys())

        if cookies:
            logger.info(
                "curl_cffi fetch %s: impersonate=%s, cookies=%s, "
                "ua=%s, proxy_session=%s",
                url, impersonate, cookie_names,
                (user_agent or "")[:60], proxy_session_id,
            )

        async def _send(curl_options: Optional[Dict] = None):
            session_kwargs: Dict = {"impersonate": impersonate}
            if curl_options:
                session_kwargs["curl_options"] = curl_options
            async with AsyncSession(**session_kwargs) as session:
                if method != "GET":
                    return await session.request(
                        method,
                        url,
                        data=body,
                        headers=headers or None,
                        cookies=cookies_dict,
                        proxies=proxies,
                        timeout=timeout,
                    )
                return await session.get(
                    url,
                    headers=headers or None,
                    cookies=cookies_dict,
                    proxies=proxies,
                    timeout=timeout,
                )

        # A Firefox ClientHello lists ffdhe2048/ffdhe3072, which curl_cffi cannot compute: a host
        # that ended such a handshake is sent one without them, and a handshake that fails is tried
        # once more without them before the error stands (see FIREFOX_CURVES_NO_FFDHE).
        host = (urlparse(url).hostname or "").lower()
        no_ffdhe = _no_ffdhe_options() if impersonate.startswith("firefox") else None
        start = time.time()
        try:
            response = await _send(no_ffdhe if host in self.no_ffdhe_hosts else None)
        except Exception as exc:  # noqa: BLE001
            if not no_ffdhe or host in self.no_ffdhe_hosts or not _tls_handshake_failed(exc):
                raise _fetch_failed(url, exc) from exc
            try:
                response = await _send(no_ffdhe)
            except Exception:  # noqa: BLE001 -- the first error is the one to report
                raise _fetch_failed(url, exc) from exc
            self.no_ffdhe_hosts.add(host)
            logger.info(
                "curl_cffi: %s ended the %s handshake listing the finite-field groups; its "
                "Firefox fetches leave ffdhe2048/ffdhe3072 out from now on", host, impersonate,
            )

        elapsed_ms = int((time.time() - start) * 1000)

        body: bytes = response.content or b""

        # Strip Content-Encoding and Content-Length — curl_cffi transparently
        # decompresses the body, so the original encoding no longer applies.
        # Without this, Scrapy's HttpCompressionMiddleware tries to decompress
        # the already-decompressed body → gzip.BadGzipFile.
        clean_headers: Dict[str, str] = {}
        for k, v in response.headers.items():
            if str(k).lower() in ("content-encoding", "content-length"):
                continue
            clean_headers[str(k)] = str(v)

        # Log replay outcome for debugging cookie/fingerprint issues
        if cookies:
            cf_mitigated = clean_headers.get("cf-mitigated", "")
            cf_ray = clean_headers.get("cf-ray", "")
            server = clean_headers.get("server", "")
            new_cookies = "set-cookie" in clean_headers
            # the shared classifier's verdict (a small page or an error status alone is no block)
            blocked = is_blocked(response.status_code, body, headers=clean_headers)
            body_hint = ""
            if blocked and body:
                text = body[:200].decode("utf-8", errors="replace")
                if "Just a moment" in text:
                    body_hint = "cf-challenge"
                elif "Attention Required" in text:
                    body_hint = "cf-block"
                elif "captcha" in text.lower():
                    body_hint = "captcha"
                else:
                    body_hint = text[:60].replace("\n", " ")
            logger.info(
                "curl_cffi replay %s: status=%d size=%d elapsed=%dms "
                "blocked=%s cf_mitigated=%s cf_ray=%s server=%s "
                "new_set_cookie=%s body_hint=%s",
                url, response.status_code, len(body), elapsed_ms,
                blocked, cf_mitigated, cf_ray, server,
                new_cookies, body_hint,
            )

        return LocalFetchResult(
            url=url,
            final_url=str(response.url),
            status_code=int(response.status_code),
            headers=clean_headers,
            body=body,
            elapsed_ms=elapsed_ms,
            backend="curl_cffi",
            set_cookies=_set_cookie_values(response.headers),
        )
