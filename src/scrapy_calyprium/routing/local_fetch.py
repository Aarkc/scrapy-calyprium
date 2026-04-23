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
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import urlparse, urlunparse

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
    """Raised when the local fetch backend cannot be used at all."""


# ---------------------------------------------------------------------------
# Proxy-session URL injection
# ---------------------------------------------------------------------------


def _inject_proxy_session(
    proxy_url: str,
    proxy_session_id: str,
    provider: Optional[str] = None,
) -> str:
    """Inject sticky proxy session and provider into the proxy URL username.

    Encodes the provider (e.g. webshare_rotating) and session id into the
    gateway proxy username so the Veil gateway routes through the correct
    upstream with sticky IP pinning.
    """
    parsed = urlparse(proxy_url)
    if not parsed.username:
        return proxy_url
    parts = [parsed.username]
    if provider:
        parts.append(f"p_{provider}")
    parts.append(f"session_{proxy_session_id}")
    new_user = "-".join(parts)
    netloc = f"{new_user}:{parsed.password}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return urlunparse(
        (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
    )


def _build_cookie_header(cookies: List[Dict]) -> str:
    return "; ".join(
        f"{c['name']}={c['value']}" for c in cookies if c.get("name") and "value" in c
    )


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
    ):
        self.default_preset = default_preset
        self.timeout = timeout

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

        logger.info("LocalFetcher initialized with backend=%s", self.backend)

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
    ) -> LocalFetchResult:
        """Perform a single HTTP GET and return raw bytes.

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
            preset: TLS fingerprint preset (httpcloak: chrome-143,
                firefox-latest; curl_cffi: chrome120, etc.). Defaults to the
                fetcher's default_preset.
            timeout: request timeout in seconds.
            extra_headers: additional headers to send.

        Returns:
            LocalFetchResult with `body` as raw bytes.

        Raises:
            LocalFetchError on any transport-level failure. The caller should
            interpret a 4xx/5xx status code via `block_detect.is_blocked` to
            decide whether to escalate to Mimic.
        """
        effective_preset = preset or self.default_preset
        effective_timeout = timeout or self.timeout

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
                preset=effective_preset,
                timeout=effective_timeout,
                extra_headers=extra_headers,
            )
        elif backend == "curl_cffi":
            return await self._fetch_curl_cffi(
                url=url,
                cookies=cookies,
                user_agent=user_agent,
                proxy_url=proxy_url,
                proxy_session_id=proxy_session_id,
                provider=provider,
                preset=effective_preset,
                timeout=effective_timeout,
                extra_headers=extra_headers,
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
        preset: str,
        timeout: int,
        extra_headers: Optional[Dict[str, str]],
    ) -> LocalFetchResult:
        import time

        assert _HTTPCLOAK is not None  # narrowed by __init__
        httpcloak = _HTTPCLOAK

        effective_proxy = proxy_url
        if proxy_session_id and proxy_url:
            effective_proxy = _inject_proxy_session(proxy_url, proxy_session_id, provider=provider)

        headers: Dict[str, str] = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        if cookies:
            headers["Cookie"] = _build_cookie_header(cookies)
            # Disable compression to avoid httpcloak decompression errors
            # (the server-side fetcher does the same — see AAR-12 sibling).
            headers["Accept-Encoding"] = "identity"
        if extra_headers:
            headers.update(extra_headers)

        start = time.time()

        def _do_sync_fetch():
            session = httpcloak.Session(
                preset=preset, proxy=effective_proxy, timeout=timeout
            )
            try:
                if headers:
                    return session.get(url, headers=headers)
                return session.get(url)
            finally:
                session.close()

        try:
            # httpcloak is sync; run in a thread to avoid blocking the loop.
            response = await asyncio.to_thread(_do_sync_fetch)
        except Exception as exc:  # noqa: BLE001
            raise LocalFetchError(f"httpcloak fetch failed for {url}: {exc}") from exc

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

        return LocalFetchResult(
            url=url,
            final_url=str(response.url) if hasattr(response, "url") else url,
            status_code=int(response.status_code),
            headers=clean_headers,
            body=body,
            elapsed_ms=elapsed_ms,
            backend="httpcloak",
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
        preset: str,
        timeout: int,
        extra_headers: Optional[Dict[str, str]],
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
            effective_proxy = _inject_proxy_session(proxy_url, proxy_session_id, provider=provider)

        proxies = None
        if effective_proxy:
            proxies = {"http": effective_proxy, "https": effective_proxy}

        headers: Dict[str, str] = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        if extra_headers:
            headers.update(extra_headers)

        cookies_dict = None
        if cookies:
            cookies_dict = {c["name"]: c["value"] for c in cookies if c.get("name")}

        start = time.time()
        try:
            async with AsyncSession(impersonate=impersonate) as session:
                response = await session.get(
                    url,
                    headers=headers or None,
                    cookies=cookies_dict,
                    proxies=proxies,
                    timeout=timeout,
                )
        except Exception as exc:  # noqa: BLE001
            raise LocalFetchError(f"curl_cffi fetch failed for {url}: {exc}") from exc

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

        return LocalFetchResult(
            url=url,
            final_url=str(response.url),
            status_code=int(response.status_code),
            headers=clean_headers,
            body=body,
            elapsed_ms=elapsed_ms,
            backend="curl_cffi",
        )
