"""
Mimic Browser Middleware for Scrapy.

Integrates with the Mimic browser automation service to provide
anti-detection browser rendering for web scraping.

Activates for requests with ``meta['mimic'] = True``, ``meta['playwright'] = True``,
or when ``MIMIC_ALL_REQUESTS = True``.

Settings:
    MIMIC_SERVICE_URL: Mimic API URL (required)
    CALYPRIUM_API_KEY: API key for authentication (required)
    MIMIC_STEALTH_LEVEL: basic, moderate, or maximum (default: moderate)
    MIMIC_BROWSER_ENGINE: Specific browser engine to use (optional)
    MIMIC_USE_PROXY: Route browser through proxy (default: False)
    MIMIC_ALL_REQUESTS: Render all requests via browser (default: False)
    MIMIC_WAIT_UNTIL: When to consider navigation complete (default: networkidle)
    MIMIC_WAIT_AFTER_LOAD: Extra wait in ms after load (default: 0)
    MIMIC_USE_SPECTRE: Use Spectre for fingerprints (default: True)
    MIMIC_TARGET_DOMAIN: Target domain for per-domain fingerprints (optional)

    MIMIC_LOCAL_FETCH: opt-in local-first auto-routing (AAR-17). When True
        and the optional [local] extra is installed, requests are first
        attempted via in-process httpcloak with cached cookies from a per-spider
        domain cache. Mimic /api/solve is called only when local fetch hits
        a challenge. Falls back to the existing /api/fetch + /api/session
        browser path on unrecoverable failure. Default: False. Every local fetch
        sends the headers the spider set on the request (not the fingerprint's;
        the fetcher keeps its User-Agent, client hints and codings), and a POST
        its method and body (the solve loads a page, then the POST is replayed).
    MIMIC_ESCALATION_TTL: seconds a domain stays escalated to ``maximum``
        stealth after a blocked Mimic response (default: 600)
    MIMIC_TRANSPORT_RETRIES: retries for transport errors (timeouts,
        connection resets) when Scrapy's RetryMiddleware is disabled, as the
        platform does (default: 2)

    Every attempt gets a diagnosis from the fetch layer's shared classifier
    (Mimic's own ``routing.diagnosis`` when it sends one), kept in
    ``meta['calyprium_diagnosis']`` (see ``scrapy_calyprium._outcome``), and the
    diagnosis's move decides whether the request is tried again:
    CALYPRIUM_RETRY_ENABLED: re-schedule a response whose move is
        retry_elsewhere, retry_later, slow_down or climb, and fix_ours once;
        never a stop (not found, walls, policy) (default: True)
    CALYPRIUM_RETRY_TIMES: retries per request (default: 2; a request's
        ``meta['max_retry_times']`` overrides it, ``meta['dont_retry']`` skips it)
    CALYPRIUM_RETRY_DELAY: seconds before retrying a retry_later, and the base of
        slow_down's doubling backoff; a Retry-After wins (default: 5)
    CALYPRIUM_RETRY_MAX_DELAY: cap on any retry delay, in seconds (default: 60)
    A retry carries ``meta['calyprium_attempt_number']`` (1, 2, ...) and
    ``meta['calyprium_prior_class']``, which /api/fetch receives as ``attempt``
    and ``prior_class`` so Mimic can resume its ladder. With Scrapy's
    RetryMiddleware enabled, the statuses in RETRY_HTTP_CODES are left to it.

    Browser sessions are created lazily, on the first request that needs one
    (AAR-64): with MIMIC_ALL_REQUESTS / local-first only /api/fetch or
    /api/solve are used, so an eager session sat idle for the whole run.
    A failure never tears down the shared HTTP client or the run-wide stealth
    level: it falls back per request, a dead session is dropped and re-created
    lazily, and a blocked domain is escalated on its own, with decay.

    MIMIC_SOLVE_DEADLINE_S: how long Mimic's /api/solve may run (Mimic's
        SOLVE_DEADLINE_S, default 240); local-first solves wait that plus 30 s
    MIMIC_LOCAL_PRESET: TLS preset for local fetches (default: chrome-143)
    MIMIC_LOCAL_PROXY_URL: outbound proxy URL for local fetches (optional). When
        absent, derived from VEIL_GATEWAY_URL with ``calyprium:<CALYPRIUM_API_KEY>``
        as the proxy credential.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from scrapy import signals
from scrapy.downloadermiddlewares.cookies import CookiesMiddleware
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse, Response, TextResponse, XmlResponse

from scrapy_calyprium import _headers, _outcome, _trace, _wait
from scrapy_calyprium._policy import RunPolicy
from scrapy_calyprium._veil import resolve_local_proxy_url
from scrapy_calyprium.routing import diagnosis as dg

# Mimic reply headers a Scrapy response must not carry: hop-by-hop headers, the framing of
# the page Mimic already decoded (Content-Encoding would make HttpCompressionMiddleware
# decode it twice) and cookies, which Mimic keeps and which must never reach logs.
_DROP_HEADERS = frozenset((
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "proxy-connection",
    "te", "trailer", "trailers", "transfer-encoding", "upgrade",
    "content-encoding", "content-length", "set-cookie", "set-cookie2", "cookie",
))
# meta flag: the request already had its one fix_ours retry
_FIX_OURS_RETRIED = "calyprium_fix_ours_retried"
# SpectreMiddleware's meta key naming the headers its fingerprint set (spectre.FINGERPRINT_HEADERS)
_FINGERPRINT_HEADERS = "spectre_fingerprint_headers"


def _header_text(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.decode("latin-1")


def _local_headers(request) -> Dict[str, str]:
    """The request's own headers, for the local fetch: the ones the spider set, not the device
    fingerprint's (SpectreMiddleware names them) and not Scrapy's automatic Referer. The
    fetcher drops what would break its browser identity (local_fetch._OWNED_HEADERS)."""
    fingerprint = {str(n).lower() for n in request.meta.get(_FINGERPRINT_HEADERS) or ()}
    if _headers.scrapy_referer(request):
        fingerprint.add("referer")
    out: Dict[str, str] = {}
    for key, values in request.headers.items():
        name = _header_text(key) if isinstance(key, bytes) else str(key)
        if not values or name.lower() in fingerprint:
            continue
        sep = "; " if name.lower() == "cookie" else ", "
        out[name] = sep.join(_header_text(v) if isinstance(v, bytes) else str(v) for v in values)
    return out


def _mimic_headers(env: Any) -> Optional[Dict[str, Any]]:
    """The headers of Mimic's reply for the Scrapy response (see ``_DROP_HEADERS``)."""
    if not isinstance(env, dict):
        return None
    out: Dict[str, Any] = {}
    for key, value in env.items():
        if value is None or str(key).lower() in _DROP_HEADERS:
            continue
        out[str(key)] = [str(v) for v in value] if isinstance(value, (list, tuple)) else str(value)
    return out or None


def _header(env: Any, name: str) -> str:
    if not isinstance(env, dict):
        return ""
    for key, value in env.items():
        if str(key).lower() == name:
            if isinstance(value, (list, tuple)):
                return ", ".join(map(str, value))
            return str(value or "")
    return ""


def _response_from_envelope(request, url, status, headers, body, content_type):
    """Build the right Scrapy Response class from a Mimic /api/fetch envelope.

    The envelope always carries the RAW inner body (AAR-12: bytes preserved); the
    only choice is which Response wrapper gives the spider a usable parser:
      * HTML/XHTML  -> HtmlResponse   (response.css / html xpath)
      * XML         -> XmlResponse    (XML xpath — case-sensitive tags; an
                                       HtmlResponse would lower-case them and a
                                       TextResponse would HTML-parse them, so an
                                       OData/Granicus XML feed yields 0 items)
      * JSON / text -> TextResponse   (response.json() / response.text)
      * anything else / unknown -> HtmlResponse (default) or raw Response (binary)
    """
    ct = (content_type or "").lower()
    encoding = "utf-8"
    if "charset=" in ct:
        encoding = ct.split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
    if isinstance(body, str):
        body = body.encode(encoding, "replace")
    kw = dict(url=url, status=status, headers=headers, body=body or b"", request=request)
    if "text/html" in ct or "xhtml" in ct:
        return HtmlResponse(encoding=encoding, **kw)
    if "xml" in ct:
        return XmlResponse(encoding=encoding, **kw)
    if "text/" in ct or "json" in ct or "javascript" in ct or "csv" in ct:
        return TextResponse(encoding=encoding, **kw)
    if not ct:
        return HtmlResponse(encoding=encoding, **kw)  # legacy default
    return Response(**kw)  # binary / unknown -> raw bytes

# AAR-17: optional local-first routing components. Imported lazily so the
# middleware still works for users who haven't installed scrapy-calyprium[local].
try:
    from scrapy_calyprium.routing.auto import LIGHT_PROBE_INTERVAL_S, SpiderAutoRouter, RouteResult
    from scrapy_calyprium.routing.domain_cache import DomainCache
    from scrapy_calyprium.routing import domain_cache as domain_cache_module
    from scrapy_calyprium.routing.local_fetch import (
        LocalFetcher,
        is_local_fetch_available,
        join_cookie_headers,
    )
    from scrapy_calyprium.routing.solve_client import SolveClient, solve_timeout_from_settings
    from scrapy_calyprium.routing.slot_stats import SlotStatsReporter
    _HAS_LOCAL_ROUTING = True
except ImportError:
    _HAS_LOCAL_ROUTING = False

logger = logging.getLogger(__name__)


def _transport_errors() -> tuple:
    from twisted.internet import error as tx
    from twisted.web.client import ResponseFailed

    errors = [
        tx.TimeoutError, tx.DNSLookupError, tx.ConnectionRefusedError,
        tx.ConnectionDone, tx.ConnectError, tx.ConnectionLost, tx.TCPTimedOutError,
        ResponseFailed, IOError, httpx.TransportError,
    ]
    try:
        from scrapy.core.downloader.handlers.http11 import TunnelError
        errors.append(TunnelError)
    except ImportError:  # pragma: no cover
        pass
    try:
        from twisted.web._newclient import ResponseNeverReceived
        errors.append(ResponseNeverReceived)
    except ImportError:  # pragma: no cover
        pass
    return tuple(errors)


#: Exceptions treated as transient transport failures (mirrors Scrapy's
#: RetryMiddleware.EXCEPTIONS_TO_RETRY).
_TRANSPORT_ERRORS = _transport_errors()


def _default_pool_size(settings) -> int:
    """The cookie pool a spider gets without MIMIC_LOCAL_POOL_SIZE: two slots per request it
    sends to a domain at a time, at most the legacy 8 (Scrapy's default 8 per domain keeps 8)."""
    per_domain = settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN", 8) or 8
    overall = settings.getint("CONCURRENT_REQUESTS", 16) or 16
    return min(8, max(2, 2 * min(per_domain, overall)))


def _builtin_retry_active(settings) -> bool:
    """True if Scrapy's own RetryMiddleware will retry transport errors."""
    if not settings.getbool("RETRY_ENABLED", True):
        return False
    try:
        mws = settings.getwithbase("DOWNLOADER_MIDDLEWARES")
    except AttributeError:
        mws = settings.getdict("DOWNLOADER_MIDDLEWARES")
    for path, order in dict(mws).items():
        name = path if isinstance(path, str) else getattr(path, "__name__", "")
        if name.rsplit(".", 1)[-1] == "RetryMiddleware":
            return order is not None
    return False


class MimicBrowserMiddleware:
    """
    Scrapy middleware that routes requests through Mimic's browser
    automation service for JavaScript rendering and anti-detection.
    """

    def __init__(
        self,
        service_url: str,
        api_key: Optional[str] = None,
        stealth_level: str = "moderate",
        browser_engine: Optional[str] = None,
        use_proxy: bool = False,
        proxy_country: Optional[str] = None,
        use_spectre: bool = True,
        spectre_profile_id: Optional[str] = None,
        spectre_session_id: Optional[str] = None,
        spectre_device_type: Optional[str] = None,
        spectre_browser_family: Optional[str] = None,
        target_domain: Optional[str] = None,
    ):
        self.service_url = service_url.rstrip("/")
        self.api_key = api_key
        self.stealth_level = stealth_level
        self.browser_engine = browser_engine
        self.use_proxy = use_proxy
        self.proxy_country = proxy_country
        self.use_spectre = use_spectre
        self.spectre_profile_id = spectre_profile_id
        self.spectre_session_id = spectre_session_id
        self.spectre_device_type = spectre_device_type
        self.spectre_browser_family = spectre_browser_family
        self.target_domain = target_domain

        # Profile policy (AAR-62) — spider_id, paid-solve, engine allow-list.
        self.policy: RunPolicy = RunPolicy()
        self.render_all: bool = False
        # AAR-64: per-domain stealth escalation (domain -> expiry, monotonic)
        # instead of a permanent run-wide switch to "maximum".
        self.escalation_ttl: float = 600.0
        self._escalated: Dict[str, float] = {}
        self.transport_retries: int = 2
        # Calyprium retry: re-schedule what the diagnosis says is worth another attempt.
        self.retry_enabled: bool = True
        self.retry_times: int = 2
        self.retry_delay: float = 5.0
        self.retry_max_delay: float = 60.0
        self.retry_priority_adjust: int = -1
        # Statuses Scrapy's own RetryMiddleware retries, when it is enabled (left to it).
        self._builtin_retry_codes: frozenset = frozenset()
        self._session_lock: Optional[asyncio.Lock] = None
        self._consecutive_failures = 0
        self.session_id: Optional[str] = None
        self.ws_endpoint: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None
        self.crawler = None  # Set in from_crawler
        # CalypriumRequestTracer extension (found lazily; None if not installed).
        self._tracer = None

        # AAR-17: local-first routing state — initialized in spider_opened
        # if MIMIC_LOCAL_FETCH=True and the [local] extra is installed.
        self._local_router: Optional["SpiderAutoRouter"] = None
        self._local_cache: Optional["DomainCache"] = None
        self._solve_client: Optional["SolveClient"] = None
        self._slot_stats_reporter: Optional["SlotStatsReporter"] = None
        self._local_enabled: bool = False
        # Scrapy's CookiesMiddleware of this crawl (found lazily; False once known to be absent).
        self._cookie_jar_mw: Any = None
        self._local_stats = {
            "local_success": 0,
            "local_blocked": 0,
            "local_solve": 0,
            "fallback_legacy": 0,
        }

    @classmethod
    def from_crawler(cls, crawler):
        service_url = crawler.settings.get("MIMIC_SERVICE_URL")
        if not service_url:
            raise NotConfigured("MIMIC_SERVICE_URL not configured")

        api_key = (
            crawler.settings.get("CALYPRIUM_API_KEY")
            or crawler.settings.get("MIMIC_API_KEY")
        )

        if not api_key:
            raise NotConfigured(
                "CALYPRIUM_API_KEY or MIMIC_API_KEY must be configured"
            )

        policy = RunPolicy.from_settings(crawler.settings)
        middleware = cls(
            service_url=service_url,
            api_key=api_key,
            stealth_level=crawler.settings.get("MIMIC_STEALTH_LEVEL", "moderate"),
            browser_engine=crawler.settings.get("MIMIC_BROWSER_ENGINE"),
            use_proxy=crawler.settings.getbool("MIMIC_USE_PROXY", False),
            # VEIL_COUNTRY or MIMIC_PROXY_COUNTRY: the exit of every hop (see _policy)
            proxy_country=policy.country,
            use_spectre=crawler.settings.getbool("MIMIC_USE_SPECTRE", True),
            spectre_profile_id=crawler.settings.get("MIMIC_SPECTRE_PROFILE_ID"),
            spectre_session_id=crawler.settings.get("MIMIC_SPECTRE_SESSION_ID"),
            spectre_device_type=crawler.settings.get("MIMIC_SPECTRE_DEVICE_TYPE"),
            spectre_browser_family=crawler.settings.get("MIMIC_SPECTRE_BROWSER_FAMILY"),
            target_domain=crawler.settings.get("MIMIC_TARGET_DOMAIN"),
        )
        middleware.render_all = crawler.settings.getbool("MIMIC_ALL_REQUESTS", False)
        middleware.policy = policy
        middleware.escalation_ttl = crawler.settings.getfloat("MIMIC_ESCALATION_TTL", 600.0)
        builtin_retry = _builtin_retry_active(crawler.settings)
        middleware.transport_retries = (
            0 if builtin_retry
            else crawler.settings.getint("MIMIC_TRANSPORT_RETRIES", 2)
        )
        settings = crawler.settings
        middleware.retry_enabled = settings.getbool("CALYPRIUM_RETRY_ENABLED", True)
        middleware.retry_times = max(0, settings.getint("CALYPRIUM_RETRY_TIMES", 2))
        middleware.retry_delay = max(0.0, settings.getfloat("CALYPRIUM_RETRY_DELAY", 5.0))
        middleware.retry_max_delay = max(0.0, settings.getfloat("CALYPRIUM_RETRY_MAX_DELAY", 60.0))
        middleware.retry_priority_adjust = settings.getint("RETRY_PRIORITY_ADJUST", -1)
        if builtin_retry:
            middleware._builtin_retry_codes = frozenset(
                int(c) for c in settings.getlist("RETRY_HTTP_CODES") or ()
            )
        middleware.crawler = crawler

        # AAR-17: local-first routing opt-in
        local_enabled = crawler.settings.getbool("MIMIC_LOCAL_FETCH", False)
        if local_enabled and not _HAS_LOCAL_ROUTING:
            logger.warning(
                "MIMIC_LOCAL_FETCH=True but scrapy-calyprium[local] is not "
                "installed; falling back to legacy routing"
            )
            local_enabled = False
        if local_enabled and not is_local_fetch_available():
            logger.warning(
                "MIMIC_LOCAL_FETCH=True but no local fetch backend is available "
                "(httpcloak / curl_cffi); falling back to legacy routing"
            )
            local_enabled = False
        middleware._local_enabled = local_enabled

        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def _fetch_policy_fields(self) -> dict:
        """Profile policy fields for Mimic ``/api/fetch`` (FetchRequest)."""
        fields = self.policy.mimic_fetch_fields()
        if self.proxy_country and "proxy_country" not in fields:
            fields["proxy_country"] = self.proxy_country
        engine = self.policy.clamp_engine(self.browser_engine)
        if engine:
            fields["browser_engine"] = engine
        return fields

    @staticmethod
    def _attempt_fields(request) -> dict:
        """``attempt`` / ``prior_class`` for /api/fetch on a Calyprium retry, so Mimic can resume
        its ladder (Mimic's FetchRequest ignores fields it does not know)."""
        attempt = _outcome.attempt_number(request)
        if not attempt:
            return {}
        fields: Dict[str, Any] = {"attempt": attempt}
        prior = request.meta.get(_outcome.PRIOR_CLASS)
        if prior:
            fields["prior_class"] = str(prior)
        return fields

    def _keep_mimic_outcome(self, request, data: dict, status: int, html: Any, content_type: str,
                            env_headers: Any, final_url: str) -> None:
        """Keep the attempt's diagnosis (Mimic's, else the classifier's) and Mimic's error
        text in meta."""
        error = _header(env_headers, "x-mimic-error")
        if error:
            request.meta[_outcome.FETCH_ERROR] = error[:300]
        diagnosis = _outcome.from_mimic(data.get("routing"))
        if diagnosis is None:
            if error and not html and int(status or 0) >= 500:
                # A degraded reply (502/504): Mimic's own failure, not a page from the site.
                # (Mimic's pacing answer is a 429 with Retry-After: a rate limit, below.)
                diagnosis = _outcome.of_error(error)
            else:
                # Full headers (cookie names hint at the vendor; a diagnosis never holds values).
                diagnosis = _outcome.of_response(status, html, content_type or None, env_headers,
                                                 request.url, final_url)
        request.meta[_outcome.DIAGNOSIS] = diagnosis

    @staticmethod
    def _mimic_failure(error: BaseException) -> Tuple[Dict[str, Any], str]:
        """Diagnosis and text of a Mimic call that raised (the request falls back to a plain
        download)."""
        if isinstance(error, httpx.HTTPStatusError):
            resp = error.response
            try:
                body = resp.json()
                detail = str(body.get("detail") or "") if isinstance(body, dict) else ""
            except Exception:  # noqa: BLE001 -- not JSON
                detail = (resp.text or "")[:200]
            text = f"mimic {resp.status_code}: {detail}".strip()
            if resp.status_code in (429, 502, 503, 504):
                ra = (resp.headers.get("retry-after") or "").strip()
                return dg.Diagnosis(
                    dg.CAPACITY, "", 0, f"mimic_{resp.status_code}",
                    retry_after=float(ra) if ra.isdigit() else None,
                ).to_dict(), text
            return _outcome.of_error(text), text
        return _outcome.of_error(error), f"{type(error).__name__}: {error}"

    def _get_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        # the download attempt this call is made for (set in process_request): Mimic's spans join its trace
        headers.update(_trace.headers())
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    def _find_tracer(self):
        """The active CalypriumRequestTracer extension, if any (cached)."""
        if self._tracer is None:
            extensions = getattr(self.crawler, "extensions", None)
            for ext in getattr(extensions, "middlewares", None) or ():
                if hasattr(ext, "record_span"):
                    self._tracer = ext
                    break
        return self._tracer

    def _init_local_router(self, spider):
        """AAR-17: stand up the local-first routing components."""
        if not self._local_enabled or self._local_router is not None:
            return
        try:
            preset = self.crawler.settings.get("MIMIC_LOCAL_PRESET", "chrome-143")
            # MIMIC_LOCAL_PROXY_URL if injected, else built from
            # VEIL_GATEWAY_URL + the run's own key (see scrapy_calyprium._veil).
            proxy_url = resolve_local_proxy_url(self.crawler.settings)
            timeout = self.crawler.settings.getint("DOWNLOAD_TIMEOUT", 60)
            # Cookie-pool sizing (throughput ≈ pool_size × rpm_cap). Defaults
            # match the legacy 8 slots × 10 RPM. Raise the pool to scale up;
            # keep rpm_cap low to protect cf_clearance lifetime. A spider that
            # sends one or two requests to a domain at a time gets a smaller
            # pool (two slots per request in flight), so it doesn't pay for
            # solves (and walled hosts' rationed exits) it can never use.
            pool_size = self.crawler.settings.getint(
                "MIMIC_LOCAL_POOL_SIZE", _default_pool_size(self.crawler.settings))
            max_slots = self.crawler.settings.getint("MIMIC_LOCAL_MAX_SLOTS", max(8, pool_size))
            rpm_cap = self.crawler.settings.getfloat("MIMIC_LOCAL_SLOT_RPM_CAP", 10.0)
            # Target throughput: ask for a pages/min rate and let the pool size
            # itself to meet it — slots = ceil(target_rpm / rpm_cap), clamped to
            # MAX_SLOTS. The proactive refill (in SpiderAutoRouter) then holds
            # the pool there, so throughput levels out at the target instead of
            # decaying into a low-gear equilibrium after a disruption.
            target_rpm = self.crawler.settings.getint("MIMIC_LOCAL_TARGET_RPM", 0)
            if target_rpm > 0 and rpm_cap > 0:
                import math
                pool_size = min(max_slots, max(1, math.ceil(target_rpm / rpm_cap)))
            # Never more solves at once than the pool has room for beside its first slot.
            room = max(1, pool_size - 1)
            parallel_solves = self.crawler.settings.getint("MIMIC_LOCAL_PARALLEL_SOLVES", min(3, room))
            cold_start_burst = self.crawler.settings.getint("MIMIC_LOCAL_COLD_START_BURST", min(4, room))
            # Cookie TTL (seconds): how long a solved cf_clearance is reused
            # before retirement. Extend it (0 = keep the built-in default) when
            # the domain's clearance outlives the default, for more pages/solve.
            cookie_ttl = self.crawler.settings.getfloat("MIMIC_LOCAL_COOKIE_TTL", 0.0)
            # httpcloak fetches run in a dedicated thread pool; size it to the
            # pool so N slots x rpm_cap is the real throughput limit, not the
            # default asyncio executor's ~32 workers. Default tracks the pool
            # (min 64) so scaling the pool scales fetch concurrency with it.
            fetch_concurrency = self.crawler.settings.getint(
                "MIMIC_LOCAL_FETCH_CONCURRENCY", max(64, pool_size * 2)
            )
            domain_cache_module.configure(
                max_slots=max_slots, rpm_cap=rpm_cap,
                cookie_ttl=(cookie_ttl if cookie_ttl > 0 else None),
            )
            self._local_cache = DomainCache()
            fetcher = LocalFetcher(
                default_preset=preset,
                timeout=timeout,
                fetch_concurrency=fetch_concurrency,
            )
            # Solve via Tessera when configured (Jevi/2Captcha API solving with a
            # mimic-browser fallback); otherwise fall back to mimic's /api/solve.
            # Same request/response contract, so SolveClient is unchanged.
            # A profile that forbids paid solving keeps solves on mimic's
            # (free) browser: Tessera's primary solvers are paid APIs.
            tessera_url = self.crawler.settings.get("TESSERA_SERVICE_URL")
            use_tessera = bool(tessera_url) and not self.policy.paid_solve_denied
            solve_service_url = tessera_url if use_tessera else self.service_url
            self._solve_backend = "tessera" if use_tessera else "mimic"
            # Wait as long as Mimic may solve (MIMIC_SOLVE_DEADLINE_S + margin).
            solve_timeout = solve_timeout_from_settings(self.crawler.settings)
            self._solve_client = SolveClient(
                service_url=solve_service_url,
                ip_health_url=self.service_url,  # IP reputation stays in mimic
                api_key=self.api_key,
                service_secret=self.crawler.settings.get("FORGE_SERVICE_SECRET")
                or self.crawler.settings.get("MIMIC_SERVICE_SECRET"),
                user_id=self.crawler.settings.get("FORGE_USER_ID")
                or self.crawler.settings.get("MIMIC_USER_ID")
                or self.crawler.settings.get("RECRAWL_USER_ID"),
                timeout=solve_timeout,
                policy=self.policy,
            )
            tracer = self._find_tracer()

            self._local_router = SpiderAutoRouter(
                fetcher=fetcher,
                cache=self._local_cache,
                solve_client=self._solve_client,
                proxy_url=proxy_url,
                provider=self.crawler.settings.get("VEIL_PROVIDER"),
                # Restrict captcha solving to these Tessera solvers (e.g. ["jevi"]).
                # Comma-separated MIMIC_LOCAL_SOLVERS; empty -> Tessera's full chain.
                solvers=self.crawler.settings.getlist("MIMIC_LOCAL_SOLVERS") or None,
                tracer=tracer,
                target_pool_size=pool_size,
                solve_parallel_solves=parallel_solves,
                cold_start_burst=cold_start_burst,
                # Plain fetches over this many sticky gateway sessions (exits), round robin; 0 = one.
                light_sessions=self.crawler.settings.getint("MIMIC_LOCAL_LIGHT_SESSIONS", 0),
                # Seconds a domain whose plain fetch was refused goes straight to its clearances
                # before one request tries the plain fetch again; 0 = always try it first.
                light_probe_interval=self.crawler.settings.getfloat(
                    "MIMIC_LOCAL_LIGHT_PROBE_INTERVAL", LIGHT_PROBE_INTERVAL_S),
            )

            # Slot-stats reporter — periodically batch the local DomainCache's
            # per-slot stats and POST them to Mimic so cross-spider visibility
            # is preserved. Disabled by setting MIMIC_SLOT_STATS_INTERVAL=0.
            interval = self.crawler.settings.getfloat(
                "MIMIC_SLOT_STATS_INTERVAL", 30.0,
            )
            if interval > 0:
                self._slot_stats_reporter = SlotStatsReporter(
                    cache=self._local_cache,
                    service_url=self.service_url,
                    api_key=self.api_key,
                    service_secret=self.crawler.settings.get("FORGE_SERVICE_SECRET")
                    or self.crawler.settings.get("MIMIC_SERVICE_SECRET"),
                    user_id=self.crawler.settings.get("FORGE_USER_ID")
                    or self.crawler.settings.get("MIMIC_USER_ID")
                    or self.crawler.settings.get("RECRAWL_USER_ID"),
                    spider=spider.name if spider else None,
                    interval_seconds=interval,
                )
                # Start the background task — fire-and-forget; spider_opened
                # is sync but we can schedule the asyncio coro on the loop.
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    loop.create_task(self._slot_stats_reporter.start())
                except RuntimeError:
                    pass

            # Expose the router on the spider so parse callbacks can call
            # mimic_router.report_silent_failure(...) when a 200 response
            # passed is_blocked but turned out to contain no useful data.
            if spider is not None:
                spider.mimic_router = self._local_router

            logger.info(
                f"MimicMiddleware: local-first routing enabled "
                f"(backend={fetcher.backend}, preset={preset}, proxy={'yes' if proxy_url else 'no'}, "
                f"solve_backend={self._solve_backend}, solve_timeout={solve_timeout:.0f}s, "
                f"target_pool={pool_size}"
                + (f" (from target_rpm={target_rpm}@{rpm_cap}rpm)" if target_rpm > 0 else "")
                + (f", cookie_ttl={int(cookie_ttl)}s" if cookie_ttl > 0 else "")
                + f", parallel_solves={parallel_solves}, "
                f"slot_stats_interval={interval}s)"
            )
        except Exception as e:
            logger.warning(
                f"MimicMiddleware: failed to initialize local routing: {e}; "
                f"falling back to legacy"
            )
            self._local_router = None
            self._local_enabled = False

    async def spider_opened(self, spider):
        """Stand up local routing. Browser sessions are created lazily."""
        # AAR-17: stand up the local-first router (no-op if disabled)
        self._init_local_router(spider)

    async def _ensure_session(self, spider) -> Optional[str]:
        """Create the browser session on first use (one creator at a time)."""
        if self.session_id:
            return self.session_id
        if self._session_lock is None:
            self._session_lock = asyncio.Lock()
        async with self._session_lock:
            if not self.session_id:
                await self._create_session(spider)
        return self.session_id

    async def _create_session(self, spider) -> None:
        logger.info(
            f"MimicMiddleware: Creating session "
            f"(engine: {self.browser_engine or 'auto'}, "
            f"level: {self.stealth_level})"
        )

        try:
            client = await self._get_client()

            body = {
                "stealth_level": self.stealth_level,
                "use_proxy": self.use_proxy,
                "sticky_session": True,
                "use_spectre": self.use_spectre,
            }

            body.update(self.policy.mimic_session_fields())
            engine = self.policy.clamp_engine(self.browser_engine)
            if engine:
                body["browser_engine"] = engine
            if self.proxy_country:
                body["proxy_country"] = self.proxy_country
            if self.use_spectre:
                if self.spectre_profile_id:
                    body["spectre_profile_id"] = self.spectre_profile_id
                if self.spectre_session_id:
                    body["spectre_session_id"] = self.spectre_session_id
                if self.spectre_device_type:
                    body["spectre_device_type"] = self.spectre_device_type
                if self.spectre_browser_family:
                    body["spectre_browser_family"] = self.spectre_browser_family
                if self.target_domain:
                    body["target_domain"] = self.target_domain

            response = await client.post(
                f"{self.service_url}/api/session",
                json=body,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            data = response.json()
            self.session_id = data["session_id"]
            self.ws_endpoint = data.get("ws_endpoint") or data.get("worker")

            logger.info(
                f"MimicMiddleware: Session {self.session_id} "
                f"(engine: {data.get('browser_engine', '?')}, "
                f"worker: {self.ws_endpoint})"
            )

            if spider is not None:
                spider.mimic_session_id = self.session_id
                spider.mimic_ws_endpoint = self.ws_endpoint

        except httpx.HTTPStatusError as e:
            logger.error(f"MimicMiddleware: Session create failed: {e.response.text}")
        except Exception as e:
            logger.error(f"MimicMiddleware: Error creating session: {e}")

    async def _drop_session(self) -> None:
        """Forget a (presumably dead) session; best-effort DELETE. The shared
        HTTP client stays open for other in-flight requests."""
        session_id, self.session_id = self.session_id, None
        if not session_id:
            return
        try:
            client = await self._get_client()
            await client.delete(
                f"{self.service_url}/api/session/{session_id}",
                headers=self._get_headers(),
            )
        except Exception as e:
            logger.debug(f"MimicMiddleware: Error dropping session {session_id}: {e}")

    def _stealth_for(self, domain: str) -> str:
        expires = self._escalated.get(domain)
        if expires is not None:
            if time.monotonic() < expires:
                return "maximum"
            self._escalated.pop(domain, None)
        return self.stealth_level

    async def spider_closed(self, spider):
        """Close browser session when spider closes."""
        # AAR-17: report local-first stats and tear down the solve client
        if self._local_enabled:
            logger.info(
                "MimicMiddleware: local-first stats: success=%d cookies_solve=%d "
                "blocked=%d fallback_legacy=%d",
                self._local_stats["local_success"],
                self._local_stats["local_solve"],
                self._local_stats["local_blocked"],
                self._local_stats["fallback_legacy"],
            )
            if self._local_router:
                # No solves for a crawl that is over: none started, the running ones cancelled.
                self._local_router.stop_refill()
            if self._slot_stats_reporter:
                try:
                    await self._slot_stats_reporter.stop()
                except Exception:
                    pass
            if self._solve_client:
                try:
                    await self._solve_client.close()
                except Exception:
                    pass

        if self.session_id:
            logger.info(f"MimicMiddleware: Closing session {self.session_id}")
            try:
                client = await self._get_client()
                await client.delete(
                    f"{self.service_url}/api/session/{self.session_id}",
                    headers=self._get_headers(),
                )
            except Exception as e:
                logger.error(f"MimicMiddleware: Error closing session: {e}")
        if self._client:
            try:
                await self._client.aclose()
            finally:
                self._client = None

    _SKIP_PATTERNS = (".xml", "/robots.txt", "/sitemap")

    @staticmethod
    def _domain_for(request) -> str:
        try:
            return urlparse(request.url).netloc
        except Exception:
            return ""

    @staticmethod
    def _keep_local_fallback(request, diagnosis: Optional[dict], error: Optional[str]) -> None:
        """Why the local route handed the request on (the next path diagnoses its own attempt)."""
        if diagnosis is not None:
            request.meta[_outcome.FALLBACK_DIAGNOSIS] = diagnosis
        if error:
            request.meta[_outcome.FETCH_ERROR] = str(error)[:300]

    async def _try_local_route(self, request, spider) -> Optional[Response]:
        """AAR-17: try the local-first auto-router. Return a Response on success,
        None to fall through to the legacy /api/fetch path.

        Per-request opt-out via meta['mimic_local_skip']=True; per-request
        opt-in to *always* use the browser via meta['mimic_force_browser']=True.
        """
        if not self._local_router:
            return None
        if request.meta.get("mimic_force_browser") or request.meta.get("mimic_local_skip"):
            return None

        domain = self._domain_for(request)
        if not domain:
            return None

        # The request as made: its own headers, and the method and body of anything but a GET
        # (a storefront's GraphQL or search POST is fetched, solved and replayed locally too).
        # Its Cookie header carries the cookies earlier responses set (Scrapy's cookie jar).
        headers = _local_headers(request)
        jar = self._jar_cookie(request, spider)
        if jar:
            name = next((k for k in headers if k.lower() == "cookie"), "Cookie")
            headers[name] = join_cookie_headers(headers.get(name, ""), jar)
        route_kwargs: Dict[str, Any] = {"headers": headers}
        method = (request.method or "GET").upper()
        if method != "GET":
            route_kwargs.update(method=method, body=request.body)
        try:
            result = await self._local_router.fetch(request.url, domain=domain, **route_kwargs)
        except Exception as e:
            logger.warning(
                f"MimicMiddleware: local route raised for {request.url}: {e}; "
                f"falling back to legacy"
            )
            self._local_stats["fallback_legacy"] += 1
            request.meta[_outcome.FALLBACK_DIAGNOSIS] = _outcome.of_error(e)
            return None

        route_diag = getattr(result, "diagnosis", None)
        diagnosis = route_diag.to_dict() if isinstance(route_diag, dg.Diagnosis) else None

        if result.needs_legacy_fallback:
            self._local_stats["fallback_legacy"] += 1
            logger.debug(
                f"MimicMiddleware: local route fell through for {domain} "
                f"({result.routing_method}, error={result.error})"
            )
            self._keep_local_fallback(request, diagnosis, result.error)
            return None

        # A rate limit comes back as the response it is: the Calyprium retry slows down
        # (falling through would fetch the same URL again at once by another path).
        rate_limited = (result.fetch is not None and result.blocked and diagnosis is not None
                        and diagnosis.get("move") == dg.SLOW_DOWN)
        if (result.fetch is None or result.blocked) and not rate_limited:
            self._local_stats["local_blocked"] += 1
            self._keep_local_fallback(request, diagnosis, result.error)
            return None

        # Success — wrap as a Scrapy Response with raw bytes (AAR-12 fix point)
        if rate_limited:
            self._local_stats["local_blocked"] += 1
        elif result.routing_method == "solve_then_replay":
            self._local_stats["local_solve"] += 1
        else:
            self._local_stats["local_success"] += 1

        if diagnosis is None:
            diagnosis = _outcome.of_response(
                result.fetch.status_code, result.fetch.body,
                result.fetch.content_type or None, result.fetch.headers,
                request.url, result.fetch.final_url,
            )
        request.meta[_outcome.DIAGNOSIS] = diagnosis
        request.meta[_outcome.ROUTING_METHOD] = result.routing_method
        request.meta["mimic_local_route"] = result.routing_method
        if result.trace_id:
            # The router already recorded this attempt's span.
            request.meta["calyprium_trace_id"] = result.trace_id
        request.meta["mimic_domain_level"] = result.domain_level
        request.meta["mimic_slot_id"] = result.slot_id
        request.meta["mimic_domain"] = domain

        # Pick the right Scrapy response class based on Content-Type so that
        # spiders can use response.text / response.css() on HTML pages while
        # binary responses (PDFs, images) stay as raw bytes via Response.
        # AAR-12 binary preservation still holds — body is raw bytes either
        # way; only the type wrapper differs.
        ct = (
            result.fetch.headers.get("content-type")
            or result.fetch.headers.get("Content-Type")
            or ""
        )
        response_headers: Dict[str, Any] = dict(result.fetch.headers)
        set_cookies = getattr(result.fetch, "set_cookies", None)
        if set_cookies:
            # One header per cookie, so Scrapy's cookie middleware stores every one.
            for key in [k for k in response_headers if str(k).lower() == "set-cookie"]:
                del response_headers[key]
            response_headers["Set-Cookie"] = list(set_cookies)
        return _response_from_envelope(
            request,
            url=result.fetch.final_url or request.url,
            status=result.fetch.status_code,
            headers=response_headers,
            body=result.fetch.body,
            content_type=ct,
        )

    def _cookies_middleware(self) -> Optional[CookiesMiddleware]:
        """Scrapy's CookiesMiddleware of this crawl, or None: COOKIES_ENABLED off,
        MIMIC_LOCAL_COOKIE_JAR off, or no crawler yet."""
        if self._cookie_jar_mw is None:
            crawler = self.crawler
            if crawler is None:
                return None
            if not crawler.settings.getbool("MIMIC_LOCAL_COOKIE_JAR", True):
                self._cookie_jar_mw = False
                return None
            manager = getattr(getattr(getattr(crawler, "engine", None), "downloader", None),
                              "middleware", None)
            if manager is None:
                return None  # the downloader isn't built yet: look again next time
            self._cookie_jar_mw = next(
                (m for m in getattr(manager, "middlewares", ()) if isinstance(m, CookiesMiddleware)),
                False,
            )
        return self._cookie_jar_mw or None

    def _jar_cookie(self, request, spider) -> str:
        """The Cookie header Scrapy's cookie middleware would send with ``request``: the
        cookies earlier responses set, and the request's own ``cookies=``. That middleware
        (700) never sees a request this one (200) answers, so the local route sent none of
        them; its process_response still stores what each response sets. Empty when cookies
        are off, under meta ``dont_merge_cookies``, or with nothing stored for the URL."""
        mw = self._cookies_middleware()
        if mw is None:
            return ""
        probe = request.replace()  # it rewrites the Cookie header of the request it is given
        try:
            mw.process_request(probe, spider)
        except Exception as exc:  # noqa: BLE001 — a jar failure must not fail the fetch
            logger.debug("MimicMiddleware: cookie jar unavailable for %s: %s", request.url, exc)
            return ""
        value = probe.headers.get("Cookie")
        return _header_text(value) if value else ""

    async def process_request(self, request, spider):
        """Intercept requests that need stealth rendering.

        Three modes (in order of precedence):

        - **AAR-17 local-first** (MIMIC_LOCAL_FETCH=True + scrapy-calyprium[local]):
          Every request first tries in-process httpcloak with cached cookies
          from a per-spider domain cache. Only blocked requests fall through to
          the legacy /api/fetch path below. Recommended for production.
        - **render_all** (MIMIC_ALL_REQUESTS=True): Every request goes through
          Mimic's /api/fetch with auto-routing. Older mode, kept for compat.
        - **per-request** (meta['mimic']=True): Only flagged requests go through
          a browser session. Used when most pages work with plain HTTP but a few
          need JavaScript rendering.
        """
        if request.meta.get("_internal"):
            return None

        # Per-attempt trace state (a retried/replaced request copies meta, so
        # drop the previous attempt's): start time, routing and the marker that
        # SpiderAutoRouter already traced it. See CalypriumRequestTracer.
        request.meta.pop("calyprium_trace_id", None)
        request.meta.pop("calyprium_routing", None)
        # ...and its outcome (see _outcome): each attempt is diagnosed afresh.
        for key in (_outcome.DIAGNOSIS, _outcome.FETCH_ERROR, _outcome.FALLBACK_DIAGNOSIS,
                    _outcome.ROUTING_METHOD):
            request.meta.pop(key, None)
        request.meta["_calyprium_t0"] = time.monotonic()
        # The attempt CalypriumTraceMiddleware started, for the calls made below (this coroutine's own context).
        ids = _trace.attempt_ids(request)
        if ids:
            _trace.ATTEMPT.set(ids)
        _outcome.CURRENT_ATTEMPT.set(_outcome.attempt_number(request))

        # AAR-17: local-first fast path
        if self._local_enabled:
            local_response = await self._try_local_route(request, spider)
            if local_response is not None:
                return local_response

        explicit_browser = (
            request.meta.get("playwright", False)
            or request.meta.get("mimic", False)
            or request.meta.get("stealth", False)
        )

        if not self.render_all and not explicit_browser:
            return None

        # Skip sitemap/robots unless explicitly flagged
        url_lower = request.url.lower()
        if any(p in url_lower for p in self._SKIP_PATTERNS):
            if not request.meta.get("mimic_sitemap"):
                return None

        request.meta["mimic_browser"] = True

        try:
            client = await self._get_client()

            # Auto-routing mode: use /api/fetch which tries httpcloak first,
            # then escalates to browser only when blocked.
            if self.render_all and not explicit_browser:
                return await self._fetch_auto(client, request)

            # Explicit browser mode: use a session for full JS rendering
            return await self._fetch_browser(client, request, spider)

        except Exception as e:
            # Per-request fallback: this request continues as a plain download.
            # Never tear down the shared client or local router here — other
            # coroutines are using them (AAR-64).
            logger.error(f"MimicMiddleware: Failed {request.url}: {e}")
            # Keep why Mimic failed; the plain download diagnoses its own outcome.
            diagnosis, text = self._mimic_failure(e)
            for key in (_outcome.DIAGNOSIS, "calyprium_routing", _outcome.ROUTING_METHOD):
                request.meta.pop(key, None)
            request.meta[_outcome.FALLBACK_DIAGNOSIS] = diagnosis
            request.meta[_outcome.FETCH_ERROR] = text[:300]
            self._consecutive_failures += 1
            if self._consecutive_failures >= 3 and self.session_id:
                logger.warning(
                    f"MimicMiddleware: {self._consecutive_failures} consecutive "
                    f"failures, dropping session {self.session_id} "
                    f"(re-created on next use)"
                )
                self._consecutive_failures = 0
                await self._drop_session()

            return None

    @staticmethod
    def _routing_from_fetch(data: dict) -> dict:
        """How mimic served a ``/api/fetch`` (its ``routing`` object: method,
        engine, network, tier, escalation), for the request tracer. Older
        mimic builds return no ``routing``; fall back to the engine used."""
        routing = data.get("routing")
        if isinstance(routing, dict) and routing:
            return dict(routing)
        engine = str(data.get("browser_engine") or "")
        http = engine.lower() in ("", "httpcloak", "http", "curl_cffi")
        return {
            "routing_method": "httpcloak" if http else "browser",
            "engine": engine,
        }

    async def _fetch_auto(self, client, request):
        """Fetch via /api/fetch with auto-routing (httpcloak → browser)."""
        fetch_payload = {
            "url": request.url,
            "stealth_level": self._stealth_for(self._domain_for(request)),
            **self._fetch_policy_fields(),
            **self._attempt_fields(request),
        }

        resp = await client.post(
            f"{self.service_url}/api/fetch",
            json=fetch_payload,
            headers=self._get_headers(),
            timeout=60.0,
        )
        resp.raise_for_status()
        data = resp.json()

        html = data.get("html") or data.get("content") or ""
        status = data.get("status_code", 200)
        env_headers = data.get("headers") or {}
        ct = _header(env_headers, "content-type")
        final_url = data.get("final_url") or request.url

        self._consecutive_failures = 0
        routing = self._routing_from_fetch(data)
        request.meta["calyprium_routing"] = routing
        request.meta[_outcome.ROUTING_METHOD] = routing.get("routing_method") or "mimic_fetch"
        self._keep_mimic_outcome(request, data, status, html, ct, env_headers, final_url)

        # Wrap by Content-Type so an XML/JSON API body (e.g. Legistar OData) gets
        # the right parser instead of being HTML-parsed into 0 items (AAR-12).
        # Mimic's headers ride along (minus framing and cookies, see _DROP_HEADERS).
        return _response_from_envelope(
            request,
            url=final_url,
            status=status,
            headers=_mimic_headers(env_headers),
            body=html,
            content_type=ct,
        )

    async def _fetch_browser(self, client, request, spider):
        """Fetch via browser session for full JS rendering."""
        await self._ensure_session(spider)

        if not self.session_id:
            logger.warning("MimicMiddleware: No session, falling back")
            return None

        request.meta["mimic_session_id"] = self.session_id

        wait_until = request.meta.get("mimic_wait_until")
        wait_after = request.meta.get("mimic_wait_after_load")
        if not wait_until and self.crawler:
            wait_until = self.crawler.settings.get("MIMIC_WAIT_UNTIL")
        if not wait_after and self.crawler:
            wait_after = self.crawler.settings.getint("MIMIC_WAIT_AFTER_LOAD", 0)

        action_payload = {
            "action": "navigate",
            "url": request.url,
            "wait_for": wait_until or "networkidle",
        }
        if wait_after and int(wait_after) > 0:
            action_payload["wait_after_load"] = int(wait_after)

        action_url = f"{self.service_url}/api/session/{self.session_id}/action"
        resp = await client.post(
            action_url,
            json=action_payload,
            headers=self._get_headers(),
            timeout=60.0,
        )
        resp.raise_for_status()
        data = resp.json()

        html = data.get("html") or data.get("content") or data.get("page_source", "")
        status = data.get("status_code", 200)
        env = data.get("headers") or {}
        final_url = data.get("final_url") or request.url
        # A session navigate is always a browser render.
        routing = {
            "routing_method": "browser_direct",
            "engine": data.get("browser_engine") or self.browser_engine or "browser",
            "tier": "browser",
        }

        if not html:
            # Fallback to /api/fetch
            fetch_resp = await client.post(
                f"{self.service_url}/api/fetch",
                json={
                    "url": request.url,
                    "session_id": self.session_id,
                    "stealth_level": self._stealth_for(self._domain_for(request)),
                    **self._fetch_policy_fields(),
                    **self._attempt_fields(request),
                },
                headers=self._get_headers(),
                timeout=60.0,
            )
            fetch_resp.raise_for_status()
            data = fetch_resp.json()
            html = data.get("html") or data.get("content") or ""
            status = data.get("status_code", 200)
            env = data.get("headers") or {}
            final_url = data.get("final_url") or request.url
            routing = self._routing_from_fetch(data)

        ct = _header(env, "content-type")
        self._consecutive_failures = 0
        request.meta["calyprium_routing"] = routing
        request.meta[_outcome.ROUTING_METHOD] = routing.get("routing_method") or "browser_direct"
        self._keep_mimic_outcome(request, data, status, html, ct, env, final_url)

        return _response_from_envelope(
            request,
            url=final_url,
            status=status,
            headers=_mimic_headers(env),
            body=html,
            content_type=ct,
        )

    async def process_response(self, request, response, spider):
        """Diagnose the attempt, escalate a blocked domain, and retry what is worth it.

        The diagnosis is the one the Mimic / local path kept in meta for the
        response it built (Mimic's own when it sends one), else the shared
        classifier's verdict on this response (a plain download). AAR-5: block
        detection never matches loose words on a page; see block_detect.

        A response whose diagnosis says try again (see ``_retry``) is replaced by
        a new request; ``response_received`` never fires for it, so its span is
        recorded here.
        """
        if request.meta.get("_internal"):
            return response
        diagnosis = _outcome.for_response(request, response)

        if request.meta.get("mimic_browser") and _outcome.blocked(diagnosis):
            logger.warning(
                f"MimicMiddleware: Possible block at {request.url} "
                f"(status: {response.status}, {diagnosis.get('class')})"
            )
            domain = self._domain_for(request)
            if domain and self.stealth_level != "maximum":
                if domain not in self._escalated:
                    logger.info(
                        f"MimicMiddleware: escalating {domain} to maximum stealth "
                        f"for {self.escalation_ttl:.0f}s"
                    )
                self._escalated[domain] = time.monotonic() + self.escalation_ttl

        retry = await self._retry(request, diagnosis, response)
        return retry if retry is not None else response

    async def _retry(self, request, diagnosis: Optional[dict], response=None):
        """A new attempt of ``request`` when its diagnosis's move is worth one, else None.

        retry_elsewhere / climb go again at once (another exit; Mimic resumes its
        ladder from ``attempt`` / ``prior_class``), retry_later after
        CALYPRIUM_RETRY_DELAY, slow_down after a doubling backoff, and a
        Retry-After the site or Mimic gave wins; fix_ours goes again once; stop
        moves never. Capped at CALYPRIUM_RETRY_TIMES per request.
        """
        if not self.retry_enabled or not isinstance(diagnosis, dict):
            return None
        meta = request.meta
        if meta.get("dont_retry") or meta.get("_internal"):
            return None
        cls = str(diagnosis.get("class") or "")
        move = diagnosis.get("move")
        if move == dg.FIX_OURS:
            if meta.get(_FIX_OURS_RETRIED):
                return None
        elif move not in _outcome.RETRY_MOVES:
            return None
        if response is not None and response.status in self._builtin_retry_codes:
            return None  # Scrapy's RetryMiddleware already retried this status
        attempt = _outcome.attempt_number(request)
        try:
            limit = int(meta.get("max_retry_times", self.retry_times))
        except (TypeError, ValueError):
            limit = self.retry_times
        stats = getattr(self.crawler, "stats", None)
        if attempt >= limit:
            if stats is not None:
                stats.inc_value("calyprium/retry/max_reached")
            logger.info(
                f"MimicMiddleware: gave up on {request.url} after {attempt} "
                f"retr{'y' if attempt == 1 else 'ies'} ({cls})"
            )
            return None

        if response is not None:
            # This attempt went out on the wire, and response_received will not fire for it.
            tracer = self._find_tracer()
            if tracer is not None and hasattr(tracer, "trace_response"):
                try:
                    tracer.trace_response(request, response)
                except Exception as exc:  # noqa: BLE001 -- tracing never breaks a crawl
                    logger.debug(f"MimicMiddleware: retry trace failed: {exc}")
        if stats is not None:
            stats.inc_value("calyprium/retry/count")
            stats.inc_value(f"calyprium/retry/{cls or 'unknown'}")
        delay = self._retry_delay(diagnosis, attempt)
        vendor = diagnosis.get("vendor")
        logger.info(
            f"MimicMiddleware: retrying {request.url} ({cls}"
            f"{', ' + str(vendor) if vendor else ''}: {move}) "
            f"{attempt + 1}/{limit}" + (f" in {delay:.1f}s" if delay else "")
        )
        if delay > 0:
            await self._sleep(delay)
        retry = request.replace(dont_filter=True,
                                priority=request.priority + self.retry_priority_adjust)
        retry.meta[_outcome.ATTEMPT_NUMBER] = attempt + 1
        retry.meta[_outcome.PRIOR_CLASS] = cls
        if move == dg.FIX_OURS:
            retry.meta[_FIX_OURS_RETRIED] = True
        return retry

    def _retry_delay(self, diagnosis: dict, attempt: int) -> float:
        """Seconds to wait before retrying: Retry-After when given, else by move."""
        move = diagnosis.get("move")
        retry_after = diagnosis.get("retry_after")
        try:
            retry_after = float(retry_after) if retry_after is not None else None
        except (TypeError, ValueError):
            retry_after = None
        if retry_after is not None and retry_after >= 0:
            delay = retry_after
        elif move == dg.SLOW_DOWN:
            delay = self.retry_delay * (2 ** attempt)
        elif move == dg.RETRY_LATER:
            delay = self.retry_delay
        else:
            delay = 0.0
        return max(0.0, min(delay, self.retry_max_delay))

    @staticmethod
    async def _sleep(delay: float) -> None:
        """Wait without blocking the reactor (see ``_wait.sleep``). Holds the request's place in
        CONCURRENT_REQUESTS meanwhile, so a rate-limited crawl slows down as a whole; a download
        slot's delay would not apply, since a Mimic-served request never enters the slot queue."""
        await _wait.sleep(delay)

    async def process_exception(self, request, exception, spider):
        """Capped retry of transport errors (timeouts, resets, DNS).

        The platform disables Scrapy's RetryMiddleware, so without this a
        single timeout dropped the URL for good (AAR-64). Only active when
        RetryMiddleware isn't, so retries never double up. The error's
        diagnosis is kept in ``meta['calyprium_diagnosis']`` (an errback sees it).
        """
        if request.meta.get("mimic_browser"):
            logger.error(f"MimicMiddleware: Exception for {request.url}: {exception}")
        if not isinstance(exception, _TRANSPORT_ERRORS):
            return None
        diagnosis = _outcome.of_error(exception)
        request.meta[_outcome.DIAGNOSIS] = diagnosis
        # Every failed attempt went out on the wire: one span each.
        tracer = self._find_tracer()
        if tracer is not None and hasattr(tracer, "trace_exception"):
            tracer.trace_exception(request, exception)
        if not self.transport_retries:
            return None
        retries = request.meta.get("mimic_transport_retries", 0)
        stats = getattr(self.crawler, "stats", None)
        if retries >= self.transport_retries:
            if stats is not None:
                stats.inc_value("mimic/transport_retry/max_reached")
            return None
        if stats is not None:
            stats.inc_value("mimic/transport_retry/count")
            stats.inc_value(f"calyprium/retry/{diagnosis['class']}")
        logger.info(
            f"MimicMiddleware: retrying {request.url} after transport error "
            f"({retries + 1}/{self.transport_retries}): {exception!r}"
        )
        retry = request.replace(dont_filter=True, priority=request.priority - 1)
        retry.meta["mimic_transport_retries"] = retries + 1
        return retry
