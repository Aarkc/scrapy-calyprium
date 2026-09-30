"""
Spectre Device Fingerprint Middleware for Scrapy.

Applies consistent device fingerprints to all spider requests
by fetching fingerprints from the Spectre service. Fingerprints
include browser headers (User-Agent, Accept-Language, etc.) that
make requests appear to come from real devices.

Settings:
    CALYPRIUM_API_KEY: API key for authentication (required)
    SPECTRE_SERVICE_URL: Spectre API URL (default: https://spectre.calyprium.com)
    SPECTRE_PROFILE_ID: Optional profile ID for specific configuration
    SPECTRE_STICKY_SESSION: Use sticky sessions (default: False)
    SPECTRE_ROTATE_PER_REQUEST: Rotate fingerprint per request (default: False)
    SPECTRE_DEVICE_TYPE: Filter by device type (optional)
    SPECTRE_BROWSER_FAMILY: Filter by browser (optional)
    SPECTRE_OS_FAMILY: Filter by OS (optional)
    SPECTRE_BLOCK_DETECTION: Detect blocks and rotate the fingerprint on a hit
        (default: True). Set False for spiders that hit clean JSON/XML APIs where
        block-detection only causes false-positive fingerprint churn. Can also be
        bypassed per-request with ``request.meta["spectre_skip_block_detection"]``.
    SPECTRE_FALLBACK_USER_AGENT: User-Agent applied while no fingerprint is
        available (Spectre down/slow). Default: a current desktop Chrome UA.

Fingerprints are resolved off the reactor thread (AAR-64): the first one in
``spider_opened``, later ones in the background. ``process_request`` never
blocks on Spectre; a request with no fingerprint yet gets the fallback
User-Agent (the platform disables Scrapy's UserAgentMiddleware, so without it
requests went out with no UA at all). Failures back off exponentially instead
of retrying a 10s call on every request. With ``SPECTRE_ROTATE_PER_REQUEST`` a
new fingerprint is fetched in the background per request and applied as soon
as it arrives.
"""

import logging
import time
from typing import Dict, Optional
from urllib.parse import urlparse

import httpx
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.utils.defer import maybe_deferred_to_future
from twisted.internet.threads import deferToThread

logger = logging.getLogger(__name__)


class SpectreMiddleware:
    """
    Scrapy downloader middleware that applies device fingerprints
    from the Spectre service to all requests.

    Fingerprints are cached by default. Enable ``SPECTRE_ROTATE_PER_REQUEST``
    to fetch a new fingerprint for every request, or rely on per-domain
    caching for multi-site crawls. When a block is detected (403/429/503
    or captcha keywords), the cached fingerprint is cleared so the next
    request gets a fresh identity.
    """

    def __init__(
        self,
        service_url: str,
        api_key: str,
        profile_id: Optional[str] = None,
        sticky_session: bool = False,
        rotate_per_request: bool = False,
        device_type: Optional[str] = None,
        browser_family: Optional[str] = None,
        os_family: Optional[str] = None,
        block_detection: bool = True,
        fallback_user_agent: Optional[str] = None,
    ):
        self.service_url = service_url.rstrip("/")
        self.api_key = api_key
        self.profile_id = profile_id
        self.sticky_session = sticky_session
        self.rotate_per_request = rotate_per_request
        self.device_type = device_type
        self.browser_family = browser_family
        self.os_family = os_family
        self.block_detection = block_detection

        # Cached fingerprint for non-rotating mode
        self._cached_fingerprint: Optional[Dict] = None
        self._session_id: Optional[str] = None

        # Track fingerprints per domain for per-domain mode
        self._domain_fingerprints: Dict[str, Dict] = {}

        self._client: Optional[httpx.Client] = None

        from scrapy_calyprium._config import CalypriumConfig

        self.fallback_user_agent = fallback_user_agent or CalypriumConfig.user_agent
        # Background resolution state (reactor thread only).
        self._refresh_in_flight = False
        self._retry_at = 0.0
        self._backoff = self.MIN_BACKOFF

    MIN_BACKOFF = 5.0
    MAX_BACKOFF = 300.0

    #: Runs a blocking callable off the reactor; overridable in tests.
    _run_blocking = staticmethod(deferToThread)
    _now = staticmethod(time.monotonic)

    @classmethod
    def from_crawler(cls, crawler):
        api_key = (
            crawler.settings.get("CALYPRIUM_API_KEY")
            or crawler.settings.get("SPECTRE_API_KEY")
        )
        if not api_key:
            raise NotConfigured(
                "SpectreMiddleware requires CALYPRIUM_API_KEY or SPECTRE_API_KEY. "
                "Set it in settings.py or use scrapy_calyprium.configure()."
            )

        from scrapy_calyprium._config import get_config

        config = get_config()

        middleware = cls(
            service_url=crawler.settings.get(
                "SPECTRE_SERVICE_URL",
                config.spectre_url or "https://spectre.calyprium.com",
            ),
            api_key=api_key,
            profile_id=crawler.settings.get("SPECTRE_PROFILE_ID"),
            sticky_session=crawler.settings.getbool("SPECTRE_STICKY_SESSION", False),
            rotate_per_request=crawler.settings.getbool(
                "SPECTRE_ROTATE_PER_REQUEST", False
            ),
            device_type=crawler.settings.get("SPECTRE_DEVICE_TYPE"),
            browser_family=crawler.settings.get("SPECTRE_BROWSER_FAMILY"),
            os_family=crawler.settings.get("SPECTRE_OS_FAMILY"),
            block_detection=crawler.settings.getbool("SPECTRE_BLOCK_DETECTION", True),
            fallback_user_agent=crawler.settings.get("SPECTRE_FALLBACK_USER_AGENT"),
        )
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def _get_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(timeout=10.0)
        return self._client

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

    async def spider_opened(self, spider):
        """Pre-fetch a fingerprint (off the reactor) when the spider starts."""
        logger.info(
            f"SpectreMiddleware: service={self.service_url}, "
            f"profile={self.profile_id or 'default'}, "
            f"rotate_per_request={self.rotate_per_request}, "
            f"sticky_session={self.sticky_session}"
        )
        d = self._refresh()
        if d is not None:
            await maybe_deferred_to_future(d)
        fp = (self._cached_fingerprint or {}).get("fingerprint", {})
        if self._cached_fingerprint:
            logger.info(
                f"SpectreMiddleware: Using fingerprint "
                f"'{fp.get('name')}' (ID: {fp.get('id')})"
            )

    def _refresh(self, domain: Optional[str] = None):
        """Resolve a fingerprint in a thread unless one is already in flight
        or we're backing off after a failure. Returns the Deferred or None."""
        if self._refresh_in_flight or self._now() < self._retry_at:
            return None
        self._refresh_in_flight = True
        d = self._run_blocking(self._resolve_fingerprint, domain)

        def _ok(result):
            self._cached_fingerprint = result
            if domain:
                self._domain_fingerprints[domain] = result
            self._backoff = self.MIN_BACKOFF
            self._retry_at = 0.0

        def _err(failure):
            self._retry_at = self._now() + self._backoff
            logger.warning(
                f"SpectreMiddleware: fingerprint resolve failed ({failure.value}); "
                f"using fallback User-Agent, retrying in {self._backoff:.0f}s"
            )
            self._backoff = min(self._backoff * 2, self.MAX_BACKOFF)

        def _done(_):
            self._refresh_in_flight = False

        d.addCallbacks(_ok, _err)
        d.addBoth(_done)
        return d

    def spider_closed(self, spider):
        """Clean up HTTP client."""
        if self._client and not self._client.is_closed:
            self._client.close()
            self._client = None

    def _resolve_fingerprint(self, domain: Optional[str] = None) -> Dict:
        """
        Resolve a fingerprint from the Spectre service.

        Args:
            domain: Target domain for domain-specific rules.

        Returns:
            Response dict with 'fingerprint' and 'headers' keys.

        Raises:
            httpx.HTTPStatusError: If the API request fails.
        """
        url = f"{self.service_url}/api/fingerprints/resolve"

        body: Dict = {}
        if domain:
            body["domain"] = domain
        if self.profile_id:
            body["profile_id"] = self.profile_id
        if self._session_id:
            body["session_id"] = self._session_id
        if self.device_type:
            body["device_type"] = self.device_type
        if self.browser_family:
            body["browser_family"] = self.browser_family
        if self.os_family:
            body["os_family"] = self.os_family

        client = self._get_client()
        response = client.post(url, json=body, headers=self._auth_headers())
        response.raise_for_status()

        result = response.json()

        # Store session ID for sticky sessions
        if self.sticky_session and result.get("session_id"):
            self._session_id = result["session_id"]

        return result

    def _get_fingerprint_for_request(self, request) -> Optional[Dict]:
        """
        Get the fingerprint for a request without blocking.

        Handles caching, per-request rotation, and per-domain fingerprints.
        Returns None when nothing is cached yet (a background resolve is
        started, subject to failure backoff).
        """
        domain = urlparse(request.url).netloc

        if self.rotate_per_request:
            # Start the next fingerprint now; apply the latest one we have.
            self._refresh(domain)
            return self._cached_fingerprint

        fingerprint = self._domain_fingerprints.get(domain) or self._cached_fingerprint
        if fingerprint is None:
            self._refresh(domain)
        return fingerprint

    def process_request(self, request, spider):
        """Apply device fingerprint headers to the request."""
        if request.meta.get("_internal"):
            return None

        fingerprint_data = self._get_fingerprint_for_request(request)
        if not fingerprint_data:
            # Spectre down/slow: never send a request without a User-Agent.
            request.headers.setdefault("User-Agent", self.fallback_user_agent)
            request.meta["spectre_fallback_ua"] = True
            return None

        # Apply headers from fingerprint
        headers = fingerprint_data.get("headers", {})
        for header_name, header_value in headers.items():
            request.headers[header_name] = header_value

        request.headers.setdefault("User-Agent", self.fallback_user_agent)

        # Store fingerprint info in request meta for tracking/debugging
        fingerprint = fingerprint_data.get("fingerprint", {})
        request.meta["spectre_fingerprint_id"] = fingerprint.get("id")
        request.meta["spectre_fingerprint_name"] = fingerprint.get("name")

        logger.debug(
            f"SpectreMiddleware: Applied '{fingerprint.get('name')}' to {request.url}"
        )

        return None

    def process_response(self, request, response, spider):
        """Detect blocks and clear cached fingerprint to force rotation.

        AAR-5: previously this used a substring match on `b"blocked" in
        body and b"access" in body` which fired on every legitimate page
        that happened to contain both words (DigiKey product pages
        contain both 100+ times). Every false positive cleared the
        fingerprint cache and forced a Spectre re-roll. Now uses the
        AAR-15 block_detect.is_blocked() helper which checks for real
        challenge markers.

        Block-detection can be turned off entirely with SPECTRE_BLOCK_DETECTION
        = False (for clean JSON/XML API spiders) or skipped per-request with
        request.meta["spectre_skip_block_detection"].
        """
        if not self.block_detection or request.meta.get("spectre_skip_block_detection"):
            return response

        blocked = False

        if hasattr(response, "body"):
            try:
                from scrapy_calyprium.routing.block_detect import is_blocked
                content_type = response.headers.get("Content-Type", b"").decode(
                    "latin-1", "ignore"
                )
                # Pass headers so a plain IIS/nginx rate-limit 403 (no WAF signal)
                # isn't flagged as a block and churn the fingerprint. Block statuses
                # go through the same gate as everything else — previously
                # 403/429/503 were hard-coded as blocked, bypassing the gate.
                blocked = is_blocked(
                    response.status, response.body,
                    content_type=content_type, headers=dict(response.headers),
                )
            except ImportError:
                # Optional [local] extra not installed — fall back to status only.
                blocked = response.status in (403, 429, 503)
        else:
            blocked = response.status in (403, 429, 503)

        if blocked:
            fingerprint_id = request.meta.get("spectre_fingerprint_id")
            logger.warning(
                f"SpectreMiddleware: Possible block at {request.url} "
                f"(status: {response.status}, fingerprint: {fingerprint_id})"
            )
            # Rotate: resolve a fresh identity in the background; it replaces
            # the cached one as soon as it arrives (never blocks the reactor).
            domain = urlparse(request.url).netloc
            self._domain_fingerprints.pop(domain, None)
            self._refresh(domain)

        return response
