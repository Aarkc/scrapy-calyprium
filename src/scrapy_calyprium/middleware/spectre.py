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
"""

import logging
from typing import Dict, Optional
from urllib.parse import urlparse

import httpx
from scrapy import signals
from scrapy.exceptions import NotConfigured

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
    ):
        self.service_url = service_url.rstrip("/")
        self.api_key = api_key
        self.profile_id = profile_id
        self.sticky_session = sticky_session
        self.rotate_per_request = rotate_per_request
        self.device_type = device_type
        self.browser_family = browser_family
        self.os_family = os_family

        # Cached fingerprint for non-rotating mode
        self._cached_fingerprint: Optional[Dict] = None
        self._session_id: Optional[str] = None

        # Track fingerprints per domain for per-domain mode
        self._domain_fingerprints: Dict[str, Dict] = {}

        self._client: Optional[httpx.Client] = None

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

    def spider_opened(self, spider):
        """Pre-fetch fingerprint when spider starts."""
        logger.info(
            f"SpectreMiddleware: service={self.service_url}, "
            f"profile={self.profile_id or 'default'}, "
            f"rotate_per_request={self.rotate_per_request}, "
            f"sticky_session={self.sticky_session}"
        )

        # Pre-fetch fingerprint if not rotating per request
        if not self.rotate_per_request:
            try:
                self._cached_fingerprint = self._resolve_fingerprint()
                fp = self._cached_fingerprint.get("fingerprint", {})
                logger.info(
                    f"SpectreMiddleware: Using fingerprint "
                    f"'{fp.get('name')}' (ID: {fp.get('id')})"
                )
            except Exception as e:
                logger.warning(f"SpectreMiddleware: Failed to pre-fetch fingerprint: {e}")

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

    def _get_fingerprint_for_request(self, request) -> Dict:
        """
        Get the appropriate fingerprint for a request.

        Handles caching, per-request rotation, and per-domain fingerprints.
        """
        parsed = urlparse(request.url)
        domain = parsed.netloc

        # Per-request rotation — always fetch a new fingerprint
        if self.rotate_per_request:
            try:
                return self._resolve_fingerprint(domain)
            except Exception as e:
                logger.warning(f"SpectreMiddleware: Failed to resolve fingerprint for {domain}: {e}")
                if self._cached_fingerprint:
                    return self._cached_fingerprint
                raise

        # Per-domain caching
        if domain in self._domain_fingerprints:
            return self._domain_fingerprints[domain]

        # Use cached fingerprint if available
        if self._cached_fingerprint:
            return self._cached_fingerprint

        # Fetch new fingerprint
        fingerprint = self._resolve_fingerprint(domain)
        self._cached_fingerprint = fingerprint
        self._domain_fingerprints[domain] = fingerprint
        return fingerprint

    def process_request(self, request, spider):
        """Apply device fingerprint headers to the request."""
        if request.meta.get("_internal"):
            return None

        try:
            fingerprint_data = self._get_fingerprint_for_request(request)
        except Exception as e:
            logger.error(f"SpectreMiddleware: Failed to get fingerprint: {e}")
            return None  # Allow request to proceed without fingerprint

        # Apply headers from fingerprint
        headers = fingerprint_data.get("headers", {})
        for header_name, header_value in headers.items():
            request.headers[header_name] = header_value

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
        """
        blocked = False

        if response.status in (403, 429, 503):
            blocked = True
        elif hasattr(response, "body"):
            try:
                from scrapy_calyprium.routing.block_detect import is_blocked
                blocked = is_blocked(response.status, response.body)
            except ImportError:
                # Optional [local] extra not installed.
                pass

        if blocked:
            fingerprint_id = request.meta.get("spectre_fingerprint_id")
            logger.warning(
                f"SpectreMiddleware: Possible block at {request.url} "
                f"(status: {response.status}, fingerprint: {fingerprint_id})"
            )
            # Clear cache to force rotation on next request
            self._cached_fingerprint = None
            domain = urlparse(request.url).netloc
            self._domain_fingerprints.pop(domain, None)

        return response
