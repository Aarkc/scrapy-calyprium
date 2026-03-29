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
"""

import logging
from typing import Optional
from urllib.parse import urljoin

import httpx
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse

logger = logging.getLogger(__name__)


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

        self.render_all: bool = False
        self.session_id: Optional[str] = None
        self.ws_endpoint: Optional[str] = None
        self._client: Optional[httpx.AsyncClient] = None
        self.crawler = None  # Set in from_crawler

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

        middleware = cls(
            service_url=service_url,
            api_key=api_key,
            stealth_level=crawler.settings.get("MIMIC_STEALTH_LEVEL", "moderate"),
            browser_engine=crawler.settings.get("MIMIC_BROWSER_ENGINE"),
            use_proxy=crawler.settings.getbool("MIMIC_USE_PROXY", False),
            proxy_country=crawler.settings.get("MIMIC_PROXY_COUNTRY"),
            use_spectre=crawler.settings.getbool("MIMIC_USE_SPECTRE", True),
            spectre_profile_id=crawler.settings.get("MIMIC_SPECTRE_PROFILE_ID"),
            spectre_session_id=crawler.settings.get("MIMIC_SPECTRE_SESSION_ID"),
            spectre_device_type=crawler.settings.get("MIMIC_SPECTRE_DEVICE_TYPE"),
            spectre_browser_family=crawler.settings.get("MIMIC_SPECTRE_BROWSER_FAMILY"),
            target_domain=crawler.settings.get("MIMIC_TARGET_DOMAIN"),
        )
        middleware.render_all = crawler.settings.getbool("MIMIC_ALL_REQUESTS", False)
        middleware.crawler = crawler

        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)

        return middleware

    def _get_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def spider_opened(self, spider):
        """Create browser session when spider opens."""
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

            if self.browser_engine:
                body["browser_engine"] = self.browser_engine
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

            spider.mimic_session_id = self.session_id
            spider.mimic_ws_endpoint = self.ws_endpoint

        except httpx.HTTPStatusError as e:
            logger.error(f"MimicMiddleware: Session create failed: {e.response.text}")
        except Exception as e:
            logger.error(f"MimicMiddleware: Error creating session: {e}")

    async def spider_closed(self, spider):
        """Close browser session when spider closes."""
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
            finally:
                if self._client:
                    await self._client.aclose()
                    self._client = None

    _SKIP_PATTERNS = (".xml", "/robots.txt", "/sitemap")

    async def process_request(self, request, spider):
        """Intercept requests that need browser rendering."""
        needs_browser = (
            self.render_all
            or request.meta.get("playwright", False)
            or request.meta.get("mimic", False)
            or request.meta.get("stealth", False)
        )

        if not needs_browser:
            return None

        # Skip sitemap/robots unless explicitly flagged
        url_lower = request.url.lower()
        if any(p in url_lower for p in self._SKIP_PATTERNS):
            if not request.meta.get("mimic_sitemap"):
                return None

        if not self.session_id:
            await self.spider_opened(spider)

        if not self.session_id:
            logger.warning("MimicMiddleware: No session, falling back")
            return None

        request.meta["mimic_session_id"] = self.session_id
        request.meta["mimic_browser"] = True

        try:
            client = await self._get_client()
            action_url = f"{self.service_url}/api/session/{self.session_id}/action"

            # Read wait settings from request meta, crawler settings, or defaults
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

            if not html:
                # Fallback to /api/fetch
                fetch_resp = await client.post(
                    f"{self.service_url}/api/fetch",
                    json={
                        "url": request.url,
                        "session_id": self.session_id,
                        "stealth_level": self.stealth_level,
                    },
                    headers=self._get_headers(),
                    timeout=60.0,
                )
                fetch_resp.raise_for_status()
                fetch_data = fetch_resp.json()
                html = fetch_data.get("html") or fetch_data.get("content") or ""
                status = fetch_data.get("status_code", 200)

            self._consecutive_failures = 0

            return HtmlResponse(
                url=request.url,
                status=status,
                body=html.encode("utf-8") if isinstance(html, str) else html,
                encoding="utf-8",
                request=request,
            )

        except Exception as e:
            logger.error(f"MimicMiddleware: Failed {request.url}: {e}")

            # Recreate session after consecutive failures (dead browser)
            self._consecutive_failures = getattr(self, "_consecutive_failures", 0) + 1
            if self._consecutive_failures >= 3:
                logger.warning(
                    f"MimicMiddleware: {self._consecutive_failures} consecutive "
                    f"failures, recreating session"
                )
                await self.spider_closed(spider)
                self.session_id = None
                self._consecutive_failures = 0
                await self.spider_opened(spider)

            return None

    async def process_response(self, request, response, spider):
        """Detect blocks and potentially upgrade stealth level."""
        if not request.meta.get("mimic_browser"):
            return response

        blocked = False
        if response.status in (403, 429, 503):
            blocked = True
        elif hasattr(response, "body"):
            body_lower = response.body.lower()
            if b"captcha" in body_lower or (
                b"blocked" in body_lower and b"access" in body_lower
            ):
                blocked = True

        if blocked:
            logger.warning(
                f"MimicMiddleware: Possible block at {request.url} "
                f"(status: {response.status})"
            )
            if self.stealth_level != "maximum":
                logger.info("MimicMiddleware: Upgrading to maximum stealth")
                self.stealth_level = "maximum"
                await self.spider_closed(spider)
                self.session_id = None
                await self.spider_opened(spider)

        return response

    async def process_exception(self, request, exception, spider):
        if request.meta.get("mimic_browser"):
            logger.error(f"MimicMiddleware: Exception for {request.url}: {exception}")
        return None
