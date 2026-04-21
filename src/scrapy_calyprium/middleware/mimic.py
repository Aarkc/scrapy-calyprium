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
        browser path on unrecoverable failure. Default: False.
    MIMIC_LOCAL_PRESET: TLS preset for local fetches (default: chrome-143)
    MIMIC_LOCAL_PROXY_URL: outbound proxy URL for local fetches (optional)
"""

import logging
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse, Response

# AAR-17: optional local-first routing components. Imported lazily so the
# middleware still works for users who haven't installed scrapy-calyprium[local].
try:
    from scrapy_calyprium.routing.auto import SpiderAutoRouter, RouteResult
    from scrapy_calyprium.routing.domain_cache import DomainCache
    from scrapy_calyprium.routing.local_fetch import (
        LocalFetcher,
        is_local_fetch_available,
    )
    from scrapy_calyprium.routing.solve_client import SolveClient
    from scrapy_calyprium.routing.slot_stats import SlotStatsReporter
    _HAS_LOCAL_ROUTING = True
except ImportError:
    _HAS_LOCAL_ROUTING = False

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

        # AAR-17: local-first routing state — initialized in spider_opened
        # if MIMIC_LOCAL_FETCH=True and the [local] extra is installed.
        self._local_router: Optional["SpiderAutoRouter"] = None
        self._local_cache: Optional["DomainCache"] = None
        self._solve_client: Optional["SolveClient"] = None
        self._slot_stats_reporter: Optional["SlotStatsReporter"] = None
        self._local_enabled: bool = False
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

    def _get_headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    def _init_local_router(self, spider):
        """AAR-17: stand up the local-first routing components."""
        if not self._local_enabled or self._local_router is not None:
            return
        try:
            preset = self.crawler.settings.get("MIMIC_LOCAL_PRESET", "chrome-143")
            proxy_url = self.crawler.settings.get("MIMIC_LOCAL_PROXY_URL")
            timeout = self.crawler.settings.getint("DOWNLOAD_TIMEOUT", 60)
            self._local_cache = DomainCache()
            fetcher = LocalFetcher(default_preset=preset, timeout=timeout)
            self._solve_client = SolveClient(
                service_url=self.service_url,
                api_key=self.api_key,
                service_secret=self.crawler.settings.get("FORGE_SERVICE_SECRET")
                or self.crawler.settings.get("MIMIC_SERVICE_SECRET"),
                user_id=self.crawler.settings.get("FORGE_USER_ID")
                or self.crawler.settings.get("MIMIC_USER_ID")
                or self.crawler.settings.get("RECRAWL_USER_ID"),
            )
            # Look for CalypriumRequestTracer extension if active
            tracer = None
            if hasattr(self.crawler, 'extensions') and self.crawler.extensions:
                for ext in self.crawler.extensions.middlewares:
                    if hasattr(ext, 'record_span'):
                        tracer = ext
                        break

            self._local_router = SpiderAutoRouter(
                fetcher=fetcher,
                cache=self._local_cache,
                solve_client=self._solve_client,
                proxy_url=proxy_url,
                provider=self.crawler.settings.get("VEIL_PROVIDER"),
                tracer=tracer,
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
        """Create browser session when spider opens."""
        # AAR-17: stand up the local-first router (no-op if disabled)
        self._init_local_router(spider)

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
            finally:
                if self._client:
                    await self._client.aclose()
                    self._client = None

    _SKIP_PATTERNS = (".xml", "/robots.txt", "/sitemap")

    @staticmethod
    def _domain_for(request) -> str:
        try:
            return urlparse(request.url).netloc
        except Exception:
            return ""

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

        try:
            result = await self._local_router.fetch(request.url, domain=domain)
        except Exception as e:
            logger.warning(
                f"MimicMiddleware: local route raised for {request.url}: {e}; "
                f"falling back to legacy"
            )
            self._local_stats["fallback_legacy"] += 1
            return None

        if result.needs_legacy_fallback:
            self._local_stats["fallback_legacy"] += 1
            logger.debug(
                f"MimicMiddleware: local route fell through for {domain} "
                f"({result.routing_method}, error={result.error})"
            )
            return None

        if result.fetch is None or result.blocked:
            self._local_stats["local_blocked"] += 1
            return None

        # Success — wrap as a Scrapy Response with raw bytes (AAR-12 fix point)
        if result.routing_method == "solve_then_replay":
            self._local_stats["local_solve"] += 1
        else:
            self._local_stats["local_success"] += 1

        request.meta["mimic_local_route"] = result.routing_method
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
        ).lower()
        body = result.fetch.body
        url = result.fetch.final_url or request.url
        status = result.fetch.status_code
        headers = result.fetch.headers
        if "text/html" in ct or "application/xhtml" in ct:
            from scrapy.http import HtmlResponse
            encoding = "utf-8"
            if "charset=" in ct:
                encoding = ct.split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
            return HtmlResponse(
                url=url, status=status, headers=headers, body=body,
                encoding=encoding, request=request,
            )
        if "text/" in ct or "json" in ct or "xml" in ct or "javascript" in ct:
            from scrapy.http import TextResponse
            encoding = "utf-8"
            if "charset=" in ct:
                encoding = ct.split("charset=", 1)[1].split(";")[0].strip() or "utf-8"
            return TextResponse(
                url=url, status=status, headers=headers, body=body,
                encoding=encoding, request=request,
            )
        return Response(
            url=url, status=status, headers=headers, body=body, request=request,
        )

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

    async def _fetch_auto(self, client, request):
        """Fetch via /api/fetch with auto-routing (httpcloak → browser)."""
        fetch_payload = {
            "url": request.url,
            "stealth_level": self.stealth_level,
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

        self._consecutive_failures = 0

        return HtmlResponse(
            url=request.url,
            status=status,
            body=html.encode("utf-8") if isinstance(html, str) else html,
            encoding="utf-8",
            request=request,
        )

    async def _fetch_browser(self, client, request, spider):
        """Fetch via browser session for full JS rendering."""
        if not self.session_id:
            await self.spider_opened(spider)

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

    async def process_response(self, request, response, spider):
        """Detect blocks and potentially upgrade stealth level.

        AAR-5: previously this used a substring match on `b"blocked" in body
        and b"access" in body` which fired on every legitimate page that
        happened to contain both words (DigiKey product pages contain
        "access" 100+ times in markup, "blocked" twice in legal text).
        Every successful response triggered a stealth-level upgrade and
        session reset, churning the spider for no reason.

        Now delegates to the AAR-15 block_detect.is_blocked() helper which
        looks for real challenge markers (cf-browser-verification, just a
        moment, datadome, hcaptcha, etc.) plus the status-code rules.
        """
        if not request.meta.get("mimic_browser"):
            return response

        blocked = False
        if response.status in (403, 429, 503):
            blocked = True
        elif hasattr(response, "body"):
            try:
                from scrapy_calyprium.routing.block_detect import is_blocked
                blocked = is_blocked(response.status, response.body)
            except ImportError:
                # Optional [local] extra not installed — skip detection.
                # Pages will only be flagged on hard status codes above.
                pass

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
