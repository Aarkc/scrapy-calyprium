"""
Veil Proxy Middleware for Scrapy.

Routes all spider requests through the Veil proxy gateway for IP rotation
and geo-targeting. Supports datacenter, residential, and rotating proxies.

Settings:
    VEIL_GATEWAY_URL: Proxy gateway URL (default: auto-detected)
    CALYPRIUM_API_KEY: API key for authentication (required)
    VEIL_USER_ID: User ID for proxy routing (required)
    VEIL_PROFILE: Optional profile ID for custom routing rules
    VEIL_PROXY_TYPE: Optional proxy type (datacenter, residential, residential_rotating)
"""

import base64
import logging
from typing import Optional

from scrapy import signals
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)


def basic_auth_header(username: str, password: str) -> bytes:
    """Generate a Basic auth header value."""
    credentials = f"{username}:{password}"
    encoded = base64.b64encode(credentials.encode("utf-8"))
    return b"Basic " + encoded


class VeilProxyMiddleware:
    """
    Scrapy downloader middleware that routes all requests through
    the Veil proxy gateway.

    Requests are authenticated via Basic auth with the user ID and API key
    encoded in the proxy credentials. Optional proxy type and profile
    parameters are encoded in the username.
    """

    PROXY_TYPE_MAP = {
        "datacenter": "dc",
        "residential": "res",
        "residential_rotating": "res",
    }

    def __init__(
        self,
        gateway_url: str,
        api_key: str,
        user_id: str,
        profile: Optional[str] = None,
        proxy_type: Optional[str] = None,
    ):
        self.gateway_url = gateway_url
        self.api_key = api_key
        self.user_id = user_id
        self.profile = profile
        self.proxy_type = proxy_type

    @classmethod
    def from_crawler(cls, crawler):
        api_key = (
            crawler.settings.get("CALYPRIUM_API_KEY")
            or crawler.settings.get("VEIL_API_KEY")
        )
        user_id = crawler.settings.get("VEIL_USER_ID")

        if not api_key or not user_id:
            raise NotConfigured(
                "VeilProxyMiddleware requires an API key (CALYPRIUM_API_KEY or "
                "VEIL_API_KEY) and VEIL_USER_ID. "
                "Set them in settings.py or use scrapy_calyprium.configure()."
            )

        from scrapy_calyprium._config import get_config
        config = get_config()

        middleware = cls(
            gateway_url=crawler.settings.get(
                "VEIL_GATEWAY_URL", config.veil_url or "https://proxy.calyprium.com"
            ),
            api_key=api_key,
            user_id=user_id,
            profile=crawler.settings.get("VEIL_PROFILE"),
            proxy_type=crawler.settings.get("VEIL_PROXY_TYPE"),
        )
        crawler.signals.connect(
            middleware.spider_opened, signal=signals.spider_opened
        )
        return middleware

    def spider_opened(self, spider):
        logger.info(
            f"VeilProxyMiddleware: gateway={self.gateway_url}, "
            f"user={self.user_id}, profile={self.profile or 'default'}"
        )

    def process_request(self, request, spider):
        """Route request through the Veil proxy gateway."""
        request.meta["proxy"] = self.gateway_url

        # Build username with optional proxy type parameter
        username = self.user_id
        if self.proxy_type and self.proxy_type in self.PROXY_TYPE_MAP:
            username = f"{username}-type_{self.PROXY_TYPE_MAP[self.proxy_type]}"

        request.headers["Proxy-Authorization"] = basic_auth_header(
            username, self.api_key
        )

        if self.profile:
            request.headers["X-Veil-Profile"] = self.profile

        return None
