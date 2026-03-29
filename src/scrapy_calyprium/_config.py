"""
Configuration management for scrapy-calyprium.

Resolves credentials and service URLs from multiple sources:
1. Explicit arguments to configure()
2. Environment variables (CALYPRIUM_API_KEY, etc.)
3. Credentials file (~/.calyprium/credentials)
4. Scrapy settings (VEIL_API_KEY, MIMIC_SERVICE_URL, etc.)

Usage::

    # Option 1: configure() in settings.py (recommended)
    import scrapy_calyprium
    scrapy_calyprium.configure(api_key="caly_...")

    # Option 2: Environment variables
    export CALYPRIUM_API_KEY=caly_...

    # Option 3: Scrapy settings directly
    VEIL_API_KEY = "..."
    MIMIC_SERVICE_URL = "https://mimic.calyprium.com"
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


# Default service URLs for SaaS (standalone) users.
# Self-hosted users override via env vars or configure().
_SAAS_DEFAULTS = {
    "veil_url": "https://veil.calyprium.com",
    "mimic_url": "https://mimic.calyprium.com",
    "spectre_url": "https://spectre.calyprium.com",
    "prism_url": "https://prism.calyprium.com",
}

# Docker-internal defaults for self-hosted / platform deployments.
_DOCKER_DEFAULTS = {
    "veil_url": "http://proxy-gateway:8080",
    "mimic_url": "http://mimic:8005",
    "spectre_url": "http://spectre:8005",
    "prism_url": "http://calyprium-prism:8008",
}


def _is_docker() -> bool:
    """Detect if running inside a Docker container."""
    return (
        os.path.exists("/.dockerenv")
        or os.getenv("KUBERNETES_SERVICE_HOST") is not None
    )


@dataclass
class CalypriumConfig:
    """Configuration for Calyprium services."""

    # Master API key (accepted by all services)
    api_key: Optional[str] = None

    # Per-service credentials (override api_key)
    veil_api_key: Optional[str] = None
    veil_user_id: Optional[str] = None
    veil_url: Optional[str] = None
    veil_profile: Optional[str] = None

    mimic_url: Optional[str] = None
    mimic_api_key: Optional[str] = None
    mimic_user_id: Optional[str] = None
    mimic_stealth_level: str = "moderate"

    spectre_url: Optional[str] = None
    spectre_api_key: Optional[str] = None
    spectre_user_id: Optional[str] = None

    prism_url: Optional[str] = None

    # httpcloak settings
    httpcloak_preset: str = "chrome-143"
    httpcloak_timeout: int = 30

    # User-Agent to use with httpcloak
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    )

    def resolve(self) -> "CalypriumConfig":
        """Fill in missing values from environment variables and defaults."""
        defaults = _DOCKER_DEFAULTS if _is_docker() else _SAAS_DEFAULTS

        # Master API key
        if not self.api_key:
            self.api_key = os.getenv("CALYPRIUM_API_KEY")

        # Veil
        if not self.veil_api_key:
            self.veil_api_key = os.getenv("VEIL_API_KEY") or self.api_key
        if not self.veil_user_id:
            self.veil_user_id = os.getenv("VEIL_USER_ID")
        if not self.veil_url:
            self.veil_url = os.getenv(
                "VEIL_GATEWAY_URL", defaults["veil_url"]
            )

        # Mimic
        if not self.mimic_api_key:
            self.mimic_api_key = os.getenv("MIMIC_API_KEY") or self.api_key
        if not self.mimic_user_id:
            self.mimic_user_id = os.getenv("MIMIC_USER_ID")
        if not self.mimic_url:
            self.mimic_url = os.getenv(
                "MIMIC_SERVICE_URL", defaults["mimic_url"]
            )

        # Spectre
        if not self.spectre_api_key:
            self.spectre_api_key = os.getenv("SPECTRE_API_KEY") or self.api_key
        if not self.spectre_user_id:
            self.spectre_user_id = os.getenv("SPECTRE_USER_ID")
        if not self.spectre_url:
            self.spectre_url = os.getenv(
                "SPECTRE_SERVICE_URL", defaults["spectre_url"]
            )

        # Prism
        if not self.prism_url:
            self.prism_url = os.getenv("PRISM_URL", defaults["prism_url"])

        return self

    def to_scrapy_settings(self) -> dict:
        """Convert to a dict of Scrapy settings."""
        settings = {}

        # Middleware stack — Veil handles proxy routing, Mimic handles
        # browser rendering with httpcloak and Spectre internally.
        settings["DOWNLOADER_MIDDLEWARES"] = {
            "scrapy_calyprium.middleware.veil.VeilProxyMiddleware": 100,
            "scrapy_calyprium.middleware.mimic.MimicBrowserMiddleware": 200,
        }

        # Master API key (used by all middleware as fallback)
        if self.api_key:
            settings["CALYPRIUM_API_KEY"] = self.api_key

        # Veil
        if self.veil_url:
            settings["VEIL_GATEWAY_URL"] = self.veil_url
        if self.veil_api_key:
            settings["VEIL_API_KEY"] = self.veil_api_key
        if self.veil_user_id:
            settings["VEIL_USER_ID"] = self.veil_user_id
        if self.veil_profile:
            settings["VEIL_PROFILE"] = self.veil_profile

        # Mimic
        if self.mimic_url:
            settings["MIMIC_SERVICE_URL"] = self.mimic_url
        if self.mimic_api_key:
            settings["MIMIC_API_KEY"] = self.mimic_api_key
        settings["MIMIC_STEALTH_LEVEL"] = self.mimic_stealth_level
        settings["MIMIC_USE_SPECTRE"] = True

        # Prism
        if self.prism_url:
            settings["PRISM_URL"] = self.prism_url

        return settings


# Module-level singleton
_config: Optional[CalypriumConfig] = None


def configure(
    api_key: Optional[str] = None,
    **kwargs,
) -> CalypriumConfig:
    """Configure scrapy-calyprium and return the resolved config.

    Call this in your Scrapy settings.py to auto-populate all middleware
    settings. Any Scrapy settings you define after this call will override
    the values set by configure().

    Args:
        api_key: Master API key for all Calyprium services.
        **kwargs: Any CalypriumConfig field (veil_url, mimic_stealth_level, etc.)

    Returns:
        Resolved CalypriumConfig instance.

    Example::

        # settings.py
        import scrapy_calyprium

        scrapy_calyprium.configure(api_key="caly_...")

        # Override specific settings after configure():
        CONCURRENT_REQUESTS = 8
    """
    global _config
    _config = CalypriumConfig(api_key=api_key, **kwargs).resolve()

    # Inject settings into the calling module's namespace.
    # This works because settings.py is executed as a module, and we can
    # write into the caller's globals.
    import inspect
    frame = inspect.currentframe()
    if frame and frame.f_back:
        caller_globals = frame.f_back.f_globals
        for key, value in _config.to_scrapy_settings().items():
            # Only set if not already defined by the user
            if key not in caller_globals:
                caller_globals[key] = value

    return _config


def get_config() -> CalypriumConfig:
    """Get the current config, creating a default one if needed."""
    global _config
    if _config is None:
        _config = CalypriumConfig().resolve()
    return _config
