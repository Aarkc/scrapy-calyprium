"""Veil gateway proxy credentials.

The Veil gateway authenticates with HTTP Basic proxy auth and reads routing
parameters out of the username::

    <user>[-<key>_<value>]...:<password>

Keys understood by the gateway's ``ConfigResolver.PARAM_MAPPING`` include
``p`` (provider), ``type``, ``session`` (sticky IP), ``spider`` (billing
attribution), ``ap`` (allowed providers, dot-joined) and ``country``.

Local-first proxy URL (``MIMIC_LOCAL_PROXY_URL``)
-------------------------------------------------
The in-process httpcloak fetcher needs a proxy URL with embedded credentials.
If the platform does not inject ``MIMIC_LOCAL_PROXY_URL``, the SDK derives it
from ``VEIL_GATEWAY_URL`` and the run's own spider key::

    http://calyprium:<url-quoted CALYPRIUM_API_KEY>@<gateway host>:<port>

The fetcher then appends ``-p_<provider>-session_<id>`` to the username per
cookie slot, so replays stay pinned to the IP the clearance was solved on.
"""
from __future__ import annotations

from typing import Any, Optional
from urllib.parse import quote, urlparse

LOCAL_PROXY_USERNAME = "calyprium"


def _get(settings: Any, key: str) -> Optional[str]:
    try:
        return settings.get(key) if settings is not None else None
    except AttributeError:
        return None


def build_local_proxy_url(
    gateway_url: Optional[str],
    api_key: Optional[str],
    username: str = LOCAL_PROXY_USERNAME,
) -> Optional[str]:
    """``scheme://<username>:<api_key>@host[:port]`` or None if unbuildable."""
    if not gateway_url or not api_key:
        return None
    parsed = urlparse(gateway_url)
    if not parsed.hostname:
        return None
    netloc = f"{username}:{quote(api_key, safe='')}@{parsed.hostname}"
    if parsed.port:
        netloc += f":{parsed.port}"
    return f"{parsed.scheme or 'http'}://{netloc}"


def resolve_local_proxy_url(settings: Any) -> Optional[str]:
    """``MIMIC_LOCAL_PROXY_URL`` if set, else derived from the gateway + key."""
    explicit = _get(settings, "MIMIC_LOCAL_PROXY_URL")
    if explicit:
        return explicit
    api_key = _get(settings, "CALYPRIUM_API_KEY") or _get(settings, "VEIL_API_KEY")
    return build_local_proxy_url(_get(settings, "VEIL_GATEWAY_URL"), api_key)
