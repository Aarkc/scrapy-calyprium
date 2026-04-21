"""Async client for Mimic's POST /api/solve endpoint.

AAR-17. Used by the spider-side auto-routing logic when local httpcloak hits
a challenge and we need a real browser solve to earn fresh clearance cookies.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class SolveResult:
    success: bool
    cookies: List[Dict]
    user_agent: str
    proxy_session_id: str
    engine: str
    preset: str
    duration_ms: int
    learned_rpm_cap: Optional[float] = None
    error: Optional[str] = None
    # Resolved physical egress IP behind proxy_session_id, populated by
    # Mimic via a one-shot api.ipify.org probe during the solve. Spider
    # stores it on its CookieSlot and reports it back via /api/ip-health/
    # report on failures so Mimic's per-(domain, ip) reputation tracker
    # can rotate around burned IPs across the whole spider fleet.
    egress_ip: Optional[str] = None
    # Veil provider used for the solve — replay must use the same provider
    provider: Optional[str] = None


class SolveError(Exception):
    """Raised on transport-level / 5xx failures from /api/solve.

    A 403 (real domain block) is *not* raised — it's surfaced as
    `SolveResult(success=False)` so the spider can decide whether to retry.
    """


class SolveClient:
    """Thin async wrapper around `POST {service_url}/api/solve`."""

    def __init__(
        self,
        service_url: str,
        api_key: Optional[str] = None,
        service_secret: Optional[str] = None,
        user_id: Optional[str] = None,
        timeout: float = 90.0,
    ):
        self.service_url = service_url.rstrip("/")
        self.api_key = api_key
        self.service_secret = service_secret
        self.user_id = user_id
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    def _build_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["X-API-Key"] = self.api_key
        if self.service_secret:
            headers["X-Service-Secret"] = self.service_secret
        if self.user_id:
            headers["X-User-Id"] = self.user_id
            headers["X-Service-Name"] = "scrapy-calyprium"
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    async def solve(
        self,
        *,
        domain: str,
        target_url: Optional[str] = None,
        engine_hint: Optional[str] = None,
        proxy_session_id: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> SolveResult:
        """Call /api/solve and return the cookies (or a structured failure)."""
        client = await self._get_client()
        body = {"domain": domain}
        if target_url:
            body["target_url"] = target_url
        if engine_hint:
            body["engine_hint"] = engine_hint
        if proxy_session_id:
            body["proxy_session_id"] = proxy_session_id
        if provider:
            body["provider"] = provider

        url = f"{self.service_url}/api/solve"
        try:
            response = await client.post(url, json=body, headers=self._build_headers())
        except httpx.HTTPError as exc:
            logger.warning("SolveClient: transport error for %s: %s", domain, exc)
            raise SolveError(f"transport error: {exc}") from exc

        if response.status_code == 403:
            try:
                detail = response.json().get("detail", "")
            except Exception:
                detail = response.text[:200]
            return SolveResult(
                success=False,
                cookies=[],
                user_agent="",
                proxy_session_id="",
                engine="",
                preset="chrome-latest",
                duration_ms=0,
                error=detail,
            )

        if response.status_code == 429:
            raise SolveError(
                f"rate-limited by Mimic /api/solve for {domain}; retry-after="
                f"{response.headers.get('Retry-After', '?')}"
            )

        if response.status_code >= 500:
            raise SolveError(
                f"Mimic /api/solve returned {response.status_code} for {domain}: "
                f"{response.text[:200]}"
            )

        if response.status_code != 200:
            raise SolveError(
                f"Mimic /api/solve returned unexpected {response.status_code} "
                f"for {domain}: {response.text[:200]}"
            )

        data = response.json()
        cookies = data.get("cookies", [])
        normalized = [
            {"name": c.get("name"), "value": c.get("value"), "domain": c.get("domain"), "path": c.get("path")}
            for c in cookies
            if c.get("name") and "value" in c
        ]
        return SolveResult(
            success=bool(data.get("success", True)),
            cookies=normalized,
            user_agent=data.get("user_agent", ""),
            proxy_session_id=data.get("proxy_session_id", ""),
            engine=data.get("engine", ""),
            preset=data.get("preset", "chrome-latest"),
            duration_ms=int(data.get("duration_ms", 0)),
            learned_rpm_cap=data.get("learned_rpm_cap"),
            egress_ip=data.get("egress_ip"),
            provider=data.get("provider"),
        )

    async def report_ip_outcome(
        self,
        *,
        proxy_session_id: str,
        domain: str,
        outcome: str,
        status_code: Optional[int] = None,
        egress_ip: Optional[str] = None,
    ) -> None:
        """Fire-and-forget POST to /api/ip-health/report.

        Closes the per-(domain, IP) reputation feedback loop. The spider's
        local httpcloak replay does not flow through Mimic's server-side
        routing, so without this report Mimic only learns about failures
        from its own /api/fetch path — a small fraction of total traffic.
        Reporting here lets Mimic populate its IP blacklist from real
        spider observations and rotate around burned IPs on the next
        solve.

        Errors are swallowed: a failure to report should never affect
        spider throughput. The reputation system is a soft signal.
        """
        if outcome not in ("blocked", "success"):
            logger.warning(
                "SolveClient.report_ip_outcome: invalid outcome %r", outcome,
            )
            return
        client = await self._get_client()
        body: Dict = {
            "proxy_session_id": proxy_session_id,
            "domain": domain,
            "outcome": outcome,
        }
        if status_code is not None:
            body["status_code"] = status_code
        if egress_ip:
            body["egress_ip"] = egress_ip
        url = f"{self.service_url}/api/ip-health/report"
        try:
            response = await client.post(
                url, json=body, headers=self._build_headers(), timeout=5.0,
            )
            if response.status_code >= 400:
                logger.debug(
                    "report_ip_outcome %s/%s -> %d: %s",
                    domain, outcome, response.status_code,
                    response.text[:200],
                )
        except Exception as exc:
            logger.debug(
                "report_ip_outcome %s/%s failed: %s", domain, outcome, exc,
            )
