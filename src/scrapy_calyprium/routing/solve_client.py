"""Async client for Mimic's POST /api/solve endpoint.

AAR-17. Used by the spider-side auto-routing logic when local httpcloak hits
a challenge and we need a real browser solve to earn fresh clearance cookies.
"""
from __future__ import annotations

import base64
import binascii
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

from scrapy_calyprium import _trace
from scrapy_calyprium._policy import RunPolicy

logger = logging.getLogger(__name__)

#: Mimic's ``SOLVE_DEADLINE_S``: how long one /api/solve may run, every fresh-IP
#: browser attempt included. A run whose Mimic is configured otherwise says so
#: with the ``MIMIC_SOLVE_DEADLINE_S`` setting.
DEFAULT_SOLVE_DEADLINE_S = 240.0
#: Waited on top of the deadline: Mimic checks it only between attempts, so
#: the last attempt (and the reply) can run past it.
SOLVE_DEADLINE_MARGIN_S = 30.0
#: A solve service that cannot even be reached is not solving.
SOLVE_CONNECT_TIMEOUT_S = 15.0


def solve_timeout_from_settings(settings: Any) -> float:
    """Seconds to wait for /api/solve: ``MIMIC_SOLVE_DEADLINE_S`` (Mimic's
    ``SOLVE_DEADLINE_S``, default 240) plus a margin. Waiting less abandons
    solves Mimic is still running: they pile up on its browser pool while the
    spider retries (the old fixed 90 s did that)."""
    deadline = DEFAULT_SOLVE_DEADLINE_S
    if settings is not None:
        try:
            deadline = float(settings.getfloat("MIMIC_SOLVE_DEADLINE_S", DEFAULT_SOLVE_DEADLINE_S))
        except (AttributeError, TypeError, ValueError):
            deadline = DEFAULT_SOLVE_DEADLINE_S
    if deadline <= 0:
        deadline = DEFAULT_SOLVE_DEADLINE_S
    return deadline + SOLVE_DEADLINE_MARGIN_S


def _country(value: Any) -> Optional[str]:
    """A solve's exit country as the gateway username takes it (``country_<cc>``), or None."""
    text = str(value or "").strip().lower()
    return text if re.fullmatch(r"[a-z0-9_]{2,16}", text) else None


def _describe(exc: BaseException) -> str:
    """The exception's type and message: httpx's timeouts carry no message, and
    a bare "transport error:" said nothing."""
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


@dataclass
class SolvedResponse:
    """The response a challenge stood in for, as the solve fetched it (Akamai's challenge action: the
    wrapped request posted again): handed on as the request's response."""

    status: int
    url: str
    headers: Dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    set_cookies: List[str] = field(default_factory=list)


def _solved_response(data: Any) -> Optional[SolvedResponse]:
    """A solve response's ``response`` object, or None (absent or unreadable)."""
    if not isinstance(data, dict):
        return None
    try:
        body = base64.b64decode(data.get("body_b64") or "", validate=True)
        return SolvedResponse(
            status=int(data.get("status") or 0),
            url=str(data.get("url") or ""),
            headers={str(k): str(v) for k, v in (data.get("headers") or {}).items()},
            body=body,
            set_cookies=[str(line) for line in (data.get("set_cookies") or [])],
        )
    except (binascii.Error, TypeError, ValueError):
        return None


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
    # Exit country the solve left through (Mimic's SolveResponse.country): replays
    # leave through the same one (gateway ``-country_<cc>``), or the clearance meets
    # another IP. None when the solver does not say (the run's country then holds).
    country: Optional[str] = None
    # The response the challenge stood in for (``akamai_captcha``), when the solve fetched it.
    response: Optional[SolvedResponse] = None


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
        timeout: float = DEFAULT_SOLVE_DEADLINE_S + SOLVE_DEADLINE_MARGIN_S,
        ip_health_url: Optional[str] = None,
        policy: Optional[RunPolicy] = None,
        backend: Optional[str] = None,
    ):
        # Profile policy (AAR-62): spider_id / country ride on every solve and
        # engine_hint is clamped to the allow-list.
        self.policy = policy or RunPolicy()
        # "tessera" or "mimic": which /api/solve this is. Their ``provider`` fields differ (Tessera: the
        # solver to force; Mimic: the Veil provider of the exit), which a solve on a slot's own exit minds.
        self.backend = backend
        self.service_url = service_url.rstrip("/")
        # IP-health reputation lives in mimic; when /api/solve is pointed at a
        # separate solver service (Tessera), reports still go to mimic.
        self.ip_health_url = (ip_health_url or service_url).rstrip("/")
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
        elif self.service_secret:
            # Legacy: only without a spider key (AAR-32 — never ship the
            # master secret alongside a key that already authenticates).
            headers["X-Service-Secret"] = self.service_secret
        if self.user_id:
            headers["X-User-Id"] = self.user_id
            headers["X-Service-Name"] = "scrapy-calyprium"
        # the download attempt that needed the solve: Mimic's and Tessera's spans join its trace
        headers.update(_trace.headers())
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(
                self.timeout, connect=min(self.timeout, SOLVE_CONNECT_TIMEOUT_S)))
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
        solvers: Optional[List[str]] = None,
        challenge_type: Optional[str] = None,
        page_html: Optional[str] = None,
        cookies: Optional[List[Dict]] = None,
        user_agent: Optional[str] = None,
        country: Optional[str] = None,
        paid: bool = False,
        deadline_ms: Optional[int] = None,
    ) -> SolveResult:
        """Call /api/solve and return the cookies (or a structured failure).

        ``challenge_type`` names the vendor the block showed (``challenge_family``):
        Tessera then runs that vendor's solvers instead of assuming Cloudflare.
        Mimic's own /api/solve ignores it (its browser passes any vendor), except
        ``akamai_captcha``: a challenge met on the caller's own session, answered on
        it. Such a solve sends the challenge page (``page_html``), the session's
        ``cookies`` and ``user_agent``, its exit (``proxy_session_id``, ``provider``,
        ``country``), the run's paid-solve policy (``paid``) and a ``deadline_ms``;
        its result carries the request's ``response``."""
        client = await self._get_client()
        body = {"domain": domain, **self.policy.solve_fields()}
        if country:
            body["country"] = country
        if paid:
            body.update(self.policy.paid_solve_fields())
        if page_html:
            body["page_html"] = page_html
        if cookies:
            body["cookies"] = [
                {"name": c["name"], "value": c["value"], "domain": c.get("domain") or f".{domain}",
                 "path": c.get("path") or "/"}
                for c in cookies if c.get("name") and c.get("value") is not None
            ]
        if user_agent:
            body["user_agent"] = user_agent
        if deadline_ms:
            body["deadline_ms"] = int(deadline_ms)
        if target_url:
            body["target_url"] = target_url
        engine_hint = self.policy.clamp_engine(engine_hint)
        if engine_hint:
            body["engine_hint"] = engine_hint
        if proxy_session_id:
            body["proxy_session_id"] = proxy_session_id
        if provider:
            body["provider"] = provider
        # Ordered captcha-solver allowlist (e.g. ["jevi"]). Keeps the solve off
        # providers that can't handle this domain (whose long failure timeouts
        # would blow our read budget); we drive retries with fresh IPs instead.
        if solvers:
            body["solvers"] = solvers
        if challenge_type:
            body["challenge_type"] = challenge_type

        url = f"{self.service_url}/api/solve"
        started = time.monotonic()
        try:
            response = await client.post(url, json=body, headers=self._build_headers())
        except httpx.HTTPError as exc:
            reason = _describe(exc)
            logger.warning(
                "SolveClient: transport error for %s after %.0fs (timeout %.0fs): %s",
                domain, time.monotonic() - started, self.timeout, reason,
            )
            raise SolveError(f"transport error: {reason}") from exc

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
            country=_country(data.get("country")),
            response=_solved_response(data.get("response")),
            error=data.get("error") if not data.get("success", True) else None,
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
        url = f"{self.ip_health_url}/api/ip-health/report"
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
