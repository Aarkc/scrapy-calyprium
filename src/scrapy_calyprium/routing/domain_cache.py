"""Per-spider domain cache for local-first auto-routing.

A lightweight in-memory cache that tracks, per domain:

- the routing level (light / cookies / heavy)
- one or more clearance cookie slots earned from Mimic /api/solve
- per-slot fail counts and the AAR-14 circuit breaker semantics

This is the spider-side counterpart of `mimic.routing.domain_cache.DomainDefenseCache`.
We deliberately keep it simpler than the server version — no learned RPM caps,
no cross-domain IP health (the spider can call /api/ip-health/check for that
when it matters). The goal is to make decisions about which cookies to replay
and when to ask Mimic for a new solve.

Persisted to disk in JOBDIR (planned follow-up) so cookies survive spider
restarts on Scrapyd job resume.

AAR-17.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


# Cookie pool tunables — match server-side defaults so behavior is consistent.
TTL_LIGHT = 3600
TTL_COOKIES = 1800
TTL_HEAVY = 21600
MAX_SLOT_FAILURES = 3
MAX_SLOTS_PER_DOMAIN = 8

# AAR-14 circuit breaker tunables
PROMOTION_COOLDOWN_SECONDS = 300
MIN_DOMAIN_FAILURES_FOR_PROMOTION = 3
HEAVY_REPROBE_INITIAL_SECONDS = 600
HEAVY_REPROBE_MAX_SECONDS = 3600


def _now() -> float:
    return time.time()


@dataclass
class CookieSlot:
    """One set of clearance cookies bound to a sticky proxy session.

    Tracks per-slot request timestamps in a 60s window so the parent
    DomainEntry can compute slot RPM and enforce a learned per-slot rate cap.
    """
    slot_id: str
    cookies: List[Dict]
    user_agent: str
    proxy_session_id: str
    preset: str = "chrome-latest"
    created_at: float = field(default_factory=_now)
    fail_count: int = 0
    success_count: int = 0
    block_count: int = 0
    # Resolved physical egress IP for this slot, populated from the
    # /api/solve response. Reported back to Mimic on slot failures so the
    # server-side per-(domain, ip) reputation tracker rotates around
    # burned IPs across the whole spider fleet. None means Mimic
    # couldn't resolve it (no proxy creds, probe failed) — the slot is
    # still usable, it just contributes no reputation signal.
    egress_ip: Optional[str] = None
    _request_times: List[float] = field(default_factory=list)

    @property
    def is_expired(self) -> bool:
        return (_now() - self.created_at) > TTL_COOKIES

    @property
    def is_live(self) -> bool:
        return not self.is_expired and self.fail_count < MAX_SLOT_FAILURES

    def record_request(self) -> None:
        """Track a request timestamp for per-slot RPM calculation."""
        now = _now()
        self._request_times.append(now)
        # Trim entries older than 60s
        cutoff = now - 60
        if len(self._request_times) > 10 and self._request_times[0] < cutoff:
            self._request_times = [t for t in self._request_times if t > cutoff]

    def requests_per_minute(self) -> int:
        cutoff = _now() - 60
        return sum(1 for t in self._request_times if t > cutoff)

    def to_dict(self) -> Dict:
        return {
            "slot_id": self.slot_id,
            "cookies": self.cookies,
            "user_agent": self.user_agent,
            "proxy_session_id": self.proxy_session_id,
            "preset": self.preset,
            "created_at": self.created_at,
            "fail_count": self.fail_count,
            "success_count": self.success_count,
            "block_count": self.block_count,
            "egress_ip": self.egress_ip,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "CookieSlot":
        # request_times is intentionally not persisted — it's a rolling window
        # that gets rebuilt from live traffic after restart.
        return cls(**{k: v for k, v in data.items() if k != "_request_times"})


@dataclass
class DomainEntry:
    level: str  # "light" | "cookies" | "heavy"
    updated_at: float = field(default_factory=_now)
    ttl: float = float(TTL_LIGHT)
    slots: List[CookieSlot] = field(default_factory=list)
    _robin_idx: int = 0
    _domain_failure_count: int = 0
    _last_promotion_attempt: float = 0.0
    _next_reprobe_at: float = 0.0
    _reprobe_backoff: float = float(HEAVY_REPROBE_INITIAL_SECONDS)
    # Adaptive rate limiting (ported from server-side mimic.routing.domain_cache).
    # learned_rpm_cap is the per-slot RPM ceiling we believe is safe before
    # the upstream WAF starts blocking. None = no observations yet.
    learned_rpm_cap: Optional[float] = None
    _block_rpms: List[float] = field(default_factory=list)
    _last_block_time: float = 0.0
    _last_cap_raise_time: float = 0.0

    @property
    def is_expired(self) -> bool:
        if self.level == "cookies" and self.slots:
            return not any(s.is_live for s in self.slots)
        return (_now() - self.updated_at) > self.ttl

    def live_slots(self) -> List[CookieSlot]:
        return [s for s in self.slots if s.is_live]

    def next_slot(self) -> Optional[CookieSlot]:
        """Pick the least-loaded live slot, respecting the learned rate cap."""
        live = self.live_slots()
        if not live:
            return None
        if len(live) == 1:
            return live[0]

        # Sort by current per-slot RPM (ascending — least loaded first)
        sorted_slots = sorted(live, key=lambda s: s.requests_per_minute())

        if self.learned_rpm_cap is not None:
            under_cap = [
                s for s in sorted_slots
                if s.requests_per_minute() < self.learned_rpm_cap
            ]
            if under_cap:
                return under_cap[0]

        return sorted_slots[0]

    def record_block_rpm(self, slot: CookieSlot) -> None:
        """Record the RPM the slot was running at when it got blocked.

        Updates learned_rpm_cap to 70% of the median observed block RPM
        (rolling window of last 10 blocks). Capped at a 5 RPM floor so we
        never throttle below a sane minimum.
        """
        slot_rpm = slot.requests_per_minute()
        self._last_block_time = _now()

        if slot_rpm < 5:
            return  # too low to learn from — probably wasn't rate-limited

        self._block_rpms.append(slot_rpm)
        self._block_rpms = self._block_rpms[-10:]

        sorted_rpms = sorted(self._block_rpms)
        median_rpm = sorted_rpms[len(sorted_rpms) // 2]
        old_cap = self.learned_rpm_cap
        self.learned_rpm_cap = max(5.0, median_rpm * 0.7)

        logger.info(
            "Rate cap updated: %s -> %.0f RPM/slot (blocked at %d RPM, %d obs)",
            old_cap, self.learned_rpm_cap, slot_rpm, len(self._block_rpms),
        )

    def maybe_raise_cap(self) -> None:
        """Gradually raise the cap when no recent blocks."""
        if self.learned_rpm_cap is None:
            return
        now = _now()
        if now - self._last_block_time < 300:
            return
        if now - self._last_cap_raise_time < 180:
            return

        old_cap = self.learned_rpm_cap
        self.learned_rpm_cap *= 1.1
        self._last_cap_raise_time = now
        logger.info(
            "Rate cap raised: %.1f -> %.1f RPM/slot (no blocks for %.0fs)",
            old_cap, self.learned_rpm_cap, now - self._last_block_time,
        )

    def domain_rpm(self) -> int:
        """Aggregate RPM across all live slots."""
        return sum(s.requests_per_minute() for s in self.live_slots())

    def to_dict(self) -> Dict:
        return {
            "level": self.level,
            "updated_at": self.updated_at,
            "ttl": self.ttl,
            "slots": [s.to_dict() for s in self.slots],
            "_robin_idx": self._robin_idx,
            "_domain_failure_count": self._domain_failure_count,
            "_last_promotion_attempt": self._last_promotion_attempt,
            "_next_reprobe_at": self._next_reprobe_at,
            "_reprobe_backoff": self._reprobe_backoff,
            "learned_rpm_cap": self.learned_rpm_cap,
            "_block_rpms": list(self._block_rpms),
            "_last_block_time": self._last_block_time,
            "_last_cap_raise_time": self._last_cap_raise_time,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "DomainEntry":
        slots = [CookieSlot.from_dict(s) for s in data.pop("slots", [])]
        entry = cls(level=data.pop("level"), slots=slots, **data)
        return entry


class DomainCache:
    """In-process domain cache for a single spider.

    Created on `spider_opened`, persisted to JOBDIR on `spider_closed`,
    reloaded on resume.
    """

    def __init__(self):
        self._entries: Dict[str, DomainEntry] = {}

    # ---------------- queries ----------------

    def get(self, domain: str) -> Optional[DomainEntry]:
        entry = self._entries.get(domain)
        if entry and entry.is_expired:
            return None
        return entry

    def get_level(self, domain: str) -> str:
        entry = self.get(domain)
        return entry.level if entry else "unknown"

    def is_due_for_reprobe(self, domain: str) -> bool:
        entry = self._entries.get(domain)
        if not entry or entry.level != "heavy":
            return False
        return _now() >= entry._next_reprobe_at

    # ---------------- mutations ----------------

    def set_light(self, domain: str) -> None:
        self._entries[domain] = DomainEntry(level="light", ttl=float(TTL_LIGHT))

    def set_cookies_from_solve(
        self,
        domain: str,
        cookies: List[Dict],
        user_agent: str,
        proxy_session_id: str,
        preset: str = "chrome-latest",
        egress_ip: Optional[str] = None,
    ) -> CookieSlot:
        """Add a new cookie slot from a Mimic /api/solve response."""
        entry = self._entries.get(domain)
        if not entry or entry.level != "cookies":
            entry = DomainEntry(level="cookies", ttl=float(TTL_COOKIES))
            self._entries[domain] = entry

        slot = CookieSlot(
            slot_id=uuid4().hex[:12],
            cookies=cookies,
            user_agent=user_agent,
            proxy_session_id=proxy_session_id,
            preset=preset,
            egress_ip=egress_ip,
        )
        entry.slots.append(slot)
        if len(entry.slots) > MAX_SLOTS_PER_DOMAIN:
            entry.slots = entry.slots[-MAX_SLOTS_PER_DOMAIN:]
        entry.updated_at = _now()
        logger.debug(
            "DomainCache: added slot for %s (slots=%d, session=%s, egress_ip=%s)",
            domain, len(entry.slots), proxy_session_id, egress_ip,
        )
        return slot

    def record_request(self, domain: str, slot_id: str) -> None:
        """Record an in-flight request against a slot's RPM window.

        Called by SpiderAutoRouter immediately before sending a replay.
        Drives the per-slot RPM the rate cap is computed against, and
        opportunistically tries to raise the cap if things are stable.
        """
        entry = self._entries.get(domain)
        if not entry or entry.level != "cookies":
            return
        slot = next((s for s in entry.slots if s.slot_id == slot_id), None)
        if not slot:
            return
        slot.record_request()
        entry.maybe_raise_cap()

    def record_slot_failure(
        self,
        domain: str,
        slot_id: str,
        status_code: Optional[int] = None,
    ) -> bool:
        """Record a slot failure. Returns True iff promoted to heavy.

        AAR-14 semantics: infrastructure failures (None / 502 / 504 / 520 /
        522 / 524 / connection errors) are NOT counted against the slot.
        Real domain-side blocks (403/429/503) feed the rate-cap learner.
        """
        if _is_infrastructure_failure(status_code):
            logger.debug(
                "DomainCache: ignoring infra failure for %s slot %s (status=%s)",
                domain, slot_id, status_code,
            )
            return False

        entry = self._entries.get(domain)
        if not entry or entry.level != "cookies":
            return False
        slot = next((s for s in entry.slots if s.slot_id == slot_id), None)
        if not slot:
            return False
        slot.fail_count += 1
        slot.block_count += 1
        entry._domain_failure_count += 1

        # Feed the rate-cap learner with the RPM the slot was running at
        # when it got blocked. Source of truth for the spider's adaptive
        # throttle now that local-first replays don't go through Mimic.
        entry.record_block_rpm(slot)

        if entry.live_slots():
            return False
        return self._maybe_promote_heavy(domain, "all_slots_exhausted")

    def record_slot_success(self, domain: str, slot_id: str) -> None:
        entry = self._entries.get(domain)
        if not entry or entry.level != "cookies":
            return
        slot = next((s for s in entry.slots if s.slot_id == slot_id), None)
        if slot:
            slot.success_count += 1
        entry._domain_failure_count = 0
        entry.updated_at = _now()

    def _maybe_promote_heavy(self, domain: str, reason: str) -> bool:
        """AAR-14 circuit-breakered promotion."""
        entry = self._entries.get(domain)
        now = _now()
        cooling_down = (
            entry is not None
            and (now - entry._last_promotion_attempt) < PROMOTION_COOLDOWN_SECONDS
        )
        real_failures = entry._domain_failure_count if entry else 0

        if cooling_down and real_failures < MIN_DOMAIN_FAILURES_FOR_PROMOTION:
            logger.warning(
                "DomainCache: heavy promotion suppressed for %s "
                "(reason=%s, real_failures=%d)", domain, reason, real_failures,
            )
            if entry:
                entry._last_promotion_attempt = now
            return False

        new_entry = DomainEntry(level="heavy", ttl=float(TTL_HEAVY))
        new_entry._last_promotion_attempt = now
        new_entry._next_reprobe_at = now + HEAVY_REPROBE_INITIAL_SECONDS
        self._entries[domain] = new_entry
        logger.warning("DomainCache: %s promoted to HEAVY (reason=%s)", domain, reason)
        return True

    def record_reprobe_result(self, domain: str, success: bool) -> None:
        """Auto-recovery: feedback from a re-probe attempt."""
        entry = self._entries.get(domain)
        if not entry or entry.level != "heavy":
            return
        if success:
            logger.info(
                "DomainCache: re-probe succeeded for %s, downgrading heavy -> light",
                domain,
            )
            self.set_light(domain)
            return
        next_backoff = min(entry._reprobe_backoff * 2, float(HEAVY_REPROBE_MAX_SECONDS))
        entry._reprobe_backoff = next_backoff
        entry._next_reprobe_at = _now() + next_backoff

    # ---------------- serialization ----------------

    def to_dict(self) -> Dict:
        return {d: e.to_dict() for d, e in self._entries.items()}

    @classmethod
    def from_dict(cls, data: Dict) -> "DomainCache":
        cache = cls()
        for domain, entry_data in data.items():
            try:
                cache._entries[domain] = DomainEntry.from_dict(entry_data)
            except Exception as e:
                logger.warning(
                    "DomainCache: skipping unparsable entry for %s: %s", domain, e,
                )
        return cache


# ---------------------------------------------------------------------------
# AAR-14 failure classification (mirrors server-side)
# ---------------------------------------------------------------------------


_INFRA_STATUS_CODES = {502, 504, 520, 522, 524}


def _is_infrastructure_failure(status_code: Optional[int]) -> bool:
    """Distinguish "our infra broke" from "domain blocked us"."""
    if status_code is None or status_code == 0:
        return True
    return status_code in _INFRA_STATUS_CODES
