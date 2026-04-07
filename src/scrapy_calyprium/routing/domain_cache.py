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
    """One set of clearance cookies bound to a sticky proxy session."""
    slot_id: str
    cookies: List[Dict]
    user_agent: str
    proxy_session_id: str
    preset: str = "chrome-latest"
    created_at: float = field(default_factory=_now)
    fail_count: int = 0

    @property
    def is_expired(self) -> bool:
        return (_now() - self.created_at) > TTL_COOKIES

    @property
    def is_live(self) -> bool:
        return not self.is_expired and self.fail_count < MAX_SLOT_FAILURES

    def to_dict(self) -> Dict:
        return {
            "slot_id": self.slot_id,
            "cookies": self.cookies,
            "user_agent": self.user_agent,
            "proxy_session_id": self.proxy_session_id,
            "preset": self.preset,
            "created_at": self.created_at,
            "fail_count": self.fail_count,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "CookieSlot":
        return cls(**data)


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

    @property
    def is_expired(self) -> bool:
        if self.level == "cookies" and self.slots:
            return not any(s.is_live for s in self.slots)
        return (_now() - self.updated_at) > self.ttl

    def live_slots(self) -> List[CookieSlot]:
        return [s for s in self.slots if s.is_live]

    def next_slot(self) -> Optional[CookieSlot]:
        live = self.live_slots()
        if not live:
            return None
        slot = live[self._robin_idx % len(live)]
        self._robin_idx = (self._robin_idx + 1) % max(len(live), 1)
        return slot

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
        )
        entry.slots.append(slot)
        if len(entry.slots) > MAX_SLOTS_PER_DOMAIN:
            entry.slots = entry.slots[-MAX_SLOTS_PER_DOMAIN:]
        entry.updated_at = _now()
        logger.debug(
            "DomainCache: added slot for %s (slots=%d, session=%s)",
            domain, len(entry.slots), proxy_session_id,
        )
        return slot

    def record_slot_failure(
        self,
        domain: str,
        slot_id: str,
        status_code: Optional[int] = None,
    ) -> bool:
        """Record a slot failure. Returns True iff promoted to heavy.

        AAR-14 semantics: infrastructure failures (None / 502 / 504 / 520 /
        522 / 524 / connection errors) are NOT counted against the slot.
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
        entry._domain_failure_count += 1

        if entry.live_slots():
            return False
        return self._maybe_promote_heavy(domain, "all_slots_exhausted")

    def record_slot_success(self, domain: str, slot_id: str) -> None:
        entry = self._entries.get(domain)
        if not entry or entry.level != "cookies":
            return
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
