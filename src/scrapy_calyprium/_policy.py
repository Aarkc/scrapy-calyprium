"""Per-run profile policy forwarded to Veil and Mimic (AAR-62).

Forge's ``build_run_settings`` projects the spider's Profile onto these
Scrapy settings. The SDK must forward them or none of the per-spider budgets,
paid-solve policy, engine allow-lists or provider allow-lists are enforced,
and Veil spend is never attributed to the spider.

=========================  ===============================================
Setting                    Forwarded as
=========================  ===============================================
``SPIDER_ID``              Veil ``-spider_<id>``; Mimic/Tessera ``spider_id``
``RUN_ID``                 Veil ``-run_<id>``; Mimic/Tessera ``run_id`` (forge
                           ``spider_runs.id``, per-run cost attribution;
                           digits only, anything else is ignored)
``MIMIC_ALLOW_PAID_SOLVE`` Mimic ``allow_paid_solve``; ``False`` also keeps
                           local-first solves off Tessera's paid solvers
``MIMIC_PAID_SOLVE_MODE``  Mimic ``paid_solve_mode`` (off | token | all)
``MIMIC_ALLOWED_ENGINES``  Mimic ``allowed_engines``; clamps ``engine_hint``
``VEIL_ALLOWED_PROVIDERS`` Veil ``-ap_<a.b.c>`` (dot-joined)
``VEIL_COUNTRY``           Veil ``-country_<cc>``; Mimic solve ``country``
=========================  ===============================================
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

#: Every run setting the SDK reads for policy. Contract-tested against
#: forge's ``build_run_settings`` so drift fails CI.
POLICY_SETTINGS = (
    "SPIDER_ID",
    "RUN_ID",
    "MIMIC_ALLOW_PAID_SOLVE",
    "MIMIC_PAID_SOLVE_MODE",
    "MIMIC_ALLOWED_ENGINES",
    "VEIL_ALLOWED_PROVIDERS",
    "VEIL_COUNTRY",
)


def _raw(settings: Any, key: str) -> Optional[str]:
    value = None
    if settings is not None:
        try:
            value = settings.get(key)
        except AttributeError:
            value = None
    if value is None or value == "":
        value = os.getenv(key)
    return value if value not in (None, "") else None


def _as_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        items = value
    else:
        items = str(value).split(",")
    return [str(v).strip() for v in items if str(v).strip()]


def _as_run_id(value: Any) -> Optional[str]:
    """Forge's integer run id as a string; None unless it is a positive integer.

    The value ends up in the Veil proxy username, so anything but digits is
    dropped rather than forwarded."""
    if value is None:
        return None
    text = str(value).strip()
    if not text.isdigit() or not text.isascii() or int(text) <= 0:
        return None
    return str(int(text))


def _as_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class RunPolicy:
    spider_id: Optional[str] = None
    run_id: Optional[str] = None
    allow_paid_solve: Optional[bool] = None
    paid_solve_mode: Optional[str] = None
    allowed_engines: List[str] = field(default_factory=list)
    allowed_providers: List[str] = field(default_factory=list)
    country: Optional[str] = None

    @classmethod
    def from_settings(cls, settings: Any) -> "RunPolicy":
        mode = _raw(settings, "MIMIC_PAID_SOLVE_MODE")
        country = _raw(settings, "VEIL_COUNTRY")
        return cls(
            spider_id=_raw(settings, "SPIDER_ID"),
            run_id=_as_run_id(_raw(settings, "RUN_ID")),
            allow_paid_solve=_as_bool(_raw(settings, "MIMIC_ALLOW_PAID_SOLVE")),
            paid_solve_mode=mode.strip().lower() if mode else None,
            allowed_engines=_as_list(_raw(settings, "MIMIC_ALLOWED_ENGINES")),
            allowed_providers=_as_list(_raw(settings, "VEIL_ALLOWED_PROVIDERS")),
            country=country.strip().lower() if country else None,
        )

    @property
    def paid_solve_denied(self) -> bool:
        """True only when the policy explicitly forbids paid solving."""
        return self.allow_paid_solve is False or self.paid_solve_mode == "off"

    # -- Veil ------------------------------------------------------------

    def veil_username_params(self) -> List[str]:
        """``key_value`` parts for the Veil proxy username (see gateway
        ``ConfigResolver.PARAM_MAPPING``)."""
        params = []
        if self.spider_id:
            params.append(f"spider_{self.spider_id}")
        if self.run_id:
            params.append(f"run_{self.run_id}")
        if self.allowed_providers:
            params.append(f"ap_{'.'.join(self.allowed_providers)}")
        if self.country:
            params.append(f"country_{self.country}")
        return params

    def veil_username(self, base: str) -> str:
        return "-".join([base, *self.veil_username_params()])

    # -- Mimic -----------------------------------------------------------

    def _attribution(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.spider_id:
            out["spider_id"] = self.spider_id
        if self.run_id:
            out["run_id"] = self.run_id
        return out

    def mimic_fetch_fields(self) -> Dict[str, Any]:
        """Fields for Mimic ``FetchRequest`` (``/api/fetch``)."""
        out: Dict[str, Any] = {}
        out.update(self._attribution())
        if self.allow_paid_solve is not None:
            out["allow_paid_solve"] = self.allow_paid_solve
        if self.paid_solve_mode:
            out["paid_solve_mode"] = self.paid_solve_mode
        if self.allowed_engines:
            out["allowed_engines"] = list(self.allowed_engines)
        if self.country:
            out["proxy_country"] = self.country
        return out

    def mimic_session_fields(self) -> Dict[str, Any]:
        """Fields for Mimic ``SessionCreateRequest`` (``/api/session``)."""
        out: Dict[str, Any] = {}
        out.update(self._attribution())
        if self.allowed_engines:
            out["allowed_engines"] = list(self.allowed_engines)
        return out

    def solve_fields(self) -> Dict[str, Any]:
        """Fields for Mimic/Tessera ``SolveRequest`` (``/api/solve``)."""
        out: Dict[str, Any] = {}
        out.update(self._attribution())
        if self.country:
            out["country"] = self.country
        return out

    def clamp_engine(self, engine: Optional[str]) -> Optional[str]:
        """Keep an engine choice inside the allow-list (None = let the server pick
        unless an allow-list forces the first allowed engine)."""
        if not self.allowed_engines:
            return engine
        if engine in self.allowed_engines:
            return engine
        return self.allowed_engines[0]
