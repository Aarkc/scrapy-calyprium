"""Spider-side auto-routing orchestrator.

Wires together:

- `LocalFetcher` (httpcloak / curl_cffi backend, raw bytes)
- `DomainCache` (per-spider, AAR-14 circuit breaker)
- `SolveClient` (Mimic /api/solve cold path)

Flow per request:

  1. If cache says heavy and not due for re-probe → caller should fall through
     to legacy MimicMiddleware which uses /api/fetch + browser. We don't try
     to do browser navigation locally.
  2. If cache has cookie slots → pick one, do local httpcloak with cookies,
     check is_blocked. On success: return. On block: mark slot failed,
     fall through.
  3. Otherwise → local httpcloak without cookies. On success: cache as
     "light" and return. On block: fall through.
  4. Call /api/solve to get fresh cookies → cache as a new slot → retry
     local httpcloak with the new cookies. A transport error or a rate limit
     never solves (a solve cannot fix either): the router backs off instead.
  5. If solve returned success=False (real domain block) → mark domain heavy
     and surface a 403 to the caller. Caller decides whether to fall through
     to the legacy browser path or fail the Scrapy request.

"Blocked" is the shared classifier's verdict (block_detect.diagnose), and every
`RouteResult` carries the diagnosis of how it ended. Returns `RouteResult`. The
caller (MimicMiddleware) wraps it in a Scrapy Response.

Akamai Bot Manager in front of a form (apps.ilsos.gov's search, 2026-10-26) is met in three
ways no other vendor shows, each handled on the slot's own exit:

* the form's POST with the page's unaccepted ``_abck`` gets no answer at all (the stream is
  reset in ~130 ms, or the request is held): a transport error that is the site's verdict, not
  the proxy's. It is solved as an Akamai sensor on the page (the Referer, else the homepage),
  and the POST replayed on that solve's exit;
* the POST with an accepted ``_abck`` meets Akamai's challenge action ("Challenge
  Validation", diagnosis ``akamai_captcha``): the solve service answers it on the slot's
  session (a paid recaptcha token, the page's verify call, the form posted again) and its
  answer is the request's response;
* minutes later the sensor's validation lapses (the site answers its 404 and resets ``_abck``):
  the sensor is run again on the same exit and its cookies merged into the slot, the site's
  session and ``sec_cpt`` kept, before the request is replayed.

AAR-17.
"""
from __future__ import annotations

import asyncio
import email.utils
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from scrapy_calyprium import _outcome
from scrapy_calyprium.routing import diagnosis as dg
from scrapy_calyprium.routing.block_detect import challenge_family, diagnose
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import (
    LocalFetchError,
    LocalFetchResult,
    LocalFetcher,
)
from scrapy_calyprium.routing.solve_client import SolveClient, SolveError

logger = logging.getLogger(__name__)


def _light_session_id() -> str:
    return "lt" + uuid.uuid4().hex[:14]


@dataclass
class RouteResult:
    """Outcome of an auto-route attempt."""
    fetch: Optional[LocalFetchResult]
    routing_method: str  # "httpcloak_light" | "httpcloak_cookies" | "solve_then_replay"
    blocked: bool
    domain_level: str
    needs_legacy_fallback: bool = False  # deprecated — solve retries instead of falling back
    error: Optional[str] = None
    # Identifier of the cookie slot used for this fetch (cookies / solve_then_replay
    # paths only). The middleware stuffs this into request.meta so the spider's
    # parse callback can call back to record_slot_failure when the response
    # was structurally valid (status 200, passes is_blocked) but contained no
    # useful data — a silent block. Without this signal the rate cap never
    # learns about Cloudflare interstitials served with status 200.
    slot_id: Optional[str] = None
    # Set when the request tracer recorded a span for this fetch, so the
    # middleware can keep the response from being traced a second time.
    trace_id: Optional[str] = None
    # Why the fetch ended as it did (the shared classifier's verdict on `fetch`, or on the
    # transport error when there is no response). Filled by fetch() when left None.
    diagnosis: Optional[dg.Diagnosis] = None


#: A fresh Cloudflare clearance is often refused on its first replay before Cloudflare accepts it
#: (cookie warm-up: domain_cache.MAX_SLOT_FAILURES tolerates the same). A solve's clearance refused
#: so is replayed once more after this pause before the router gives up on it (Mimic's
#: NATIVE_REPLAY_RETRY_DELAY_S). 0 = no second replay.
WARMUP_REPLAY_DELAY_S = 2.0
# How long a domain whose plain (cookieless) fetch was refused sends its requests straight to its
# cookie slots before one request tries the plain fetch again (MIMIC_LOCAL_LIGHT_PROBE_INTERVAL).
LIGHT_PROBE_INTERVAL_S = 60.0


def _cloudflare_clearance(slot) -> bool:
    return any(str(c.get("name", "")) == "cf_clearance" for c in (slot.cookies or []))


def _warmup_refusal(fetch: LocalFetchResult) -> bool:
    """A replay refused the way a fresh cf_clearance's first use is: Cloudflare's 403 / 503, or
    its challenge again (cf-mitigated / the challenge page). A rate limit is not."""
    return fetch.status_code in (403, 503) or challenge_family(
        fetch.status_code, fetch.body, headers=fetch.headers) == "cloudflare_managed"


#: Path segments (and host names) of an API endpoint: a browser has no page to load there.
_API_SEGMENTS = frozenset({"api", "apis", "graphql", "gql", "rest", "rpc", "ajax", "services"})


_SITES = None


def _site(host: str) -> str:
    """The registrable domain of ``host`` (``ohiosos.gov`` for ``businesssearch.ohiosos.gov``),
    from tldextract's bundled public suffix list (Scrapy's own dependency; the list is never
    fetched); the host itself when it has none or tldextract is missing."""
    global _SITES
    host = (host or "").lower().rstrip(".")
    if _SITES is None:
        try:
            from tldextract import TLDExtract
            _SITES = TLDExtract(suffix_list_urls=(), include_psl_private_domains=True)
        except Exception:  # noqa: BLE001 — without it only one host is its own site
            _SITES = False
    if _SITES:
        try:
            parts = _SITES(host)
        except Exception:  # noqa: BLE001
            return host
        if parts.domain and parts.suffix:
            return f"{parts.domain}.{parts.suffix}"
    return host


def _calling_page(url: str, headers: Optional[Dict[str, str]] = None) -> Optional[str]:
    """The page whose script made a request to another host of its own site, or None.

    A browser sends ``Origin`` on a script's cross-origin request (fetch, XMLHttpRequest) and
    never on a navigation. When it names another host of the request's site, the request is
    the page's call to an API host, and that page is where a browser earns the clearance
    (Cloudflare's cf_clearance holds for every host of the zone): the ``Referer`` when it is
    on the origin's host, else the origin's homepage. The API host's own URLs may be no page
    at all: businesssearchapi.ohiosos.gov blocks every request that does not come from
    businesssearch.ohiosos.gov (no Origin or Referer of it) with a 403 page that no solve
    clears, so a browser sent there earns nothing, while the search page's managed challenge
    gives the clearance its API takes (2026-10-26)."""
    found = {str(k).lower(): str(v) for k, v in (headers or {}).items() if v is not None}
    origin = urlparse(found.get("origin", "").strip())
    host = (urlparse(url).hostname or "").lower()
    page_host = (origin.hostname or "").lower()
    if origin.scheme not in ("http", "https") or not page_host or not host or page_host == host:
        return None
    if _site(page_host) != _site(host):
        return None
    referer = urlparse(found.get("referer", "").strip())
    if referer.scheme in ("http", "https") and (referer.hostname or "").lower() == page_host:
        return referer.geturl()
    return f"{origin.scheme}://{origin.netloc}/"


def _solve_url(url: str, method: str = "GET", headers: Optional[Dict[str, str]] = None) -> str:
    """The page a solve loads for a request. Mimic's /api/solve opens a URL in a browser (a GET):
    a page's call to another host of its site (``_calling_page``) solves on that page; else a
    GET solves on its own URL, a POST to a page (a search form) on that page, and a POST to an
    API (a JSON or GraphQL body, or an api / graphql path) on the site's homepage, whose
    clearance cookies are the host's. The request itself is then replayed as it was made."""
    page = _calling_page(url, headers)
    if page:
        return page
    if (method or "GET").upper() in ("GET", "HEAD"):
        return url
    parsed = urlparse(url)
    ctype = next((str(v).lower() for k, v in (headers or {}).items()
                  if str(k).lower() == "content-type"), "")
    segments = [seg.lower() for seg in parsed.path.split("/") if seg]
    api = (
        "json" in ctype or "graphql" in ctype
        or (parsed.hostname or "").split(".")[0] in _API_SEGMENTS
        or any(seg in _API_SEGMENTS or "graphql" in seg or seg.endswith(".json") for seg in segments)
    )
    if not api or not parsed.netloc:
        return url
    return f"{parsed.scheme or 'https'}://{parsed.netloc}/"


def _diagnose_fetch(fetch: LocalFetchResult, url: Optional[str] = None) -> dg.Diagnosis:
    return diagnose(fetch.status_code, fetch.body, fetch.content_type or None, fetch.headers,
                    url or fetch.url, fetch.final_url)


#: Akamai Bot Manager's own cookies. A sensor run again on a slot (the lapse) replaces these;
#: the site's own session cookie and the challenge action's ``sec_cpt`` stay, as they do in a
#: browser whose page reran the sensor script.
AKAMAI_COOKIES = frozenset({"_abck", "bm_sz", "ak_bmsc", "bm_sv", "bm_mi", "bm_so", "bm_s", "bm_ss",
                            "bm_lso", "sbsd", "sbsd_o", "sbsd_ss"})
#: The challenge action's clearance cookie (two hours from the challenge, kept across sensor runs).
CPT_COOKIE = "sec_cpt"
#: The cookies a slot on an Akamai host keeps current from its responses, as a browser does.
_AKAMAI_KEPT = AKAMAI_COOKIES | {CPT_COOKIE}
#: How long one answer of Akamai's challenge action may take at the solve service (a 2Captcha
#: token took 67 s, then two requests).
CAPTCHA_DEADLINE_MS = 200_000
#: A challenge met again on a slot this soon after its answer is not bought again at once: the slot's sensor is
#: run again first (free). On apps.ilsos.gov the challenge came back about seven minutes after an answer
#: (2026-10-26), and was answered again.
CAPTCHA_MIN_INTERVAL_S = 120.0


def _abck_accepted(value: Optional[str]) -> Optional[bool]:
    """Whether an ``_abck`` value says Akamai accepted its sensor (second field 0); None for none."""
    if not value:
        return None
    parts = str(value).split("~")
    return len(parts) > 1 and parts[1] == "0"


def _cookie_of(cookies: Optional[List[Dict]], name: str) -> Optional[str]:
    for c in cookies or []:
        if c.get("name") == name:
            return c.get("value")
    return None


def _set_cookie_updates(lines: Optional[List[str]]) -> List[Tuple[str, str, Optional[str], bool]]:
    """(name, value, domain, deletes) for each Set-Cookie line: ``deletes`` for Max-Age zero or less, or an
    Expires date already past (a browser drops the cookie then)."""
    out = []
    for line in lines or []:
        head, *attrs = str(line).split(";")
        name, sep, value = head.partition("=")
        name = name.strip()
        if not sep or not name:
            continue
        fields = {}
        for attr in attrs:
            key, _, val = attr.strip().partition("=")
            fields[key.strip().lower()] = val.strip()
        deletes = False
        if "max-age" in fields:
            try:
                deletes = int(fields["max-age"]) <= 0
            except ValueError:
                deletes = False
        elif "expires" in fields:
            try:
                when = email.utils.parsedate_to_datetime(fields["expires"].replace("-", " "))
                deletes = when.timestamp() <= time.time()
            except (TypeError, ValueError):
                deletes = False
        out.append((name, value.strip(), fields.get("domain") or None, deletes))
    return out


def _abck_reset(lines: Optional[List[str]]) -> bool:
    """Whether a response set ``_abck`` to a value whose sensor is not accepted (the lapse)."""
    return any(name == "_abck" and not deletes and _abck_accepted(value) is False
               for name, value, _, deletes in _set_cookie_updates(lines))


def _sets_akamai(lines: Optional[List[str]]) -> bool:
    """Whether a response set Akamai Bot Manager's cookies (its sensor's ``_abck`` or ``bm_sz``)."""
    return any(name in ("_abck", "bm_sz") for name, _, _, _ in _set_cookie_updates(lines))


def _request_cookies(headers: Optional[Dict[str, str]]) -> List[Dict]:
    """The cookies of a request's own Cookie header (Scrapy's jar), as ``{name, value}``."""
    header = next((str(v) for k, v in (headers or {}).items() if str(k).lower() == "cookie"), "")
    out = []
    for part in header.split(";"):
        name, sep, value = part.strip().partition("=")
        if sep and name.strip():
            out.append({"name": name.strip(), "value": value.strip()})
    return out


def _absorb(cookies: List[Dict], lines: Optional[List[str]], host: str, names=None) -> List[Dict]:
    """``cookies`` with a response's Set-Cookie lines applied as a browser does (a value replaced, a
    deleted cookie dropped, a new one added), only for ``names`` when given."""
    out = [dict(c) for c in cookies or []]
    for name, value, domain, deletes in _set_cookie_updates(lines):
        if names is not None and name not in names:
            continue
        out = [c for c in out if c.get("name") != name]
        if not deletes:
            out.append({"name": name, "value": value, "domain": domain or host, "path": "/"})
    return out


def _merge_sensor(slot_cookies: List[Dict], solved: List[Dict]) -> List[Dict]:
    """A slot's cookies after its sensor ran again: Akamai's cookies from the new run, every other
    cookie the slot holds kept (the site's session, ``sec_cpt``), and the new run's others added."""
    fresh = {c.get("name"): c for c in solved or [] if c.get("name")}
    out = []
    for c in slot_cookies or []:
        name = c.get("name")
        if name in AKAMAI_COOKIES and name in fresh:
            continue
        out.append(dict(c))
    have = {c.get("name") for c in out}
    out.extend(dict(c) for name, c in fresh.items() if name not in have)
    return out


def _sensor_page(url: str, headers: Optional[Dict[str, str]] = None) -> str:
    """The page whose sensor a form's POST needs: the request's Referer on the same host (the form's own
    page), else the host's homepage."""
    found = {str(k).lower(): str(v) for k, v in (headers or {}).items() if v is not None}
    parsed = urlparse(url)
    referer = urlparse(found.get("referer", "").strip())
    if referer.scheme in ("http", "https") and (referer.hostname or "") == (parsed.hostname or ""):
        return referer.geturl()
    return f"{parsed.scheme or 'https'}://{parsed.netloc}/"


class SpiderAutoRouter:
    def __init__(
        self,
        fetcher: LocalFetcher,
        cache: DomainCache,
        solve_client: SolveClient,
        proxy_url: Optional[str] = None,
        provider: Optional[str] = None,
        solvers: Optional[List[str]] = None,
        target_pool_size: int = 8,
        refill_interval: float = 1.0,
        cold_start_burst: int = 4,
        solve_max_retries: int = 5,
        solve_parallel_solves: int = 3,
        tracer=None,
        warmup_replay_delay_s: float = WARMUP_REPLAY_DELAY_S,
        light_sessions: int = 0,
        light_probe_interval: float = LIGHT_PROBE_INTERVAL_S,
    ):
        self.fetcher = fetcher
        self.cache = cache
        self.solve_client = solve_client
        self.proxy_url = proxy_url
        self.provider = provider
        # Ordered captcha-solver allowlist forwarded to Tessera /api/solve. When
        # set (e.g. ["jevi"]), the solve never falls through to providers that
        # can't handle the domain; failed solves return fast and the next pool
        # refill retries with a fresh veil session (new IP).
        self.solvers = solvers
        # Request tracer — if set, emits per-URL spans to ClickHouse.
        # Injected by MimicBrowserMiddleware when the CalypriumRequestTracer
        # extension is active.
        self.tracer = tracer
        # Per-domain locks for solve coalescing. When 32 concurrent spider
        # requests for the same domain all hit the block path, only one
        # actually calls /api/solve; the rest wait on the lock and reuse the
        # cookie pool the winner populated.
        self._solve_locks: Dict[str, asyncio.Lock] = {}
        # The vendor each domain's last block showed (block_detect.challenge_family),
        # named in its solves -- the hot path's and the refill's.
        self._challenge_family: Dict[str, str] = {}
        # The pause before a fresh Cloudflare clearance refused on its first replay is replayed
        # once more (WARMUP_REPLAY_DELAY_S; 0 = never).
        self.warmup_replay_delay_s = warmup_replay_delay_s
        # Plain (light) fetches spread over this many sticky gateway sessions, round robin: one exit each,
        # so a run's pages leave through that many addresses (MIMIC_LOCAL_LIGHT_SESSIONS). 0 keeps one
        # session-less proxy URL, which the gateway puts on the same exit for every tunnel. A session
        # whose fetch was refused (429, or LinkedIn's 999) or failed in transport takes a new id, so another exit.
        self._light_sessions: List[str] = [_light_session_id() for _ in range(max(0, int(light_sessions)))]
        self._light_next = 0
        # A domain whose plain fetch the site refused (a challenge, a deny) while it has live cookie
        # slots sends its requests straight to them for light_probe_interval seconds, then one request
        # tries the plain fetch again: on a walled host every plain try is a wasted request (and a
        # rationed residential one on a .gov host). A plain fetch that passes ends it. 0 = always try
        # the plain fetch first, as before.
        self.light_probe_interval = float(light_probe_interval or 0)
        self._light_refused_at: Dict[str, float] = {}

        # Phase 5: proactive cookie pool expansion. The hot path solve
        # coalescing collapses N concurrent requests into 1 solve, which is
        # right for the FIRST request to a domain (don't blow your solve
        # budget on a single page). But after that, the spider keeps using
        # that one slot until it dies — and at concurrency 32 the slot
        # tends to die in seconds because every request that gets a 403
        # bumps fail_count toward MAX_SLOT_FAILURES=3. The IP reputation
        # rotation in Phase 4 has nowhere to send traffic if the pool is
        # always size 1.
        #
        # The refill loop runs in the background per active domain and
        # mints replacement slots until the pool reaches target_pool_size.
        # Refill solves bypass the per-domain solve_lock (they're not
        # racing other callers, they're filling capacity) but still respect
        # Mimic's per-domain rate limit.
        self.target_pool_size = target_pool_size
        self.refill_interval = refill_interval
        # Cold-start burst: when the pool is severely under-provisioned
        # (e.g. just minted the very first slot via solve coalescing), the
        # one-slot-per-tick refill rhythm can't catch up before the small
        # pool collapses under concurrent traffic. On the first refill
        # check after a domain becomes "cookies", fire up to this many
        # parallel solves to bring the pool to roughly target size right
        # away. After the burst, refills go back to one-at-a-time at
        # refill_interval cadence.
        self.cold_start_burst = cold_start_burst
        self.solve_max_retries = solve_max_retries
        self.solve_parallel_solves = solve_parallel_solves
        self._refill_tasks: Dict[str, asyncio.Task] = {}
        self._refill_stop = False
        # Refill solves in flight (cancelled by stop_refill when the crawl ends).
        self._refill_running: set = set()
        # Last hot-path refill check per domain. The original design used a
        # long-lived asyncio.create_task background loop, but in Scrapy's
        # Twisted-driven environment that task's `await asyncio.sleep` never
        # gets pumped — Scrapy's reactor only services the asyncio loop
        # when there's a current await chain. Driving refill from the fetch
        # hot path is more robust: every request opportunistically checks
        # whether a refill is due, and if so spawns a fire-and-forget solve
        # via asyncio.create_task (which IS pumped because the spider's own
        # await chain is active).
        self._last_refill_check: Dict[str, float] = {}
        # Per-domain in-flight refill count. Capped at cold_start_burst on
        # first refill, 1 thereafter, so the burst can fire several solves
        # in parallel during cold start without hammering Mimic forever.
        self._refill_in_flight: Dict[str, int] = {}
        self._cold_start_done: Dict[str, bool] = {}
        # The page a domain's requests solve on when it is not the domain's own (``_calling_page``:
        # a page calling an API host of its site). Refill solves load it too: the API host's
        # homepage, which a refill opens otherwise, may be a page that no solve clears.
        self._solve_pages: Dict[str, str] = {}
        # Hosts whose responses set Akamai's sensor cookies, and those whose slots came from an
        # Akamai sensor solve for a form (their slots keep Akamai's cookies current and are
        # watched for the sensor's lapse).
        self._akamai_seen: set = set()
        self._akamai_forms: set = set()
        # When each slot's challenge action was last answered and its sensor last run again
        # (monotonic): a request that met either before then replays instead of paying again.
        self._cpt_answered: Dict[str, float] = {}
        self._resensored: Dict[str, float] = {}
        # Slots whose last answer came with the sensor's validation reset: their next request runs
        # the sensor first (rather than meeting the site's 404).
        self._resensor_due: set = set()

    def _solve_lock(self, domain: str) -> asyncio.Lock:
        lock = self._solve_locks.get(domain)
        if lock is None:
            lock = asyncio.Lock()
            self._solve_locks[domain] = lock
        return lock

    def _ensure_refill_task(self, domain: str) -> None:
        """Hot-path refill check.

        Called on every fetch() after a successful solve. Two modes:

        - **Cold start** (first time this domain becomes "cookies"): fire
          up to `cold_start_burst` solves in parallel to bring the pool
          to ~target_pool_size right away. The interval gate is bypassed
          so the burst happens on the very first call after seeding.
        - **Steady state**: gated by refill_interval (default 1s) and a
          single-slot-per-tick cap. Replenishes attrition without
          hammering Mimic.

        Replaces the original long-lived background loop, which doesn't
        survive Scrapy's Twisted/asyncio bridge — `asyncio.sleep` inside a
        detached task never wakes because the asyncio loop is only pumped
        when there's an active await chain from a Scrapy callback.
        """
        import time as _t
        if self._refill_stop:
            return
        entry = self.cache.get(domain)
        if entry is None or entry.level != "cookies":
            return
        live = len(entry.live_slots())
        if live >= self.target_pool_size:
            return
        if len(entry.slots) >= self.target_pool_size * 2:
            # Lots of dead slots accumulated — skip until they expire to
            # avoid unbounded growth from a hostile domain.
            return

        cold_start = not self._cold_start_done.get(domain, False)

        # Steady-state path is interval-gated; cold start bypasses the gate.
        if not cold_start:
            now = _t.time()
            last = self._last_refill_check.get(domain, 0.0)
            if now - last < self.refill_interval:
                return
            self._last_refill_check[domain] = now

        # Decide how many parallel solves to fire this tick.
        if cold_start:
            # Mark cold start done immediately to dedupe concurrent racers
            self._cold_start_done[domain] = True
            self._last_refill_check[domain] = _t.time()
            # Need (target - live) more slots; cap at cold_start_burst
            slots_needed = max(0, self.target_pool_size - live)
            num_to_fire = min(slots_needed, self.cold_start_burst)
            logger.info(
                "AutoRouter: cold-start refill for %s — firing %d parallel "
                "solves (live=%d, target=%d)",
                domain, num_to_fire, live, self.target_pool_size,
            )
        else:
            # Steady state: proactively solve toward target_pool_size using the
            # full parallel-solve budget. The original code fired one solve at a
            # time (in_flight>0 -> return), capping refill at ~1 solve per
            # solve-latency (~3/min). After any dip (veil blip, restart decay)
            # the pool could never climb back: deaths at the shrunken pool just
            # matched that trickle, so it stuck in a low-throughput equilibrium.
            # Topping the in-flight solves up to solve_parallel_solves lets the
            # pool actually return to — and hold — the target.
            budget = max(1, self.solve_parallel_solves)
            in_flight = self._refill_in_flight.get(domain, 0)
            if in_flight >= budget:
                return
            slots_needed = self.target_pool_size - live
            num_to_fire = min(slots_needed, budget - in_flight)
            if num_to_fire <= 0:
                return

        for _ in range(num_to_fire):
            self._refill_in_flight[domain] = (
                self._refill_in_flight.get(domain, 0) + 1
            )
            try:
                task = asyncio.create_task(self._refill_one(domain, live))
            except RuntimeError:
                self._refill_in_flight[domain] -= 1
            else:
                self._refill_running.add(task)
                task.add_done_callback(self._refill_running.discard)

    async def _refill_one(self, domain: str, live_at_check: int) -> None:
        """Mint a single fresh slot. Called via fire-and-forget from
        _ensure_refill_task. Errors are swallowed — refill is best-effort
        and a transient failure should not affect spider throughput."""
        try:
            try:
                # the page the domain's requests solve on, when it is not the domain's own
                page = self._solve_pages.get(domain)
                solve = await self.solve_client.solve(
                    domain=domain, provider=self.provider, solvers=self.solvers,
                    challenge_type=self._challenge_family.get(domain),
                    **({"target_url": page} if page else {}),
                )
            except SolveError as exc:
                logger.info(
                    "AutoRouter: refill solve failed for %s: %s", domain, exc,
                )
                return
            if not solve.success:
                logger.info(
                    "AutoRouter: refill solve returned no cookies for %s",
                    domain,
                )
                return
            self.cache.set_cookies_from_solve(
                domain=domain,
                cookies=solve.cookies,
                user_agent=solve.user_agent,
                proxy_session_id=solve.proxy_session_id,
                preset=solve.preset,
                egress_ip=solve.egress_ip,
                provider=solve.provider,
                country=solve.country,
            )
            entry = self.cache.get(domain)
            new_live = len(entry.live_slots()) if entry else 0
            logger.info(
                "AutoRouter: refill added slot for %s (egress_ip=%s, "
                "pool=%d->%d/%d)",
                domain, solve.egress_ip, live_at_check, new_live,
                self.target_pool_size,
            )
        except Exception as exc:
            logger.warning(
                "AutoRouter: refill_one crashed for %s: %s", domain, exc,
            )
        finally:
            self._refill_in_flight[domain] = max(
                0, self._refill_in_flight.get(domain, 0) - 1,
            )

    def stop_refill(self) -> None:
        """End pool refills (the crawl is over): no refill solve starts after this, and the ones
        in flight are cancelled, so a closed spider doesn't keep paying for clearances."""
        self._refill_stop = True
        for task in list(self._refill_running):
            task.cancel()

    def _report_ip_outcome(
        self,
        *,
        domain: str,
        slot,
        outcome: str,
        status_code: Optional[int] = None,
    ) -> None:
        """Fire-and-forget POST to Mimic /api/ip-health/report.

        Closes the per-(domain, IP) reputation feedback loop. The spider's
        local httpcloak replays don't flow through Mimic's server-side
        routing, so without this Mimic only learns about failures from
        its own /api/fetch traffic — a small fraction of total volume.
        With it, Mimic's IP blacklist sees real spider observations and
        the next solve's pre-screen loop can rotate around the burned
        physical IP.

        Skipped silently when the slot has no resolved egress_ip — there's
        nothing to feed the IP-level reputation tracker. The session-level
        tracker still gets the report, but per-(domain, ip) doesn't.
        """
        if slot is None or not slot.egress_ip:
            return
        try:
            asyncio.create_task(
                self.solve_client.report_ip_outcome(
                    proxy_session_id=slot.proxy_session_id,
                    domain=domain,
                    outcome=outcome,
                    status_code=status_code,
                    egress_ip=slot.egress_ip,
                )
            )
        except RuntimeError:
            # No running loop — happens in some test paths. Reputation
            # is a soft signal; dropping a single report is fine.
            pass

    def report_silent_failure(
        self,
        domain: str,
        slot_id: Optional[str],
        reason: str = "no_useful_data",
    ) -> None:
        """Spider-side feedback channel for silent block detection.

        When a spider's parse callback receives a structurally-valid 200
        response that nevertheless contains no useful data (e.g. a Cloudflare
        compatibility-mode page with real markup but no product JSON), it
        calls this method so the routing layer can roll back the slot's
        success counter and feed the rate-cap learner. Without it, the
        router happily keeps firing the same dead slot at full speed.

        Two cases:
        - slot_id is set (cookie replay path): record a slot failure with
          status_code=200, which feeds the rate-cap learner.
        - slot_id is None (light path / cold discovery): we have no slot to
          blame. Drop the "light" classification so the next request escalates
          through solve instead of replaying a useless light fetch.
        """
        if slot_id:
            logger.info(
                "AutoRouter: silent failure on %s slot %s (reason=%s)",
                domain, slot_id, reason,
            )
            # Resolve the slot to get its egress_ip for the IP report
            entry = self.cache.get(domain)
            slot = None
            if entry:
                slot = next((s for s in entry.slots if s.slot_id == slot_id), None)
            self.cache.record_slot_failure(domain, slot_id, status_code=200)
            if slot is not None:
                self._report_ip_outcome(
                    domain=domain, slot=slot, outcome="blocked",
                    status_code=200,
                )
            return

        # Light-path silent failure — drop the cache entry so the next
        # request re-probes and (hopefully) escalates to /api/solve.
        entry = self.cache.get(domain)
        if entry and entry.level == "light":
            logger.info(
                "AutoRouter: light-path silent failure on %s (reason=%s); "
                "dropping light classification to force re-probe",
                domain, reason,
            )
            # Drop the entry entirely so the next request hits step 3 again
            # and escalates through the solve flow if blocked.
            self.cache._entries.pop(domain, None)

    async def fetch(
        self, url: str, *, domain: str, headers: Optional[Dict[str, str]] = None,
        method: str = "GET", body: Optional[bytes] = None,
    ) -> RouteResult:
        """Fetch ``url`` by the local route. ``headers`` are the request's own (the spider's
        X-Api-Key, Referer, ...): every local fetch of it sends them, the fetcher keeping its
        browser identity (see local_fetch._OWNED_HEADERS). A ``method`` other than GET (a
        storefront's GraphQL or search POST) sends ``body``: the light fetch and every replay
        send the request as made; the solve loads a page (see ``_solve_url``). The route may
        send a request more than once, so it is for requests that read, not ones that order."""
        start_time = time.monotonic()
        trace_id = uuid.uuid4().hex
        result = await self._fetch_inner(url, domain=domain, headers=headers, method=method,
                                         body=body)
        if result.diagnosis is None:
            if result.fetch is not None:
                result.diagnosis = _diagnose_fetch(result.fetch, url)
            elif result.error:
                result.diagnosis = dg.diagnose_error(result.error)
        diag = result.diagnosis.to_dict() if result.diagnosis is not None else None
        # Emit trace span if tracer is active
        if self.tracer and self.tracer.run_number:
            duration_ms = int((time.monotonic() - start_time) * 1000)
            slot = None
            if result.slot_id:
                entry = self.cache.get(domain)
                if entry:
                    slot = next((s for s in entry.slots if s.slot_id == result.slot_id), None)
            status_code = result.fetch.status_code if result.fetch else 0
            if diag is not None:
                # A transport error is an error (or timeout) span, never a priced success.
                status = _outcome.outcome(diag, status_code)
            else:
                status = "blocked" if result.blocked else "success"
            self.tracer.record_span(
                trace_id=trace_id,
                url=url,
                domain=domain,
                component="spider",
                operation="fetch",
                status=status,
                status_code=status_code,
                duration_ms=duration_ms,
                routing_method=result.routing_method,
                slot_id=result.slot_id or "",
                egress_ip=slot.egress_ip if slot and slot.egress_ip else "",
                proxy_session_id=slot.proxy_session_id if slot else "",
                response_bytes=len(result.fetch.body) if result.fetch and result.fetch.body else 0,
                error_message=result.error or "",
                diagnosis=diag,
            )
            result.trace_id = trace_id
        return result

    def _light_refused(self, domain: str, entry) -> bool:
        """Whether this request skips the plain fetch: the domain's last plain fetch was refused
        less than light_probe_interval ago and it has live cookie slots to go to instead. Once
        the interval is up, this request is the one that tries the plain fetch again (its peers
        keep skipping while it does)."""
        if self.light_probe_interval <= 0 or domain not in self._light_refused_at:
            return False
        if not (entry and entry.level == "cookies" and entry.live_slots()):
            return False
        now = time.monotonic()
        if now - self._light_refused_at[domain] >= self.light_probe_interval:
            self._light_refused_at[domain] = now
            return False
        return True

    def _light_session(self):
        """The next light session (its index and id), or (-1, None) with none configured."""
        if not self._light_sessions:
            return -1, None
        at = self._light_next % len(self._light_sessions)
        self._light_next += 1
        return at, self._light_sessions[at]

    def _renew_light_session(self, at: int, session: Optional[str]) -> None:
        """A light session that was refused or failed moves to a new id (another exit), once per failure:
        a later failure of the same old id finds it already replaced."""
        if session and 0 <= at < len(self._light_sessions) and self._light_sessions[at] == session:
            self._light_sessions[at] = _light_session_id()

    async def _fetch_inner(
        self, url: str, *, domain: str, headers: Optional[Dict[str, str]] = None,
        method: str = "GET", body: Optional[bytes] = None,
    ) -> RouteResult:
        # No "heavy" bail-out.  When every slot is dead the right move is to
        # solve again with a fresh IP, not to fall through to the legacy
        # browser path (which also gets 403 without cookies).

        # Step 2a: always try light path first. If curl_cffi's fingerprint
        # passes without cookies, there's no need for cookie replay at all.
        # This avoids the rate cap from dead cookie slots throttling the
        # entire spider.
        entry = self.cache.get(domain)
        level = entry.level if entry else None
        # `result` is the response we eventually classify for the solve decision.
        # Initialise it here: the cookie-replay branch below only binds it inside
        # its try, so a replay transport error / no-slot path would otherwise hit
        # an UnboundLocalError at the solve step.
        result = None
        transport_error = None
        started = time.monotonic()
        # The request as made rides on every fetch of it: its own headers, and the method
        # and body of anything but a GET (a plain GET's fetches are exactly as they were).
        method = (method or "GET").upper()
        own: Dict = {"extra_headers": headers} if headers else {}
        if method != "GET":
            own.update(method=method, body=body)
        light_result = None
        light_diag = None
        if not self._light_refused(domain, entry):
            light_at, light_session = self._light_session()
            light_own = dict(own, proxy_session_id=light_session) if light_session else own
            try:
                light_result = await self.fetcher.fetch(
                    url=url, proxy_url=self.proxy_url, **light_own,
                )
            except LocalFetchError as exc:
                self._renew_light_session(light_at, light_session)
                # A transport error (timeout / connection reset) is NOT a block — a
                # solve can't fix a dead connection. Remember it so the solve step
                # can back off instead of escalating.
                transport_error = exc
            if light_result is not None and light_result.status_code in (429, 999):
                self._renew_light_session(light_at, light_session)
            light_diag = _diagnose_fetch(light_result, url) if light_result else None
            if light_result is not None and _sets_akamai(light_result.set_cookies):
                self._akamai_seen.add(domain)
            if light_diag is not None:
                if light_diag.blocked:
                    self._light_refused_at[domain] = time.monotonic()
                else:
                    self._light_refused_at.pop(domain, None)
        if light_result and not light_diag.blocked:
            # A cookieless 200 here is often a warm-IP fluke: hard domains like
            # DigiKey serve ~8% of cookieless requests on Evomi IPs that recently
            # passed a challenge. set_light() REPLACES the entry and discards
            # every cookie slot, so a single fluke would nuke a healthy
            # cf_clearance pool — capping the pool at ~1-2 slots and throughput at
            # a fraction of target. Re-read the entry (a concurrent peer may have
            # just built the pool) and only demote when there's no live cookie
            # pool worth protecting.
            entry = self.cache.get(domain)
            has_pool = bool(entry and entry.level == "cookies" and entry.live_slots())
            if not has_pool:
                self.cache.set_light(domain)
            return RouteResult(
                fetch=light_result,
                routing_method="httpcloak_light",
                blocked=False,
                domain_level="cookies" if has_pool else "light",
                diagnosis=light_diag,
            )

        # If a non-None light_result reached here the classifier called it a block
        # above — i.e. it was a genuine block response, not a transport error.
        # That distinction gates whether the solve step is worthwhile below.
        light_blocked = light_result is not None

        # Step 2b: light path failed → try cookie replay if available.
        if entry and level == "cookies":
            slot = entry.next_slot()
            if slot and slot.slot_id in self._resensor_due:
                # the slot's sensor validation lapsed on its last answer: run it again before this request
                rerun = await self._akamai_rerun_sensor(url, domain, slot, own, started)
                if rerun is not None:
                    return rerun
                slot = None
            if slot:
                # Adaptive rate limiting: defer if this slot is already at
                # the learned cap. The router waits inside the request
                # context (counts against Scrapy's CONCURRENT_REQUESTS),
                # which naturally backpressures the spider's queue without
                # needing a separate global throttle.
                if entry.learned_rpm_cap is not None:
                    while slot.requests_per_minute() >= entry.learned_rpm_cap:
                        # Try a different slot first
                        alt = entry.next_slot()
                        if alt and alt.slot_id != slot.slot_id and (
                            alt.requests_per_minute() < entry.learned_rpm_cap
                        ):
                            slot = alt
                            break
                        # All slots at cap — wait briefly for the rolling
                        # window to advance, then re-check.
                        await asyncio.sleep(0.5)
                        slot = entry.next_slot() or slot

                # Record the request BEFORE sending so concurrent peers see
                # an updated RPM and pick a different slot. Mark the cookie
                # busy so next_slot() routes concurrent peers to idle cookies.
                self.cache.record_request(domain, slot.slot_id)
                slot.in_flight += 1

                try:
                    result = await self.fetcher.fetch(
                        url=url,
                        cookies=slot.cookies,
                        user_agent=slot.user_agent,
                        proxy_url=self.proxy_url,
                        proxy_session_id=slot.proxy_session_id,
                        provider=slot.provider,
                        country=slot.country,
                        preset=slot.preset,
                        **own,
                    )
                except LocalFetchError as exc:
                    logger.info(
                        "AutoRouter: cookie replay infra error for %s: %s",
                        domain, exc,
                    )
                    self.cache.record_slot_failure(domain, slot.slot_id, status_code=None)
                    # Infra failures are NOT reported to IP reputation —
                    # not the IP's fault. Also a transport error, not a block:
                    # let the solve step back off rather than escalate.
                    transport_error = exc
                else:
                    handled = await self._akamai_answer(url, domain, slot, result, own, started)
                    if handled is not None:
                        return handled
                    replay_diag = _diagnose_fetch(result, url)
                    if not replay_diag.blocked:
                        self.cache.record_slot_success(domain, slot.slot_id)
                        self._report_ip_outcome(
                            domain=domain, slot=slot, outcome="success",
                        )
                        # Hot-path refill check — opportunistically grow
                        # the pool toward target_pool_size if we're under.
                        self._ensure_refill_task(domain)
                        return RouteResult(
                            fetch=result,
                            routing_method="httpcloak_cookies",
                            blocked=False,
                            domain_level="cookies",
                            slot_id=slot.slot_id,
                            diagnosis=replay_diag,
                        )
                    self.cache.record_slot_failure(
                        domain, slot.slot_id, status_code=result.status_code,
                    )
                    self._report_ip_outcome(
                        domain=domain, slot=slot, outcome="blocked",
                        status_code=result.status_code,
                    )
                finally:
                    slot.in_flight -= 1

        # Step 3: try httpcloak without cookies (light path).
        # Step 3: light path already tried in Step 2a above. If we're here,
        # both light and cookie paths failed.
        else:
            result = None  # known cookies/heavy domain, skip light probe

        # Step 4: solve for cookies.
        #
        # Hold the lock WHILE solving so concurrent requests coalesce: the
        # first to acquire the lock solves once (1 Tessera/browser call),
        # stores a slot, then releases.  The next waiter re-checks the cache
        # at the top of the loop and free-rides on the fresh slot without
        # triggering another solve.
        #
        # The old design fired N parallel solve tasks *outside* the lock. With
        # a cloud solver (Tessera) as the backend this caused every one of the
        # 180 concurrent spider requests to fire 6 parallel calls the moment
        # the pool emptied, flooding the solver with 1 000+ simultaneous
        # requests. The solver rejected most immediately (blank transport
        # error), and only the staggered retry sleeps happened to drain the
        # queue enough for round 5 to get through — adding 20 s overhead per
        # page. Pool-filling parallelism is now handled exclusively by the
        # background _ensure_refill_task / _refill_one path.
        # Transport error, not a block: the light path raised and no cookie
        # replay produced a block response, so there's nothing to classify — and
        # a solve can't fix a dead / timed-out egress connection (usually a burned
        # or overloaded IP). Escalating would waste a solve on a domain that has
        # no challenge. Back off (the middleware falls through and Scrapy retries
        # the request) instead. When we DID see a genuine block (light_blocked, or
        # a blocked cookie-replay response in `result`), fall through to solve.
        if (result is None and not light_blocked and transport_error is not None
                and self._needs_akamai_sensor(domain, method, headers)):
            # Akamai answers a form's POST with an unaccepted _abck with no answer at all (a
            # reset stream, or a held request): the site's verdict, which a sensor solve on the
            # form's page lifts. Not a proxy failure to back off from.
            self._light_refused_at[domain] = time.monotonic()
            return await self._akamai_sensor_route(url, domain, headers, own, transport_error, started)

        if result is None and not light_blocked and transport_error is not None:
            logger.info(
                "AutoRouter: %s transport error (%s); backing off instead of "
                "solving (no challenge to solve)", domain, transport_error,
            )
            return RouteResult(
                fetch=None,
                routing_method="transport_error",
                blocked=False,
                domain_level=level or "light",
                error=str(transport_error),
                diagnosis=dg.diagnose_error(transport_error),
            )

        block_status = result.status_code if result else 403
        seen = result if result is not None else light_result
        seen_diag = _diagnose_fetch(seen, url) if seen is not None else None
        # A rate limit is not a challenge: a solve cannot lift it and costs a browser or a
        # paid token. Hand the response back so the middleware slows down and retries.
        if seen_diag is not None and seen_diag.info.move == dg.SLOW_DOWN:
            logger.info(
                "AutoRouter: %s rate limited at httpcloak (status=%d); backing off instead "
                "of solving", domain, seen.status_code,
            )
            return RouteResult(
                fetch=seen,
                routing_method="httpcloak_cookies" if seen is result else "httpcloak_light",
                blocked=True,
                domain_level=level or "light",
                diagnosis=seen_diag,
            )
        if seen is not None:
            family = challenge_family(seen.status_code, seen.body, headers=seen.headers)
            if family:
                self._challenge_family[domain] = family
        logger.info(
            "AutoRouter: %s blocked at httpcloak (status=%d, %s), solving",
            domain, block_status, self._challenge_family.get(domain) or "vendor unknown",
        )

        winning_slot = None
        for attempt in range(1, 3):  # at most 2 attempts (transient error retry)
            async with self._solve_lock(domain):
                # Re-check: a concurrent waiter may have solved while we queued.
                # Only coalesce on pristine (fail_count==0) slots — those were
                # just minted by another request.  Slots with failures are the
                # ones step 2b already tried; replaying them again just burns
                # another fail_count tick without benefit.
                entry = self.cache.get(domain)
                if entry and entry.level == "cookies":
                    pristine = [s for s in entry.live_slots() if s.fail_count == 0]
                    if pristine:
                        slot = min(pristine, key=lambda s: s.requests_per_minute())
                        logger.debug(
                            "AutoRouter: %s solve coalesced — reusing pristine slot",
                            domain,
                        )
                        return await self._replay_with_slot(url, domain, slot, result, own)

                # We hold the lock — one solve, no parallel fan-out. A solve loads a page;
                # the request (a POST too) is replayed as made.
                page = _calling_page(url, headers)
                if page:
                    self._solve_pages[domain] = page
                winning_slot = await self._try_one_solve(domain, _solve_url(url, method, headers))

            if winning_slot is not None:
                return await self._replay_with_slot(url, domain, winning_slot, result, own)

            if attempt < 2:
                await asyncio.sleep(2)

        logger.warning("AutoRouter: all solve attempts exhausted for %s", domain)
        return RouteResult(
            fetch=result,
            routing_method="solve_then_replay",
            blocked=True,
            domain_level="cookies",
            error="solve failed",
            diagnosis=seen_diag,
        )

    async def _try_one_solve(self, domain: str, url: str):
        """Attempt a single solve. Returns a CookieSlot on success, None on failure."""
        try:
            solve = await self.solve_client.solve(
                domain=domain, target_url=url, provider=self.provider,
                solvers=self.solvers,
                challenge_type=self._challenge_family.get(domain),
            )
        except SolveError as exc:
            logger.debug("AutoRouter: solve attempt failed for %s: %s", domain, exc)
            return None

        if not solve.success:
            logger.debug(
                "AutoRouter: solve returned no cookies for %s", domain,
            )
            return None

        slot = self.cache.set_cookies_from_solve(
            domain=domain,
            cookies=solve.cookies,
            user_agent=solve.user_agent,
            proxy_session_id=solve.proxy_session_id,
            preset=solve.preset,
            egress_ip=solve.egress_ip,
            provider=solve.provider,
            country=solve.country,
        )
        logger.info(
            "AutoRouter: solve succeeded for %s (egress_ip=%s)",
            domain, solve.egress_ip,
        )
        self._ensure_refill_task(domain)
        return slot

    async def _replay_slot_once(self, url, domain, slot, own=None) -> LocalFetchResult:
        """One fetch of ``url`` with a cookie slot's clearance, on the slot's own identity."""
        self.cache.record_request(domain, slot.slot_id)
        slot.in_flight += 1  # busy until the fetch completes (see CookieSlot)
        try:
            return await self.fetcher.fetch(
                url=url,
                cookies=slot.cookies,
                user_agent=slot.user_agent,
                proxy_url=self.proxy_url,
                proxy_session_id=slot.proxy_session_id,
                provider=slot.provider,
                country=slot.country,
                preset=slot.preset,
                **(own or {}),
            )
        finally:
            slot.in_flight -= 1

    async def _replay_with_slot(self, url, domain, slot, original_result, own=None):
        """Replay a URL with a cookie slot (``own``: the request's own fetch arguments: its
        headers, and the method and body of anything but a GET). Returns RouteResult.

        A fresh Cloudflare clearance (a slot with a cf_clearance cookie, never replayed yet)
        that Cloudflare refuses on this first replay is replayed once more after
        ``warmup_replay_delay_s`` (cookie warm-up), before the router gives up on it. The
        refusal counts against the slot (MAX_SLOT_FAILURES tolerates it), not its IP."""
        fresh = slot.fail_count == 0 and slot.success_count == 0 and _cloudflare_clearance(slot)
        started = time.monotonic()
        try:
            replay = await self._replay_slot_once(url, domain, slot, own)
            delay = float(self.warmup_replay_delay_s or 0)
            if (fresh and delay > 0 and _diagnose_fetch(replay, url).blocked
                    and _warmup_refusal(replay)):
                self.cache.record_slot_failure(
                    domain, slot.slot_id, status_code=replay.status_code,
                )
                logger.info(
                    "AutoRouter: a fresh clearance for %s was refused on its first replay "
                    "(status=%d); replaying once more in %.1fs (cookie warm-up)",
                    domain, replay.status_code, delay,
                )
                await asyncio.sleep(delay)
                replay = await self._replay_slot_once(url, domain, slot, own)
        except LocalFetchError as exc:
            logger.warning(
                "AutoRouter: replay error for %s: %s", domain, exc,
            )
            return RouteResult(
                fetch=original_result,
                routing_method="solve_then_replay",
                blocked=True,
                domain_level="cookies",
                error=str(exc),
                diagnosis=dg.most_informative([
                    _diagnose_fetch(original_result, url) if original_result else None,
                    dg.diagnose_error(exc),
                ]),
            )

        handled = await self._akamai_answer(url, domain, slot, replay, own, started)
        if handled is not None:
            return handled
        replay_diag = _diagnose_fetch(replay, url)
        if replay_diag.blocked:
            if replay_diag.cls in (dg.CHALLENGE, dg.EDGE_DENY):
                # The site refused the clearance a solve just earned.
                replay_diag = dg.Diagnosis(dg.CLEARANCE_REFUSED, replay_diag.vendor,
                                           replay_diag.status, "replay_refused")
            self.cache.record_slot_failure(
                domain, slot.slot_id, status_code=replay.status_code,
            )
            self._report_ip_outcome(
                domain=domain, slot=slot, outcome="blocked",
                status_code=replay.status_code,
            )
            return RouteResult(
                fetch=replay,
                routing_method="solve_then_replay",
                blocked=True,
                domain_level="cookies",
                error=f"replay blocked (status={replay.status_code})",
                diagnosis=replay_diag,
            )

        self._report_ip_outcome(
            domain=domain, slot=slot, outcome="success",
        )
        return RouteResult(
            fetch=replay,
            routing_method="solve_then_replay",
            blocked=False,
            domain_level="cookies",
            slot_id=slot.slot_id,
            diagnosis=replay_diag,
        )

    # -- Akamai Bot Manager in front of a form -------------------------------------------------------

    def _needs_akamai_sensor(self, domain: str, method: str, headers: Optional[Dict[str, str]]) -> bool:
        """Whether a transport error on this request is Akamai withholding the answer: a request other
        than a GET (a form's POST) to a host whose pages set Akamai's sensor cookies, sent with no
        accepted ``_abck`` of its own."""
        if (method or "GET").upper() in ("GET", "HEAD"):
            return False
        own = _cookie_of(_request_cookies(headers), "_abck")
        if own is not None:
            return _abck_accepted(own) is False
        return domain in self._akamai_seen

    def _solve_provider(self, slot) -> Optional[str]:
        """The ``provider`` a solve on a slot's own exit names: Mimic's is the Veil provider of the exit;
        Tessera's would force a solver, so it is left out there."""
        return slot.provider if getattr(self.solve_client, "backend", None) == "mimic" else None

    async def _akamai_sensor_route(self, url, domain, headers, own, transport_error, started) -> RouteResult:
        """Solve Akamai's sensor on the form's page and replay the request on that solve's exit."""
        page = self._solve_pages.get(domain) or _sensor_page(url, headers)
        self._solve_pages[domain] = page
        self._challenge_family[domain] = "akamai"
        self._akamai_forms.add(domain)
        logger.info(
            "AutoRouter: %s gave no answer to a %s without an accepted _abck (%s); solving Akamai's "
            "sensor on %s", domain, "form request", transport_error, page,
        )
        slot = None
        async with self._solve_lock(domain):
            entry = self.cache.get(domain)
            if entry and entry.level == "cookies":
                pristine = [s for s in entry.live_slots() if s.fail_count == 0]
                if pristine:
                    slot = min(pristine, key=lambda s: s.requests_per_minute())
            if slot is None:
                slot = await self._try_one_solve(domain, page)
        if slot is None:
            return RouteResult(
                fetch=None, routing_method="solve_then_replay", blocked=True, domain_level="cookies",
                error=f"akamai sensor solve failed after: {transport_error}",
                diagnosis=dg.diagnose_error(transport_error),
            )
        return await self._replay_with_slot(url, domain, slot, None, own)

    async def _akamai_answer(self, url, domain, slot, fetch: LocalFetchResult, own, started: float,
                             fix: bool = True) -> Optional[RouteResult]:
        """What a slot's response from an Akamai host calls for, once per request (``fix``): its
        challenge action answered on the slot's session, or its sensor run again after the lapse.
        Keeps the slot's Akamai cookies current from the response, as a browser does. None: the
        response stands as it is."""
        akamai = domain in self._akamai_forms
        diag = _diagnose_fetch(fetch, url)
        captcha = diag.evidence == "akamai_captcha"
        if not (akamai or captcha):
            return None
        host = urlparse(url).hostname or domain
        accepted_before = _abck_accepted(_cookie_of(slot.cookies, "_abck"))
        reset = _abck_reset(fetch.set_cookies)
        slot.cookies = _absorb(slot.cookies, fetch.set_cookies, host, _AKAMAI_KEPT)
        if not fix:
            return None
        if captcha:
            return await self._answer_akamai_captcha(url, domain, slot, fetch, own, started)
        if akamai and reset and accepted_before and fetch.status_code != 404 and not diag.blocked:
            # the answer came, and the sensor's validation lapsed with it: it stands, and the slot's
            # next request runs the sensor first
            self._resensor_due.add(slot.slot_id)
            return None
        lapsed = (reset and accepted_before) or (
            fetch.status_code == 404 and _abck_accepted(_cookie_of(slot.cookies, "_abck")) is False)
        if akamai and lapsed:
            return await self._akamai_rerun_sensor(url, domain, slot, own, started)
        return None

    def _session_of(self, slot, own, fetch: LocalFetchResult, host: str) -> List[Dict]:
        """The cookies a slot's request was sent with (the slot's, then the request's own that the slot
        lacks), as its response left them."""
        cookies = [dict(c) for c in slot.cookies or []]
        have = {c.get("name") for c in cookies}
        for c in _request_cookies((own or {}).get("extra_headers")):
            if c["name"] not in have:
                cookies.append({"name": c["name"], "value": c["value"], "domain": host, "path": "/"})
        return _absorb(cookies, fetch.set_cookies, host)

    async def _answer_akamai_captcha(self, url, domain, slot, fetch: LocalFetchResult, own,
                                     started: float) -> Optional[RouteResult]:
        """Akamai's challenge action met on a slot: the solve service answers it on the slot's session
        and exit (a paid recaptcha token, under the run's paid-solve policy); its answer is the
        request's response. None when it was not answered (the challenge page stands)."""
        host = urlparse(url).hostname or domain
        answered_at = self._cpt_answered.get(slot.slot_id, 0.0)
        if (answered_at <= started and time.monotonic() - answered_at < CAPTCHA_MIN_INTERVAL_S
                and domain in self._akamai_forms):
            # answered on this slot moments ago and met again: not bought again at once. The sensor is run again on
            # the slot's exit (free) and the request replayed; met once more, the challenge page stands.
            if self._resensored.get(slot.slot_id, 0.0) > started:
                logger.info("AutoRouter: Akamai's challenge action on %s again right after its answer and a sensor "
                            "run; not answered again", domain)
                return None
            return await self._akamai_rerun_sensor(url, domain, slot, own, started)
        async with self._solve_lock(domain):
            if self._cpt_answered.get(slot.slot_id, 0.0) > started:
                # answered for this slot while this request waited: its replay carries the answer
                replay = await self._replay_slot_once(url, domain, slot, own)
                return await self._akamai_answer(url, domain, slot, replay, own, started, fix=False) or RouteResult(
                    fetch=replay, routing_method="httpcloak_cookies", blocked=_diagnose_fetch(replay, url).blocked,
                    domain_level="cookies", slot_id=slot.slot_id, diagnosis=_diagnose_fetch(replay, url))
            logger.info("AutoRouter: %s answered with Akamai's challenge action; asking the solve service to "
                        "answer it on slot %s's session", domain, slot.slot_id)
            try:
                solve = await self.solve_client.solve(
                    domain=domain, target_url=fetch.final_url or url, challenge_type="akamai_captcha",
                    proxy_session_id=slot.proxy_session_id, provider=self._solve_provider(slot),
                    country=slot.country, page_html=(fetch.body or b"").decode("utf-8", "replace"),
                    cookies=self._session_of(slot, own, fetch, host), user_agent=slot.user_agent,
                    paid=True, deadline_ms=CAPTCHA_DEADLINE_MS,
                )
            except SolveError as exc:
                logger.info("AutoRouter: Akamai's challenge action on %s not answered: %s", domain, exc)
                return None
            if not solve.success or solve.response is None:
                logger.info("AutoRouter: Akamai's challenge action on %s not answered: %s", domain,
                            solve.error or "no response")
                return None
            self._cpt_answered[slot.slot_id] = time.monotonic()
            slot.cookies = [{"name": c["name"], "value": c["value"], "domain": c.get("domain") or host,
                             "path": c.get("path") or "/"} for c in solve.cookies]
        r = solve.response
        answered = LocalFetchResult(url=url, final_url=r.url or url, status_code=r.status, headers=dict(r.headers),
                                    body=r.body, backend=f"solve:{solve.engine or 'tessera'}",
                                    set_cookies=list(r.set_cookies))
        diag = _diagnose_fetch(answered, url)
        if diag.blocked:
            self.cache.record_slot_failure(domain, slot.slot_id, status_code=answered.status_code)
        else:
            self.cache.record_slot_success(domain, slot.slot_id)
        logger.info("AutoRouter: Akamai's challenge action on %s answered (status=%d)", domain, r.status)
        return RouteResult(fetch=answered, routing_method="captcha_solve", blocked=diag.blocked,
                           domain_level="cookies", slot_id=slot.slot_id, diagnosis=diag)

    async def _akamai_rerun_sensor(self, url, domain, slot, own, started: float) -> Optional[RouteResult]:
        """The sensor's validation lapsed on a slot: run the sensor again on the slot's exit, merge its
        cookies into the slot (the site's session and ``sec_cpt`` kept) and replay the request once.
        None when the sensor did not run (the lapsed response stands)."""
        page = self._solve_pages.get(domain) or _sensor_page(url, (own or {}).get("extra_headers"))
        async with self._solve_lock(domain):
            if self._resensored.get(slot.slot_id, 0.0) <= started:
                logger.info("AutoRouter: Akamai's sensor validation lapsed on %s (slot %s); running the sensor "
                            "again on the slot's exit", domain, slot.slot_id)
                try:
                    solve = await self.solve_client.solve(
                        domain=domain, target_url=page, challenge_type="akamai",
                        proxy_session_id=slot.proxy_session_id, provider=self._solve_provider(slot),
                        country=slot.country,
                    )
                except SolveError as exc:
                    logger.info("AutoRouter: the sensor run on %s failed: %s", domain, exc)
                    return None
                if not solve.success or not solve.cookies:
                    logger.info("AutoRouter: the sensor run on %s earned no cookies (%s)", domain, solve.error)
                    return None
                slot.cookies = _merge_sensor(slot.cookies, solve.cookies)
                self._resensor_due.discard(slot.slot_id)
                if solve.proxy_session_id and solve.proxy_session_id != slot.proxy_session_id:
                    # the site denied the slot's exit and the solve moved to another: the slot follows it
                    slot.proxy_session_id = solve.proxy_session_id
                    slot.egress_ip = solve.egress_ip or slot.egress_ip
                self._resensored[slot.slot_id] = time.monotonic()
        try:
            replay = await self._replay_slot_once(url, domain, slot, own)
        except LocalFetchError as exc:
            return RouteResult(fetch=None, routing_method="solve_then_replay", blocked=True, domain_level="cookies",
                               error=str(exc), diagnosis=dg.diagnose_error(exc))
        diag = _diagnose_fetch(replay, url)
        if diag.evidence == "akamai_captcha":
            answered = await self._akamai_answer(url, domain, slot, replay, own, started)
            if answered is not None:
                return answered
        else:
            await self._akamai_answer(url, domain, slot, replay, own, started, fix=False)
        if diag.blocked:
            self.cache.record_slot_failure(domain, slot.slot_id, status_code=replay.status_code)
        else:
            self.cache.record_slot_success(domain, slot.slot_id)
        return RouteResult(fetch=replay, routing_method="solve_then_replay", blocked=diag.blocked,
                           domain_level="cookies", slot_id=slot.slot_id, diagnosis=diag)
