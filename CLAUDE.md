# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

scrapy-calyprium is a public PyPI package (`pip install scrapy-calyprium`) providing anti-detection Scrapy middleware for proxy routing, device fingerprinting, and browser rendering. It's the SDK that connects Scrapy spiders to the Calyprium platform services (Veil, Mimic, Spectre, Prism, Forge).

## Development Commands

```bash
pip install -e ".[dev,local]"        # Install with dev + local-first deps
pytest                                # Run all tests
pytest tests/test_block_detect.py     # Single test file
pytest -m network                     # Tests that hit real URLs (usually skipped)
pytest -k "test_rate_cap"             # Run tests matching name pattern
```

Commits must follow Conventional Commits (enforced by husky + commitlint). Semantic Release on push to main auto-publishes to PyPI.

## Architecture

### Request Flow

Scrapy requests pass through a three-layer middleware stack in priority order:

1. **VeilProxyMiddleware** (100) — Injects proxy URL + `Proxy-Authorization` header pointing at the Veil gateway. Provider and session ID are encoded in the username (`user-p_webshare_rotating-session_abc123`).

2. **SpectreMiddleware** (150) — Fetches a device fingerprint from the Spectre service and sets User-Agent + client hints. Tracks fingerprint rotation and block detection.

3. **MimicBrowserMiddleware** (200) — Two modes:
   - **Local-first** (`MIMIC_LOCAL_FETCH=True`): Requests go through `SpiderAutoRouter` which does in-process TLS-fingerprinted HTTP via httpcloak/curl_cffi with cached cookies. Only calls Mimic `/api/solve` when blocked. This is the production path.
   - **Legacy**: Routes through Mimic's `/api/fetch` or `/api/session` for server-side browser rendering.

### Request Traces

`CalypriumTraceMiddleware` (`middleware/tracing.py`, downloader and spider middleware, priority 50) gives each
request a W3C trace id and each download attempt a span id, kept in `request.meta` (`_trace.py`). The tracer
(`extensions/request_tracer.py`) sends each attempt to Forge's `/traces` as before (that span is what Forge prices;
its `trace_id` field stays a per-attempt id) with the trace ids added (`otel_*`), and the `spider.request` /
`spider.parse` spans to Forge's `/spans`. Calls to Mimic and Tessera carry `traceparent` from `_trace.ATTEMPT`,
which the Mimic middleware sets inside its own coroutine (never in a shared callback).

### Failure Diagnosis and the Calyprium Retry

`routing/diagnosis.py` is Mimic's failure classifier, **vendored verbatim** from the monorepo
(`stealth/mimic/mimic/routing/diagnosis.py`, standard library only): never edit it here, re-copy it
when Mimic changes it (`tests/test_diagnosis_vendored.py` checks `VERSION` and the stdlib-only rule).
`routing/block_detect.py` adapts SDK inputs to it (`diagnose()`), and `is_blocked()` is "the class is
in `BLOCKED_CLASSES`", so Mimic's block-page rules reach the SDK with the re-copy. `challenge_family()`
(the `challenge_type` a solve names) follows Mimic's `challenge.classify_challenge` / `api_family_of`. `_outcome.py` names the meta keys (`calyprium_diagnosis`, `calyprium_fetch_error`,
`calyprium_fallback_diagnosis`, `calyprium_attempt_number`, `calyprium_prior_class`,
`calyprium_routing_method`). The Mimic middleware keeps each attempt's diagnosis (Mimic's
`routing.diagnosis` when sent), retries by the diagnosis's move (`CALYPRIUM_RETRY_*`), traces a retried
response itself (`response_received` never fires for it), and sends `attempt` / `prior_class` to
`/api/fetch`. Retry delays use `_wait.sleep` (asyncio or Twisted, per reactor); a download slot's delay
would not apply to Mimic-served requests, which never enter the slot queue.

### Local-First Auto-Routing (`routing/`)

This is the most complex subsystem. The flow per request:

1. **DomainCache** classifies the domain: `light` (no cookies needed), `cookies` (Cloudflare-protected), or `heavy` (browser-only).
2. If `cookies`: pick a `CookieSlot` from the pool, call `LocalFetcher.fetch()` with the slot's cookies + user_agent + proxy_session_id + TLS preset.
3. If blocked: call `SolveClient.solve()` → Mimic does a browser solve → returns cookies + UA + session_id.
4. Cache the new slot, retry with cookies. Background refill loop keeps the pool at `target_pool_size`.

Key constraint: **TLS preset must match the browser that earned the cookies.** Camoufox (Firefox) cookies must be replayed with a Firefox httpcloak preset, not Chrome. Cloudflare correlates JA3/JA4 fingerprint with cookies. A Chrome slot is replayed on the preset presenting its user agent's Chrome (`chrome_preset.replay_preset(preset, user_agent)`, applied in `LocalFetcher`, as Mimic does): Tessera names `chrome-latest-windows` (Chrome 152 in httpcloak 1.7.2) for its Chrome/155 user agent, so the replay becomes `chrome-155-windows`, derived from the newest bundled Windows preset.

A fresh Cloudflare clearance (cf_clearance, never replayed) that Cloudflare refuses on its first post-solve replay is replayed once more after `SpiderAutoRouter.warmup_replay_delay_s` (2 s, Mimic's `NATIVE_REPLAY_RETRY_DELAY_S`) before the router gives up on it; that refusal counts on the slot, not against its IP.

Every local fetch of a request sends the headers the spider set on it (`mimic._local_headers`: not the Spectre fingerprint's, which `meta["spectre_fingerprint_headers"]` names, nor Scrapy's automatic Referer, see `_headers.py`). The fetcher keeps its identity: `local_fetch._OWNED_HEADERS` (User-Agent, client hints, Accept-Encoding, connection and proxy headers) are never taken from the request, and a request's Cookie header joins the slot's cookies. A request other than a GET (a storefront's GraphQL or search POST) sends its method and body on the light fetch and every replay; its solve loads a page (`auto._solve_url`: the URL itself, or the homepage for an API endpoint), since `/api/solve` only opens a URL. A page's script calling another host of its site (an `Origin` header naming that host: never sent on a navigation) solves on the calling page, the `Referer` or the origin's homepage (`auto._calling_page`), and the domain's pool refills solve there too: the API host's own URLs may be no page at all (businesssearchapi.ohiosos.gov blocks whatever does not come from its search page).

### Cookie Slot Lifecycle

Each `CookieSlot` in the `DomainCache` has:
- `cookies`, `user_agent`, `proxy_session_id`, `preset`, `egress_ip`, `provider`
- `fail_count` — incremented on 403; slot dies at `MAX_SLOT_FAILURES` (3)
- `requests_per_minute()` — adaptive rate limiting tracks RPM per slot
- `learned_rpm_cap` — Cloudflare's observed block threshold per domain

### Configuration Resolution

`scrapy_calyprium.configure(api_key=...)` in a spider's `settings.py` injects all middleware/pipeline settings into the caller's globals via `inspect.currentframe()`. Settings resolve: explicit args → env vars → defaults.

## Key Settings

| Setting | Purpose |
|---------|---------|
| `CALYPRIUM_API_KEY` | Master API key for all services |
| `MIMIC_LOCAL_FETCH` | Enable local-first httpcloak routing (bool) |
| `MIMIC_LOCAL_PROXY_URL` | Proxy URL with embedded auth for httpcloak; derived from `VEIL_GATEWAY_URL` as `calyprium:<CALYPRIUM_API_KEY>` when absent |
| `VEIL_PROVIDER` | Upstream proxy provider (e.g. `webshare_rotating`) |
| `VEIL_GATEWAY_URL` | Veil proxy gateway URL |
| `RECRAWL_TRACKING_ENABLED` | Filter out recently-crawled URLs via Forge freshness API |
| `SPIDER_ID`, `RUN_ID`, `MIMIC_ALLOW_PAID_SOLVE`, `MIMIC_PAID_SOLVE_MODE`, `MIMIC_ALLOWED_ENGINES`, `VEIL_ALLOWED_PROVIDERS`, `VEIL_COUNTRY` (or `MIMIC_PROXY_COUNTRY`) | Profile policy from forge; forwarded to Veil (username params) and Mimic/Tessera (request fields) via `_policy.RunPolicy`. The country is one resolution for every hop; a solve's own country (Mimic's `SolveResponse.country`) rides on its slot and replaces the run's on the slot's replays |
| `VEIL_PROXY_TYPE` | Veil username `-type_`; also the tier (`fast` / `residential`) `CalypriumRequestTracer` puts on Veil-path spans |
| `FORGE_SERVICE_SECRET` | Legacy Forge auth; only used when no `CALYPRIUM_API_KEY` (see `_forge.ForgeAuth`) |
| `CALYPRIUM_RETRY_ENABLED`, `CALYPRIUM_RETRY_TIMES`, `CALYPRIUM_RETRY_DELAY`, `CALYPRIUM_RETRY_MAX_DELAY` | Calyprium retry in `MimicBrowserMiddleware` (on by default; 2 retries, 5 s base, 60 s cap) |
| `PRISM_WAIT_FOR_SCAN_S` | `PrismSitemapSpider`: wait for Prism to index an unknown domain (404) before closing as `prism_no_urls` (default 900 s); refill failures back off 2 s doubling to 60 s |
| `SNAPSHOTS_ENABLED`, `SNAPSHOTS_SAMPLE`, `SNAPSHOTS_ERRORS`, `SNAPSHOTS_MAX_BYTES` | `SnapshotRecorder` extension: gzipped sample/error/blocked pages uploaded to `{run prefix}/snapshots/{sha256}.html.gz` and reported to forge `POST /jobs/spiders/{slug}/runs/{n}/snapshots` at close |

## Code Conventions

- Type hints on all function signatures
- Pydantic-free: uses dataclasses throughout
- AAR-XX references in comments point to internal design docs (Anti-Anti-bot Research tickets)
- `LocalFetcher` preserves response body as raw bytes — never decode to str (AAR-12 fix)
- httpcloak is sync (Rust FFI); wrapped in `asyncio.to_thread()` for async compatibility
- `CalypriumRequestTracer` must emit exactly one top-level span per download attempt (forge prices each into `usage_events`): `SpiderAutoRouter.fetch()` records its own and marks `meta["calyprium_trace_id"]`; everything else is traced from the `response_received` signal using `meta["calyprium_routing"]` (mimic's `routing` object) or the Veil/direct path; transport errors via `MimicBrowserMiddleware.process_exception`; a response the Calyprium retry replaces via `MimicBrowserMiddleware._retry`. A span's `status` is the attempt's diagnosis (`_outcome.outcome`), never "success" for a transport error
