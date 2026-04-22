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

### Local-First Auto-Routing (`routing/`)

This is the most complex subsystem. The flow per request:

1. **DomainCache** classifies the domain: `light` (no cookies needed), `cookies` (Cloudflare-protected), or `heavy` (browser-only).
2. If `cookies`: pick a `CookieSlot` from the pool, call `LocalFetcher.fetch()` with the slot's cookies + user_agent + proxy_session_id + TLS preset.
3. If blocked: call `SolveClient.solve()` → Mimic does a browser solve → returns cookies + UA + session_id.
4. Cache the new slot, retry with cookies. Background refill loop keeps the pool at `target_pool_size`.

Key constraint: **TLS preset must match the browser that earned the cookies.** Camoufox (Firefox) cookies must be replayed with a Firefox httpcloak preset, not Chrome. Cloudflare correlates JA3/JA4 fingerprint with cookies.

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
| `MIMIC_LOCAL_PROXY_URL` | Proxy URL with embedded auth for httpcloak (required for local-first on Cloudflare sites) |
| `VEIL_PROVIDER` | Upstream proxy provider (e.g. `webshare_rotating`) |
| `VEIL_GATEWAY_URL` | Veil proxy gateway URL |
| `RECRAWL_TRACKING_ENABLED` | Filter out recently-crawled URLs via Forge freshness API |

## Code Conventions

- Type hints on all function signatures
- Pydantic-free: uses dataclasses throughout
- AAR-XX references in comments point to internal design docs (Anti-Anti-bot Research tickets)
- `LocalFetcher` preserves response body as raw bytes — never decode to str (AAR-12 fix)
- httpcloak is sync (Rust FFI); wrapped in `asyncio.to_thread()` for async compatibility
