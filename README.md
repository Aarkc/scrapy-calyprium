# scrapy-calyprium

Anti-detection [Scrapy](https://scrapy.org) middleware for web scraping — proxy routing and stealth browser rendering powered by [Calyprium](https://calyprium.com).

## Install

```bash
pip install scrapy-calyprium
```

## Quick Start

```python
# settings.py
import scrapy_calyprium

scrapy_calyprium.configure(api_key="clp_your_key_here")
```

This auto-configures:
- **VeilProxyMiddleware** — routes requests through rotating proxies with TLS fingerprinting
- **MimicBrowserMiddleware** — renders JavaScript pages with stealth browser instances
- **S3 feed storage** — write spider output to Calyprium storage using Scrapy's built-in `S3FeedStorage`

## Usage

### Automatic Configuration (recommended)

```python
# settings.py
import scrapy_calyprium

scrapy_calyprium.configure(
    api_key="clp_your_key_here",
    mimic_stealth_level="maximum",  # basic, moderate, maximum
)
```

### Manual Configuration

```python
# settings.py
DOWNLOADER_MIDDLEWARES = {
    "scrapy_calyprium.VeilProxyMiddleware": 100,
    "scrapy_calyprium.MimicBrowserMiddleware": 200,
}

CALYPRIUM_API_KEY = "clp_your_key_here"
VEIL_USER_ID = "your-user-id"
```

### Request Tracing

`CalypriumTraceMiddleware` (added by `configure()`) gives every request one trace: the wait in Scrapy's queue,
each download attempt, the challenge solve if there is one, and the callback that parses the page. The SDK's calls
to Mimic and Tessera carry the trace (`traceparent`), so their steps join it. On the Calyprium platform the
trace of every request of a run is shown with its run. To set it up by hand, register it as both a downloader and
a spider middleware:

```python
DOWNLOADER_MIDDLEWARES = {"scrapy_calyprium.CalypriumTraceMiddleware": 50}
SPIDER_MIDDLEWARES = {"scrapy_calyprium.CalypriumTraceMiddleware": 50}
```

### Failure Diagnosis and Retries

Every download attempt gets a diagnosis from the fetch layer's shared failure classifier (the same
one Mimic and Tessera use), in `response.meta["calyprium_diagnosis"]`:
`{"class", "number", "side", "move", "vendor"?, "status"?, "evidence"?, "confidence"?, "retry_after"?}`.
A transport error leaves it on `failure.request.meta` for an errback. When Mimic's own call failed
and the request fell back to a plain download, `meta["calyprium_fallback_diagnosis"]` and
`meta["calyprium_fetch_error"]` say why.

`MimicBrowserMiddleware` retries an attempt when its diagnosis's move is worth another try:
`retry_elsewhere` and `climb` at once, `retry_later` after `CALYPRIUM_RETRY_DELAY`, `slow_down`
after a doubling backoff (a `Retry-After` wins), `fix_ours` once, and never a `stop` (not found,
sign-in / geo / consent walls, policy). A retry carries `meta["calyprium_attempt_number"]` and
`meta["calyprium_prior_class"]`, which Mimic receives to resume its ladder. `meta["dont_retry"]`
and `meta["max_retry_times"]` work as with Scrapy's RetryMiddleware; with that middleware enabled,
the statuses in `RETRY_HTTP_CODES` are left to it. Stats: `calyprium/retry/<class>` per retry,
`calyprium/class/<class>` per final response.

### Saving Output to Calyprium Storage

Spider output is saved to Calyprium's S3-compatible storage using Scrapy's built-in feed export:

```python
# settings.py
import scrapy_calyprium

scrapy_calyprium.configure(api_key="clp_your_key_here")

FEEDS = {
    "s3://calyprium/my-spider/%(time)s.jl": {
        "format": "jsonlines",
    },
}
```

The S3 credentials are auto-configured by `configure()` — no additional setup needed.

### Browser Rendering

Mark requests that need JavaScript rendering:

```python
import scrapy

class MySpider(scrapy.Spider):
    name = "example"

    def start_requests(self):
        # Regular request (proxy only)
        yield scrapy.Request("https://example.com")

        # Browser-rendered request
        yield scrapy.Request(
            "https://example.com/spa",
            meta={"mimic": True},
        )
```

## Authentication

All middleware requires a valid API key. Set it via:

1. `scrapy_calyprium.configure(api_key="clp_...")`
2. `CALYPRIUM_API_KEY` environment variable

## Settings Reference

| Setting | Description | Default |
|---------|-------------|---------|
| `CALYPRIUM_API_KEY` | API key for all services | — |
| `VEIL_GATEWAY_URL` | Proxy gateway URL | `https://proxy.calyprium.com` |
| `VEIL_USER_ID` | User ID for proxy routing | — |
| `VEIL_PROFILE` | Proxy routing profile | — |
| `VEIL_PROXY_TYPE` | `datacenter`, `residential`, `residential_rotating` | — |
| `MIMIC_SERVICE_URL` | Mimic browser service URL | `https://mimic.calyprium.com` |
| `MIMIC_STEALTH_LEVEL` | `basic`, `moderate`, `maximum` | `moderate` |
| `MIMIC_BROWSER_ENGINE` | Specific browser engine | auto |
| `MIMIC_USE_PROXY` | Route browser through proxy | `False` |
| `MIMIC_ALL_REQUESTS` | Render all requests via browser | `False` |
| `MIMIC_USE_SPECTRE` | Use device fingerprints | `True` |
| `CALYPRIUM_RETRY_ENABLED` | Retry attempts whose diagnosis says so | `True` |
| `CALYPRIUM_RETRY_TIMES` | Retries per request | `2` |
| `CALYPRIUM_RETRY_DELAY` | Seconds before a `retry_later`; base of the `slow_down` backoff | `5` |
| `CALYPRIUM_RETRY_MAX_DELAY` | Cap on any retry delay (seconds) | `60` |
| `PRISM_WAIT_FOR_SCAN_S` | `PrismSitemapSpider`: how long to wait for Prism to index an unknown domain before closing as `prism_no_urls` (ends 30 s before `CLOSESPIDER_TIMEOUT`) | `900` |

## Development

`src/scrapy_calyprium/routing/diagnosis.py` is **vendored**: it is Mimic's failure classifier,
copied verbatim from the Calyprium monorepo (`stealth/mimic/mimic/routing/diagnosis.py`). Never
edit it here; when Mimic changes it, copy the file again (its `VERSION` constant says which
contract it is) and run the tests. SDK-side adaptations live in `routing/block_detect.py`.

## License

MIT
