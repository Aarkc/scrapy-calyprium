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

## License

MIT
