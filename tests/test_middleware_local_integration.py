"""AAR-17: smoke tests for the MimicBrowserMiddleware local-first integration.

These tests stub the local router and exercise just the glue between the
middleware and the SpiderAutoRouter. The deep routing logic is covered by
`test_local_routing.py`.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from scrapy.http import Request

from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.routing.auto import RouteResult
from scrapy_calyprium.routing.local_fetch import LocalFetchResult


def _make_middleware(local_router=None) -> MimicBrowserMiddleware:
    mw = MimicBrowserMiddleware(
        service_url="http://mimic.test",
        api_key="test",
    )
    mw._local_enabled = local_router is not None
    mw._local_router = local_router
    mw._local_cache = MagicMock()
    return mw


class FakeRouter:
    def __init__(self):
        self.results = []
        self.fetch_calls = []

    def queue(self, *items):
        self.results.extend(items)

    async def fetch(self, url, *, domain):
        self.fetch_calls.append((url, domain))
        return self.results.pop(0)


@pytest.mark.asyncio
async def test_local_route_returns_response_with_raw_bytes():
    pdf_bytes = b"%PDF-1.7\n%\xb5\xed\xae\xfb\n" + bytes(range(256))
    router = FakeRouter()
    router.queue(RouteResult(
        fetch=LocalFetchResult(
            url="https://example.com/file.pdf",
            final_url="https://example.com/file.pdf",
            status_code=200,
            headers={"content-type": "application/pdf"},
            body=pdf_bytes,
            backend="httpcloak",
        ),
        routing_method="httpcloak_light",
        blocked=False,
        domain_level="light",
    ))

    mw = _make_middleware(local_router=router)
    request = Request("https://example.com/file.pdf")

    response = await mw._try_local_route(request, spider=None)

    assert response is not None
    assert response.status == 200
    # Raw bytes preserved — AAR-12 fix point
    assert response.body == pdf_bytes
    assert b"\xef\xbf\xbd" not in response.body
    assert response.headers.get(b"Content-Type") == b"application/pdf"
    assert mw._local_stats["local_success"] == 1
    assert router.fetch_calls[0] == ("https://example.com/file.pdf", "example.com")


@pytest.mark.asyncio
async def test_local_route_falls_through_when_legacy_needed():
    router = FakeRouter()
    router.queue(RouteResult(
        fetch=None,
        routing_method="fallback_legacy",
        blocked=True,
        domain_level="heavy",
        needs_legacy_fallback=True,
    ))

    mw = _make_middleware(local_router=router)
    request = Request("https://example.com/")

    response = await mw._try_local_route(request, spider=None)

    assert response is None
    assert mw._local_stats["fallback_legacy"] == 1


@pytest.mark.asyncio
async def test_per_request_force_browser_skips_local():
    router = FakeRouter()
    mw = _make_middleware(local_router=router)
    request = Request("https://example.com/", meta={"mimic_force_browser": True})

    response = await mw._try_local_route(request, spider=None)

    assert response is None
    assert router.fetch_calls == []


@pytest.mark.asyncio
async def test_per_request_local_skip_skips_local():
    router = FakeRouter()
    mw = _make_middleware(local_router=router)
    request = Request("https://example.com/", meta={"mimic_local_skip": True})

    response = await mw._try_local_route(request, spider=None)

    assert response is None
    assert router.fetch_calls == []


@pytest.mark.asyncio
async def test_disabled_returns_none_immediately():
    mw = _make_middleware(local_router=None)
    mw._local_enabled = False

    request = Request("https://example.com/")
    response = await mw._try_local_route(request, spider=None)

    assert response is None


@pytest.mark.asyncio
async def test_router_exception_falls_through_to_legacy():
    class ExplodingRouter:
        async def fetch(self, url, *, domain):
            raise RuntimeError("router exploded")

    mw = _make_middleware(local_router=ExplodingRouter())
    request = Request("https://example.com/")

    response = await mw._try_local_route(request, spider=None)

    assert response is None
    assert mw._local_stats["fallback_legacy"] == 1
