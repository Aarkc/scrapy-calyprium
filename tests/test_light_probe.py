"""A domain whose plain fetch the site refuses goes straight to its cookie slots.

On a walled host (an AWS WAF challenge on every cookieless request, a Cloudflare zone) each plain try
before the replay is a wasted request, and on a .gov host a rationed residential one: the gateway allows
a vendor ~500 requests a day to a new registrable domain. Once the plain fetch was refused and the
domain has live slots, requests skip it for light_probe_interval seconds; then one request tries it
again, and a plain fetch that passes puts the domain back on plain-first.
"""
from __future__ import annotations

import pytest

from scrapy_calyprium.routing.auto import SpiderAutoRouter
from scrapy_calyprium.routing.domain_cache import DomainCache

from tests.test_local_routing import FakeFetcher, FakeSolveClient, _blocked_403, _make_router, _ok, _solve_ok

URL = "https://example.com/"
DOMAIN = "example.com"


def _walled(fetcher, solve, interval=60.0):
    router = _make_router(fetcher=fetcher, solve=solve)
    router.light_probe_interval = interval
    return router


async def _first_request(router, fetcher, solve):
    """Plain fetch refused -> solve -> replay: the domain now has a live slot."""
    fetcher.queue(_blocked_403(), _ok())
    solve.queue(_solve_ok())
    result = await router.fetch(URL, domain=DOMAIN)
    assert not result.blocked and result.routing_method == "solve_then_replay"
    assert len(fetcher.calls) == 2 and "cookies" not in fetcher.calls[0]


@pytest.mark.asyncio
async def test_a_refused_plain_fetch_sends_the_next_requests_straight_to_the_slot():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = _walled(fetcher, solve)
    await _first_request(router, fetcher, solve)

    fetcher.queue(_ok(), _ok())
    for _ in range(2):
        result = await router.fetch(URL, domain=DOMAIN)
        assert not result.blocked and result.routing_method == "httpcloak_cookies"
    # one fetch per request, each with the slot's clearance; no plain try before it
    assert len(fetcher.calls) == 4
    assert all(call.get("cookies") for call in fetcher.calls[2:])
    assert len(solve.calls) == 1


@pytest.mark.asyncio
async def test_after_the_interval_one_request_tries_the_plain_fetch_again():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = _walled(fetcher, solve)
    await _first_request(router, fetcher, solve)

    router._light_refused_at[DOMAIN] -= 61  # the interval is up
    fetcher.queue(_ok())
    result = await router.fetch(URL, domain=DOMAIN)
    assert result.routing_method == "httpcloak_light"
    assert "cookies" not in fetcher.calls[-1]
    # it passed: the domain is back on plain-first
    assert DOMAIN not in router._light_refused_at
    fetcher.queue(_ok())
    await router.fetch(URL, domain=DOMAIN)
    assert "cookies" not in fetcher.calls[-1]


@pytest.mark.asyncio
async def test_a_probe_that_is_refused_again_skips_for_another_interval():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = _walled(fetcher, solve)
    await _first_request(router, fetcher, solve)

    router._light_refused_at[DOMAIN] -= 61
    fetcher.queue(_blocked_403(), _ok())  # the probe is refused, the slot replays
    result = await router.fetch(URL, domain=DOMAIN)
    assert result.routing_method == "httpcloak_cookies"
    fetcher.queue(_ok())
    await router.fetch(URL, domain=DOMAIN)
    assert fetcher.calls[-1].get("cookies")  # skipped again
    assert len(fetcher.calls) == 5


@pytest.mark.asyncio
async def test_without_live_slots_the_plain_fetch_is_tried():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = _walled(fetcher, solve)
    router._light_refused_at[DOMAIN] = 0.0  # refused once, long ago... or just now: no slots either way
    fetcher.queue(_ok())
    result = await router.fetch(URL, domain=DOMAIN)
    assert result.routing_method == "httpcloak_light"


@pytest.mark.asyncio
async def test_interval_zero_always_tries_the_plain_fetch_first():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = _walled(fetcher, solve, interval=0)
    await _first_request(router, fetcher, solve)
    fetcher.queue(_blocked_403(), _ok())
    result = await router.fetch(URL, domain=DOMAIN)
    assert result.routing_method == "httpcloak_cookies"
    assert len(fetcher.calls) == 4 and "cookies" not in fetcher.calls[2]


@pytest.mark.asyncio
async def test_light_sessions_do_not_leak_into_the_cookie_replay():
    """The plain fetch's sticky session is its own: the replay goes out on the slot's session (it
    used to get both, a duplicate keyword that failed every replay over to the legacy path)."""
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = SpiderAutoRouter(fetcher=fetcher, cache=DomainCache(), solve_client=solve,
                              target_pool_size=0, warmup_replay_delay_s=0.01, light_sessions=2,
                              light_probe_interval=0)
    fetcher.queue(_blocked_403(), _ok())
    solve.queue(_solve_ok(proxy_session_id="sess-solved"))
    result = await router.fetch(URL, domain=DOMAIN)
    assert not result.blocked
    light, replay = fetcher.calls
    assert light["proxy_session_id"] in router._light_sessions
    assert replay["proxy_session_id"] == "sess-solved"
    # and on the cookie-replay step of a later request
    fetcher.queue(_blocked_403(), _ok())
    result = await router.fetch(URL, domain=DOMAIN)
    assert result.routing_method == "httpcloak_cookies"
    assert fetcher.calls[-1]["proxy_session_id"] == "sess-solved"
