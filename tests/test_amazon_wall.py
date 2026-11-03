"""Amazon's own wall (a 200 whose form posts to /errors/validateCaptcha): AWS WAF's, and a verdict on the exit.

2026-11-03: two in three fresh residential exits were served Amazon's product pages without any token, and on a
flagged one a native solve bought a page or two before the wall came back. So the first time a request meets the
wall it goes again from another exit; met again, it is solved, the solve naming aws_waf (Tessera's native solver).
"""
from __future__ import annotations

import pytest

from scrapy_calyprium import _outcome
from scrapy_calyprium.routing import diagnosis as dg
from scrapy_calyprium.routing.block_detect import amazon_wall, challenge_family
from scrapy_calyprium.routing.local_fetch import LocalFetchResult

from tests.test_local_routing import FakeFetcher, FakeSolveClient, _make_router, _ok, _solve_ok

URL = "https://www.amazon.com/dp/B0TEST0000"
DOMAIN = "www.amazon.com"
WALL = ('<html><head><title>Amazon.com</title></head><body><h4>Click the button below to continue shopping</h4>'
        '<form method="get" action="/errors/validateCaptcha" name=""><input type=hidden name="amzn" value="x">'
        '<button type="submit">Continue shopping</button></form></body></html>')


def _wall(form="/errors/validateCaptcha"):
    return LocalFetchResult(url=URL, final_url=URL, status_code=200, headers={"content-type": "text/html"},
                            body=WALL.replace("/errors/validateCaptcha", form).encode(), backend="httpcloak")


async def _fetch(router, attempt):
    token = _outcome.CURRENT_ATTEMPT.set(attempt)
    try:
        return await router.fetch(URL, domain=DOMAIN)
    finally:
        _outcome.CURRENT_ATTEMPT.reset(token)


@pytest.mark.parametrize("form", ["/errors/validateCaptcha", "/errors_page/validateCaptcha"])
def test_amazons_wall_is_aws_wafs(form):
    body = WALL.replace("/errors/validateCaptcha", form)
    assert amazon_wall(200, body) and challenge_family(200, body) == "aws_waf"
    assert not amazon_wall(200, body + "<p>x</p>" * 5000)  # a whole page that happens to carry the form
    assert challenge_family(200, body + "<p>x</p>" * 5000) is None
    assert not amazon_wall(503, body)


@pytest.mark.asyncio
async def test_the_first_time_a_request_meets_it_another_exit_not_a_solve():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = _make_router(fetcher=fetcher, solve=solve)
    fetcher.queue(_wall())

    result = await _fetch(router, 0)

    assert result.blocked and not solve.calls
    assert (result.diagnosis.cls, result.diagnosis.vendor) == (dg.EDGE_DENY, dg.AWS_WAF)
    assert result.diagnosis.info.move == dg.CLIMB  # the middleware sends it again at once


@pytest.mark.asyncio
async def test_met_again_it_is_solved_naming_aws_waf():
    fetcher, solve = FakeFetcher(), FakeSolveClient()
    router = _make_router(fetcher=fetcher, solve=solve)
    fetcher.queue(_wall(), _ok())
    solve.queue(_solve_ok())

    result = await _fetch(router, 1)

    assert not result.blocked and result.routing_method == "solve_then_replay"
    assert solve.calls[0]["challenge_type"] == "aws_waf"
