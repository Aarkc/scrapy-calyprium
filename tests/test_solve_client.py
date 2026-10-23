"""SolveClient waits as long as Mimic may solve, and says why a call failed.

Mimic's /api/solve runs fresh-IP browser attempts until its SOLVE_DEADLINE_S
(240 s). The client gave up at a fixed 90 s: spiders logged
"SolveClient: transport error for <domain>:" (an httpx read timeout has no
message) every ~90 s and retried, while Mimic kept solving the abandoned calls
on its browser pool.
"""
from __future__ import annotations

import logging
from unittest import mock

import httpx
import pytest

from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from scrapy_calyprium.routing.solve_client import (
    DEFAULT_SOLVE_DEADLINE_S,
    SolveClient,
    SolveError,
    solve_timeout_from_settings,
)
from tests._helpers import make_crawler

MIMIC_SOLVE_DEADLINE_S = 240  # stealth/mimic/mimic/config.py SOLVE_DEADLINE_S


def test_default_timeout_outlasts_mimics_solve_deadline():
    assert DEFAULT_SOLVE_DEADLINE_S == MIMIC_SOLVE_DEADLINE_S
    assert SolveClient("http://mimic").timeout > MIMIC_SOLVE_DEADLINE_S


@pytest.mark.asyncio
async def test_http_client_reads_for_the_whole_solve():
    client = SolveClient("http://mimic")
    http = await client._get_client()
    try:
        assert http.timeout.read > MIMIC_SOLVE_DEADLINE_S
        # an unreachable service is not solving: connecting still fails fast
        assert http.timeout.connect <= 15
    finally:
        await client.close()


@pytest.mark.parametrize("settings,expected", [
    ({}, MIMIC_SOLVE_DEADLINE_S + 30),
    ({"MIMIC_SOLVE_DEADLINE_S": 300}, 330),
    ({"MIMIC_SOLVE_DEADLINE_S": "120"}, 150),
    ({"MIMIC_SOLVE_DEADLINE_S": 0}, MIMIC_SOLVE_DEADLINE_S + 30),
])
def test_solve_timeout_follows_the_deadline_setting(settings, expected):
    assert solve_timeout_from_settings(make_crawler(settings).settings) == expected


@pytest.mark.parametrize("settings,expected", [
    ({}, MIMIC_SOLVE_DEADLINE_S + 30),
    ({"MIMIC_SOLVE_DEADLINE_S": 300}, 330),
])
def test_local_router_solve_client_uses_the_deadline(settings, expected):
    mw = MimicBrowserMiddleware.from_crawler(make_crawler({
        "MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "clp_k",
        "MIMIC_LOCAL_FETCH": True, "MIMIC_SLOT_STATS_INTERVAL": 0, **settings,
    }))
    mw._local_enabled = True
    mw._init_local_router(spider=None)
    assert mw._local_router is not None
    assert mw._solve_client.timeout == expected


@pytest.mark.asyncio
async def test_timeout_without_a_message_is_named(caplog):
    client = SolveClient("http://mimic")
    fake = mock.AsyncMock()
    fake.post.side_effect = httpx.ReadTimeout("")
    client._client = fake
    with caplog.at_level(logging.WARNING, logger="scrapy_calyprium.routing.solve_client"):
        with pytest.raises(SolveError) as excinfo:
            await client.solve(domain="www.example.com")
    assert "ReadTimeout" in str(excinfo.value)
    assert "ReadTimeout" in caplog.text
    assert "www.example.com" in caplog.text


@pytest.mark.asyncio
async def test_transport_error_keeps_its_message():
    client = SolveClient("http://mimic")
    fake = mock.AsyncMock()
    fake.post.side_effect = httpx.ConnectError("connection refused")
    client._client = fake
    with pytest.raises(SolveError) as excinfo:
        await client.solve(domain="www.example.com")
    assert "ConnectError: connection refused" in str(excinfo.value)
