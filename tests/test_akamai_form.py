"""Akamai Bot Manager in front of a form: the three ways apps.ilsos.gov's search meets a client
(2026-10-26), each handled on the slot's own exit.

* The form's POST with the page's unaccepted ``_abck`` gets no answer (the HTTP/2 stream reset in
  ~130 ms): a sensor solve on the form's page, then the POST replayed on that solve's exit.
* With an accepted ``_abck`` the POST meets Akamai's challenge action ("Challenge Validation", a
  recaptcha): the solve service answers it on the slot's session and its answer is the response.
* About seven minutes later the sensor's validation lapses (the site's 404, ``_abck`` reset): the
  sensor runs again on the same exit, its cookies merged into the slot (the site's session and
  ``sec_cpt`` kept), and the request is replayed.

Recorded shapes only: the challenge page and the site's pages are stand-ins, no token, no cookie.
"""
from __future__ import annotations

import json
from typing import Dict, List
from unittest import mock

import httpx
import pytest

from scrapy_calyprium._policy import RunPolicy
from scrapy_calyprium.routing.auto import (
    SpiderAutoRouter,
    _abck_reset,
    _merge_sensor,
    _sensor_page,
)
from scrapy_calyprium.routing.domain_cache import DomainCache
from scrapy_calyprium.routing.local_fetch import LocalFetchError, LocalFetchResult
from scrapy_calyprium.routing.solve_client import SolvedResponse, SolveClient, SolveResult

HOST = "apps.example.gov"
PAGE = f"https://{HOST}/entitysearch/"
FORM = f"https://{HOST}/entitysearch/entitysearch"
BODY = b"command=entitySearch&method=search&searchMethod=s&searchValue=ACME"
HEADERS = {"Referer": PAGE, "Content-Type": "application/x-www-form-urlencoded",
           "Cookie": "JSESSIONID=J0; _abck=P~-1~Q~-1~-1~-1; bm_sz=Z0"}

CHALLENGE = (b'<!DOCTYPE html><html><head><title>Challenge Validation</title></head><body>'
             b'<iframe title="Challenge Content" id="sec-cpt-if" provider="recaptcha" class="recaptcha" '
             b'data-key="SITEKEY" src="/a/b/c"></iframe>'
             b'<form id="chlge" name="sec_chlge_form" method="POST" '
             b'action="/entitysearch/entitysearch?akamai-challenge-resubmit=true">'
             b'<input type="hidden" name="sec_chlge_forward_wrap" value="W1">'
             b'<input type="hidden" name="sec_chlge_content_type_wrap" value="W2"></form></body></html>')
RESULTS = b"<html><title>Business Entity Search</title><table>" + b"<tr><td>ACME</td></tr>" * 400 + b"</table></html>"
NOT_FOUND = b"<html><title>Page Not Found!</title>" + b"x" * 3000 + b"</html>"


def _fetch(status: int, body: bytes, set_cookies=(), url: str = FORM) -> LocalFetchResult:
    return LocalFetchResult(url=url, final_url=url, status_code=status,
                            headers={"content-type": "text/html;charset=ISO-8859-1"}, body=body,
                            backend="httpcloak", set_cookies=list(set_cookies))


class FakeFetcher:
    """Answers the router's fetches from a queue (an exception is raised)."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls: List[Dict] = []

    async def fetch(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        out = self.responses.pop(0)
        if isinstance(out, BaseException):
            raise out
        return out


SENSOR_COOKIES = [{"name": "JSESSIONID", "value": "J1", "domain": HOST},
                  {"name": "_abck", "value": "A~0~B~-1~-1~-1", "domain": ".example.gov"},
                  {"name": "bm_sz", "value": "Z1", "domain": ".example.gov"},
                  {"name": "ak_bmsc", "value": "K1", "domain": ".example.gov"}]


class FakeSolve:
    """The solve service: an Akamai sensor solve on the page, the challenge action answered."""

    def __init__(self, backend="tessera", captcha_ok=True, rerun_cookies=None):
        self.calls: List[Dict] = []
        self.backend = backend
        self.captcha_ok = captcha_ok
        self.rerun_cookies = rerun_cookies

    async def solve(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs.get("challenge_type") == "akamai_captcha":
            if not self.captcha_ok:
                return SolveResult(success=False, cookies=[], user_agent="", proxy_session_id="", engine="",
                                   preset="", duration_ms=0, error="akamai_captcha: forwardWrapExpired")
            answered = [c for c in kwargs["cookies"] if c["name"] != "sec_cpt"] + [
                {"name": "sec_cpt", "value": "CPT2", "domain": ".example.gov", "path": "/"},
                {"name": "bm_sv", "value": "V", "domain": ".example.gov", "path": "/"}]
            return SolveResult(success=True, cookies=answered, user_agent="UA", proxy_session_id=kwargs["proxy_session_id"],
                               engine="tessera-native", preset="chrome-155-windows", duration_ms=70_000,
                               response=SolvedResponse(status=200, url=FORM + "?akamai-challenge-resubmit=true",
                                                       headers={"content-type": "text/html;charset=ISO-8859-1"},
                                                       body=RESULTS, set_cookies=["bm_sv=V; Path=/"]))
        n = sum(1 for c in self.calls if c.get("challenge_type") == "akamai")
        cookies = self.rerun_cookies if (n > 1 and self.rerun_cookies) else SENSOR_COOKIES
        return SolveResult(success=True, cookies=[dict(c) for c in cookies], user_agent="UA",
                           proxy_session_id=kwargs.get("proxy_session_id") or "s1", engine="native",
                           preset="chrome-155-windows", duration_ms=30_000, provider="native")

    async def report_ip_outcome(self, **kwargs):
        pass


def _router(fetcher, solve):
    return SpiderAutoRouter(fetcher=fetcher, cache=DomainCache(), solve_client=solve, proxy_url="http://u:p@gw:8080",
                            target_pool_size=1, refill_interval=0.0, solve_parallel_solves=1, cold_start_burst=1,
                            light_probe_interval=3600)


def _sent_cookies(call) -> Dict[str, str]:
    return {c["name"]: c["value"] for c in call.get("cookies") or []}


async def _post(router):
    return await router.fetch(FORM, domain=HOST, headers=dict(HEADERS), method="POST", body=BODY)


def test_helpers():
    assert _sensor_page(FORM, {"Referer": PAGE}) == PAGE
    assert _sensor_page(FORM, {"Referer": "https://www.other.gov/x"}) == f"https://{HOST}/"
    assert _sensor_page(FORM, {}) == f"https://{HOST}/"
    assert _abck_reset(["_abck=X~-1~Y~-1~-1~-1; Domain=.example.gov; Path=/; Secure"])
    assert not _abck_reset(["_abck=X~0~Y~-1~-1~-1; Path=/", "bm_sz=1; Path=/"])
    assert not _abck_reset(["_abck=; Max-Age=0; Path=/"])
    merged = _merge_sensor(
        [{"name": "JSESSIONID", "value": "J0"}, {"name": "_abck", "value": "old"}, {"name": "sec_cpt", "value": "C"}],
        [{"name": "_abck", "value": "new"}, {"name": "JSESSIONID", "value": "J9"}, {"name": "bm_sz", "value": "Z"}])
    assert {c["name"]: c["value"] for c in merged} == {"JSESSIONID": "J0", "_abck": "new", "sec_cpt": "C", "bm_sz": "Z"}


@pytest.mark.asyncio
async def test_a_reset_post_is_solved_as_akamai_on_its_page_and_replayed():
    fetcher = FakeFetcher(LocalFetchError("stream error: stream ID 3; INTERNAL_ERROR; received from peer"),
                          _fetch(200, RESULTS))
    solve = FakeSolve()
    result = await _post(_router(fetcher, solve))
    assert not result.blocked and result.fetch.body == RESULTS and result.routing_method == "solve_then_replay"
    (sensor,) = solve.calls
    assert sensor["challenge_type"] == "akamai" and sensor["target_url"] == PAGE
    replay = fetcher.calls[1]
    assert replay["method"] == "POST" and replay["body"] == BODY and replay["proxy_session_id"] == "s1"
    assert _sent_cookies(replay)["_abck"] == "A~0~B~-1~-1~-1"       # the slot's accepted cookie, not the jar's


@pytest.mark.asyncio
async def test_a_get_with_a_transport_error_still_backs_off():
    fetcher = FakeFetcher(LocalFetchError("timed out"))
    solve = FakeSolve()
    result = await _router(fetcher, solve).fetch(PAGE, domain=HOST, headers={"Cookie": "_abck=P~-1~Q"})
    assert result.routing_method == "transport_error" and solve.calls == []


@pytest.mark.asyncio
async def test_a_post_with_an_accepted_abck_is_not_sent_to_a_sensor_solve():
    fetcher = FakeFetcher(LocalFetchError("timed out"))
    solve = FakeSolve()
    headers = {**HEADERS, "Cookie": "_abck=A~0~B~-1~-1~-1"}
    result = await _router(fetcher, solve).fetch(FORM, domain=HOST, headers=headers, method="POST", body=BODY)
    assert result.routing_method == "transport_error" and solve.calls == []


@pytest.mark.asyncio
async def test_the_challenge_action_is_answered_on_the_slots_session():
    fetcher = FakeFetcher(LocalFetchError("stream reset"),
                          _fetch(200, CHALLENGE, ["sec_cpt=CPT1; Domain=.example.gov; Max-Age=7200; Path=/"]))
    solve = FakeSolve()
    router = _router(fetcher, solve)
    result = await _post(router)
    assert not result.blocked and result.routing_method == "captcha_solve"
    assert result.fetch.body == RESULTS and result.fetch.set_cookies == ["bm_sv=V; Path=/"]
    sensor, captcha = solve.calls
    assert captcha["challenge_type"] == "akamai_captcha" and captcha["proxy_session_id"] == "s1"
    assert captcha["page_html"].startswith("<!DOCTYPE html>") and captcha["target_url"] == FORM
    assert captcha["paid"] is True and captcha["deadline_ms"] >= 180_000 and captcha["user_agent"] == "UA"
    assert captcha["provider"] is None                                # Tessera: a provider would force a solver
    sent = _sent_cookies(captcha)
    assert sent["sec_cpt"] == "CPT1" and sent["_abck"] == "A~0~B~-1~-1~-1" and sent["JSESSIONID"] == "J1"
    slot = router.cache.get(HOST).live_slots()[0]
    assert {c["name"]: c["value"] for c in slot.cookies}["sec_cpt"] == "CPT2"

    # the next search replays the slot (its clearance and sec_cpt), no solve
    fetcher.responses.append(_fetch(200, RESULTS))
    result = await _post(router)
    assert not result.blocked and result.routing_method == "httpcloak_cookies" and len(solve.calls) == 2
    assert _sent_cookies(fetcher.calls[-1])["sec_cpt"] == "CPT2" and fetcher.calls[-1]["proxy_session_id"] == "s1"


@pytest.mark.asyncio
async def test_mimic_is_told_the_slots_provider():
    fetcher = FakeFetcher(LocalFetchError("stream reset"), _fetch(200, CHALLENGE, ["sec_cpt=CPT1; Path=/"]))
    solve = FakeSolve(backend="mimic")
    router = _router(fetcher, solve)
    router.cache = DomainCache()
    await _post(router)
    assert solve.calls[1]["provider"] == "native"                     # what the slot recorded


@pytest.mark.asyncio
async def test_an_unanswered_challenge_stands_and_is_not_bought_again():
    fetcher = FakeFetcher(LocalFetchError("stream reset"), _fetch(200, CHALLENGE, ["sec_cpt=CPT1; Path=/"]))
    solve = FakeSolve(captcha_ok=False)
    result = await _post(_router(fetcher, solve))
    assert result.blocked and result.diagnosis.evidence == "akamai_captcha"
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha"]


async def _cleared_router(fetcher, solve):
    router = _router(fetcher, solve)
    fetcher.responses[:0] = [LocalFetchError("stream reset"), _fetch(200, CHALLENGE, ["sec_cpt=CPT1; Path=/"])]
    result = await _post(router)
    assert result.routing_method == "captcha_solve"
    return router


@pytest.mark.asyncio
async def test_the_lapse_runs_the_sensor_again_on_the_same_exit_and_keeps_the_session():
    rerun = [{"name": "JSESSIONID", "value": "J9", "domain": HOST},
             {"name": "_abck", "value": "A2~0~B~-1~-1~-1", "domain": ".example.gov"},
             {"name": "bm_sz", "value": "Z2", "domain": ".example.gov"}]
    fetcher = FakeFetcher(
        _fetch(404, NOT_FOUND, ["_abck=R~-1~S~-1~-1~-1; Domain=.example.gov; Path=/"]),   # the lapse
        _fetch(200, RESULTS))                                                                # the replay
    solve = FakeSolve(rerun_cookies=rerun)
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert not result.blocked and result.fetch.body == RESULTS and result.routing_method == "solve_then_replay"
    rerun_call = solve.calls[-1]
    assert rerun_call["challenge_type"] == "akamai" and rerun_call["proxy_session_id"] == "s1"
    assert rerun_call["target_url"] == PAGE
    sent = _sent_cookies(fetcher.calls[-1])
    assert sent["_abck"] == "A2~0~B~-1~-1~-1" and sent["bm_sz"] == "Z2"        # the new sensor's
    assert sent["JSESSIONID"] == "J1" and sent["sec_cpt"] == "CPT2"              # the session's, kept
    assert fetcher.calls[-1]["proxy_session_id"] == "s1"


@pytest.mark.asyncio
async def test_after_the_lapse_the_challenge_again_is_answered_once(monkeypatch):
    """The lapse comes minutes after the answer (the quick-repeat guard does not apply)."""
    from scrapy_calyprium.routing import auto as auto_mod
    monkeypatch.setattr(auto_mod, "CAPTCHA_MIN_INTERVAL_S", 0.0)
    fetcher = FakeFetcher(
        _fetch(404, NOT_FOUND, ["_abck=R~-1~S~-1~-1~-1; Path=/"]),
        _fetch(200, CHALLENGE, ["sec_cpt=CPT3; Path=/"]))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert not result.blocked and result.routing_method == "captcha_solve"
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha", "akamai", "akamai_captcha"]


@pytest.mark.asyncio
async def test_the_challenge_right_after_its_answer_runs_the_sensor_instead_of_paying_again():
    fetcher = FakeFetcher(
        _fetch(200, CHALLENGE, ["sec_cpt=CPT3; Path=/"]),     # met again at once
        _fetch(200, RESULTS))                                  # the replay after the sensor ran again
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert not result.blocked and result.fetch.body == RESULTS
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha", "akamai"]


@pytest.mark.asyncio
async def test_the_challenge_that_stays_after_the_sensor_run_stands_unpaid():
    fetcher = FakeFetcher(
        _fetch(200, CHALLENGE, ["sec_cpt=CPT3; Path=/"]),
        _fetch(200, CHALLENGE, ["sec_cpt=CPT4; Path=/"]))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert result.blocked and result.diagnosis.evidence == "akamai_captcha"
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha", "akamai"]


@pytest.mark.asyncio
async def test_a_challenge_minutes_after_its_answer_is_answered_again(monkeypatch):
    """apps.ilsos.gov, 2026-10-26: about seven minutes after an answer the search met the challenge again."""
    from scrapy_calyprium.routing import auto as auto_mod
    fetcher = FakeFetcher(_fetch(200, CHALLENGE, ["sec_cpt=CPT3; Path=/"]))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    monkeypatch.setattr(auto_mod, "CAPTCHA_MIN_INTERVAL_S", 0.0)
    result = await _post(router)
    assert not result.blocked and result.routing_method == "captcha_solve"
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha", "akamai_captcha"]


@pytest.mark.asyncio
async def test_a_plain_404_with_the_sensor_accepted_is_the_answer():
    fetcher = FakeFetcher(_fetch(404, NOT_FOUND))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert result.fetch.status_code == 404 and len(solve.calls) == 2              # no sensor run


@pytest.mark.asyncio
async def test_an_answer_that_resets_the_sensor_stands_and_the_next_request_runs_it_first():
    """The site answered and reset _abck with it: the answer is kept (no duplicate request), and the slot's next
    request runs the sensor before it is sent (rather than meeting the site's 404)."""
    fetcher = FakeFetcher(
        _fetch(200, RESULTS, ["_abck=R~-1~S~-1~-1~-1; Path=/"]),   # answered, the sensor's validation lapsed with it
        _fetch(200, RESULTS))                                       # the next request, after the sensor ran again
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    first = await _post(router)
    assert first.fetch.body == RESULTS and first.routing_method == "httpcloak_cookies" and len(solve.calls) == 2
    second = await _post(router)
    assert second.fetch.body == RESULTS and [c["challenge_type"] for c in solve.calls] == [
        "akamai", "akamai_captcha", "akamai"]
    assert len(fetcher.calls) == 4 and _sent_cookies(fetcher.calls[-1])["_abck"] == "A~0~B~-1~-1~-1"


@pytest.mark.asyncio
async def test_the_solve_client_sends_the_session_and_reads_the_response():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={
            "success": True, "cookies": [{"name": "sec_cpt", "value": "C", "domain": ".example.gov"}],
            "user_agent": "UA", "proxy_session_id": "s1", "engine": "native", "preset": "chrome-155-windows",
            "response": {"status": 200, "url": FORM, "headers": {"content-type": "text/html"},
                         "body_b64": "PGh0bWw+", "set_cookies": ["bm_sv=V; Path=/"]}})

    client = SolveClient("http://mimic", policy=RunPolicy(spider_id="7", allow_paid_solve=True,
                                                         paid_solve_mode="token", country="us"))
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        out = await client.solve(domain=HOST, target_url=FORM, challenge_type="akamai_captcha",
                                 proxy_session_id="s1", page_html="<html>", user_agent="UA", paid=True,
                                 cookies=[{"name": "sec_cpt", "value": "C"}], country="us", deadline_ms=200_000)
    finally:
        await client.close()
    assert out.success and out.response.body == b"<html>" and out.response.set_cookies == ["bm_sv=V; Path=/"]
    assert seen["challenge_type"] == "akamai_captcha" and seen["page_html"] == "<html>"
    assert seen["cookies"] == [{"name": "sec_cpt", "value": "C", "domain": f".{HOST}", "path": "/"}]
    assert (seen["allow_paid_solve"], seen["paid_solve_mode"], seen["deadline_ms"]) == (True, "token", 200_000)
    assert seen["spider_id"] == "7" and seen["country"] == "us"


@pytest.mark.asyncio
async def test_an_ordinary_solve_sends_no_session_or_policy():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "cookies": []})

    client = SolveClient("http://mimic", policy=RunPolicy(allow_paid_solve=False))
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        out = await client.solve(domain=HOST, target_url=PAGE, challenge_type="akamai")
    finally:
        await client.close()
    assert out.response is None
    assert not {"page_html", "cookies", "allow_paid_solve", "paid_solve_mode", "deadline_ms"} & set(seen)
