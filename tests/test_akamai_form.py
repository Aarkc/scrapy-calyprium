"""Akamai Bot Manager in front of a form: the three ways apps.ilsos.gov's search meets a client
(2026-10-26), each handled on the slot's own exit.

* The form's POST with the page's unaccepted ``_abck`` gets no answer (the HTTP/2 stream reset in
  ~130 ms): a sensor solve on the form's page, then the POST replayed on that solve's exit.
* With an accepted ``_abck`` the POST meets Akamai's challenge action ("Challenge Validation", a
  recaptcha): the solve service answers it on the slot's session and its answer is the response.
* An answer opens three requests (its resubmit and two more); the fourth meets the challenge
  again, answered again (2026-10-27, from home and on the platform).
* The clearance lapses (the site's 404 with ``_abck`` reset, or the POST held again): the slot is
  retired and the request starts over (a sensor merged beside the old ``sec_cpt`` was held).

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
                           proxy_session_id=kwargs.get("proxy_session_id") or f"s{n}", engine="native",
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
    assert _sensor_page(FORM, {"Referer": "https://www.other.gov/x"}) == f"https://{HOST}/entitysearch/"
    assert _sensor_page(FORM, {}) == f"https://{HOST}/entitysearch/"
    assert _abck_reset(["_abck=X~-1~Y~-1~-1~-1; Domain=.example.gov; Path=/; Secure"])
    assert not _abck_reset(["_abck=X~0~Y~-1~-1~-1; Path=/", "bm_sz=1; Path=/"])
    assert not _abck_reset(["_abck=; Max-Age=0; Path=/"])


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


RERUN = [{"name": "JSESSIONID", "value": "J9", "domain": HOST},
         {"name": "_abck", "value": "A2~0~B~-1~-1~-1", "domain": ".example.gov"},
         {"name": "bm_sz", "value": "Z2", "domain": ".example.gov"}]


@pytest.mark.asyncio
async def test_the_lapse_retires_the_slot_and_starts_the_request_over():
    """A fresh sensor merged beside the old sec_cpt was held by the site (canary 2026-10-27): the lapsed slot is
    retired and the request starts over on a fresh solve, none of the old session's cookies carried."""
    fetcher = FakeFetcher(
        _fetch(404, NOT_FOUND, ["_abck=R~-1~S~-1~-1~-1; Domain=.example.gov; Path=/"]),   # the lapse
        _fetch(200, RESULTS))                                                                # the fresh replay
    solve = FakeSolve(rerun_cookies=RERUN)
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert not result.blocked and result.fetch.body == RESULTS and result.routing_method == "solve_then_replay"
    fresh = solve.calls[-1]
    assert fresh["challenge_type"] == "akamai" and fresh["target_url"] == PAGE and "proxy_session_id" not in fresh
    assert fresh["extra"] == {"abck": "accepted"}
    sent = _sent_cookies(fetcher.calls[-1])
    assert sent["_abck"] == "A2~0~B~-1~-1~-1" and sent["JSESSIONID"] == "J9" and "sec_cpt" not in sent
    assert fetcher.calls[-1]["proxy_session_id"] == "s2"                     # the fresh solve's exit
    assert [s.slot_id for s in router.cache.get(HOST).live_slots()] != []


@pytest.mark.asyncio
async def test_after_the_lapse_the_fresh_start_meets_the_challenge_and_answers_it():
    fetcher = FakeFetcher(
        _fetch(404, NOT_FOUND, ["_abck=R~-1~S~-1~-1~-1; Path=/"]),
        _fetch(200, CHALLENGE, ["sec_cpt=CPT3; Path=/"]))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert not result.blocked and result.routing_method == "captcha_solve"
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha", "akamai", "akamai_captcha"]


@pytest.mark.asyncio
async def test_the_fourth_request_after_an_answer_meets_the_challenge_and_it_is_answered_again():
    """apps.ilsos.gov 2026-10-27, from home at 1-6 s spacing and on the platform: the answer's resubmit and two more
    searches answered, the fourth met the challenge (with _abck reset). It is answered again."""
    fetcher = FakeFetcher(_fetch(200, RESULTS), _fetch(200, RESULTS),
                          _fetch(200, CHALLENGE, ["_abck=R~-1~S~-1~-1~-1; Path=/", "sec_cpt=CPT3; Path=/"]))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    for _ in range(2):
        assert (await _post(router)).fetch.body == RESULTS
    result = await _post(router)
    assert not result.blocked and result.routing_method == "captcha_solve" and result.fetch.body == RESULTS
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha", "akamai_captcha"]
    assert router._since_answer[result.slot_id] == 1


@pytest.mark.asyncio
async def test_a_challenge_right_after_its_answer_is_not_bought_again_and_the_slot_retired():
    """The answer opened only its own resubmit: answering again would buy a token per request."""
    fetcher = FakeFetcher(_fetch(200, CHALLENGE, ["sec_cpt=CPT3; Path=/"]))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert result.blocked and result.diagnosis.evidence == "akamai_captcha"
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha"]
    entry = router.cache.get(HOST)
    assert entry is None or entry.live_slots() == []


@pytest.mark.asyncio
async def test_a_held_form_request_on_a_slot_starts_the_request_over():
    """Canary 2026-10-27: after the challenge, every POST on the slot was held, and the slot (an infra failure
    never retires one) was replayed and held for the rest of the run. Now it is retired and the request starts
    over."""
    fetcher = FakeFetcher(LocalFetchError("roundtrip apps.example.gov [h1]: context deadline exceeded"),
                          _fetch(200, RESULTS))
    solve = FakeSolve(rerun_cookies=RERUN)
    router = await _cleared_router(fetcher, solve)
    held_slot = router.cache.get(HOST).live_slots()[0].slot_id
    result = await _post(router)
    assert not result.blocked and result.fetch.body == RESULTS
    assert [c["challenge_type"] for c in solve.calls] == ["akamai", "akamai_captcha", "akamai"]
    assert held_slot not in [s.slot_id for s in router.cache.get(HOST).live_slots()]


@pytest.mark.asyncio
async def test_a_plain_404_with_the_sensor_accepted_is_the_answer():
    fetcher = FakeFetcher(_fetch(404, NOT_FOUND))
    solve = FakeSolve()
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert result.fetch.status_code == 404 and len(solve.calls) == 2              # no sensor run


@pytest.mark.asyncio
async def test_an_answer_that_resets_the_sensor_stands_and_the_next_request_starts_over():
    """The site answered and reset _abck with it: the answer is kept (no duplicate request), and the slot's next
    request starts over (rather than meeting the site's 404)."""
    fetcher = FakeFetcher(
        _fetch(200, RESULTS, ["_abck=R~-1~S~-1~-1~-1; Path=/"]),   # answered, the sensor's validation lapsed with it
        _fetch(200, RESULTS))                                       # the next request, on a fresh solve
    solve = FakeSolve(rerun_cookies=RERUN)
    router = await _cleared_router(fetcher, solve)
    first = await _post(router)
    assert first.fetch.body == RESULTS and first.routing_method == "httpcloak_cookies" and len(solve.calls) == 2
    second = await _post(router)
    assert second.fetch.body == RESULTS and [c["challenge_type"] for c in solve.calls] == [
        "akamai", "akamai_captcha", "akamai"]
    assert len(fetcher.calls) == 4 and _sent_cookies(fetcher.calls[-1])["_abck"] == "A2~0~B~-1~-1~-1"


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


# ---- the form's page, whoever named it (us_il_companies' canary, 2026-10-27) ----------------------------------------
# The spider's Referer equalled the one Scrapy's RefererMiddleware gives, so the middleware dropped it as Scrapy's
# (_headers.scrapy_referer) and the sensor solve went to the homepage, which redirects to www.ilsos.gov: a page with no
# sensor, "solved" with an unaccepted _abck; every replayed POST was then held until the client gave up.

def test_the_sensor_page_is_the_forms_page_whoever_named_it():
    assert _sensor_page(FORM, {}, referer=PAGE) == PAGE                         # Scrapy's Referer
    assert _sensor_page(FORM, {"Referer": PAGE}, referer="https://x.gov/") == PAGE
    assert _sensor_page(FORM, {}) == f"https://{HOST}/entitysearch/"            # the form's directory
    # the form's own address (Scrapy's Referer for a search yielded from a results page) is no page
    assert _sensor_page(FORM, {}, referer=FORM + "?akamai-challenge-resubmit=true") == f"https://{HOST}/entitysearch/"
    assert _sensor_page(f"https://{HOST}/search", {}) == f"https://{HOST}/"     # nothing better: the homepage


@pytest.mark.asyncio
async def test_a_post_without_its_referer_header_solves_on_the_page_it_came_from():
    fetcher = FakeFetcher(LocalFetchError("context deadline exceeded"), _fetch(200, RESULTS))
    solve = FakeSolve()
    headers = {k: v for k, v in HEADERS.items() if k != "Referer"}          # dropped as Scrapy's
    result = await _router(fetcher, solve).fetch(FORM, domain=HOST, headers=headers, method="POST", body=BODY,
                                                 referer=PAGE)
    assert not result.blocked and solve.calls[0]["target_url"] == PAGE
    assert solve.calls[0]["extra"] == {"abck": "accepted"}                    # a served page is no solve here


@pytest.mark.asyncio
async def test_a_sensor_solve_with_no_accepted_abck_is_not_replayed():
    fetcher = FakeFetcher(LocalFetchError("context deadline exceeded"))
    unaccepted = [{"name": "_abck", "value": "A~-1~B~-1~-1~-1", "domain": ".example.gov"},
                  {"name": "bm_sz", "value": "Z", "domain": ".example.gov"}]
    solve = FakeSolve()
    solve.rerun_cookies = None

    async def served_page(**kwargs):
        solve.calls.append(kwargs)
        return SolveResult(success=True, cookies=[dict(c) for c in unaccepted], user_agent="UA",
                           proxy_session_id="s1", engine="native", preset="chrome-155-windows", duration_ms=7000)
    solve.solve = served_page
    router = _router(fetcher, solve)
    result = await _post(router)
    assert result.blocked and "no accepted _abck" in result.error
    assert len(fetcher.calls) == 1                                           # the form is not replayed (and held)
    entry = router.cache.get(HOST)
    assert (entry is None or entry.live_slots() == []) and HOST not in router._solve_pages


@pytest.mark.asyncio
async def test_a_fresh_start_with_no_accepted_sensor_is_not_replayed():
    fetcher = FakeFetcher(_fetch(404, NOT_FOUND, ["_abck=R~-1~S~-1~-1~-1; Path=/"]))
    solve = FakeSolve(rerun_cookies=[{"name": "_abck", "value": "X~-1~Y~-1~-1~-1", "domain": ".example.gov"}])
    router = await _cleared_router(fetcher, solve)
    result = await _post(router)
    assert result.blocked and "no accepted _abck" in result.error
    assert len(fetcher.calls) == 3                                           # the lapsed 404, nothing replayed
    assert solve.calls[-1]["challenge_type"] == "akamai" and solve.calls[-1]["extra"] == {"abck": "accepted"}


@pytest.mark.asyncio
async def test_the_middleware_hands_the_router_the_referer_it_does_not_send():
    from scrapy.http import Request

    from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
    from scrapy_calyprium.routing.auto import RouteResult

    seen = {}

    class Router:
        cache = None

        async def fetch(self, url, **kwargs):
            seen.update(kwargs)
            return RouteResult(fetch=_fetch(200, RESULTS), routing_method="httpcloak_light", blocked=False,
                               domain_level="light")

    mimic = MimicBrowserMiddleware(service_url="http://mimic", api_key="clp_k")
    mimic._local_enabled, mimic._local_router = True, Router()
    request = Request(FORM, method="POST", body=BODY, headers={"Referer": PAGE,
                                                                 "Content-Type": "application/x-www-form-urlencoded"})
    request.meta["calyprium_scrapy_referer"] = PAGE                          # what Scrapy's RefererMiddleware gave
    await mimic.process_request(request, None)
    assert seen["referer"] == PAGE and "Referer" not in seen["headers"]      # used for the solve, not sent


@pytest.mark.asyncio
async def test_the_solve_client_sends_extra():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content))
        return httpx.Response(200, json={"success": True, "cookies": []})

    client = SolveClient("http://tessera")
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        await client.solve(domain=HOST, target_url=PAGE, challenge_type="akamai", extra={"abck": "accepted"})
    finally:
        await client.close()
    assert seen["extra"] == {"abck": "accepted"}


@pytest.mark.asyncio
async def test_the_answer_reaches_the_spider_decoded_whatever_encoding_the_solver_names():
    """Tessera 1.175.0 handed on the resubmit's "Content-Encoding: gzip" with its decoded body: Scrapy's
    HttpCompressionMiddleware failed on it (BadGzipFile) and the spider searched again (2026-10-27)."""
    from scrapy.downloadermiddlewares.httpcompression import HttpCompressionMiddleware
    from scrapy.http import HtmlResponse, Request

    fetcher = FakeFetcher(LocalFetchError("stream reset"), _fetch(200, CHALLENGE, ["sec_cpt=CPT1; Path=/"]))
    solve = FakeSolve()
    real = solve.solve

    async def gzip_named(**kwargs):
        out = await real(**kwargs)
        if out.response is not None:
            out.response.headers.update({"Content-Encoding": "gzip", "Content-Length": "999"})
        return out
    solve.solve = gzip_named
    result = await _post(_router(fetcher, solve))
    assert result.routing_method == "captcha_solve"
    assert not {"content-encoding", "content-length"} & {k.lower() for k in result.fetch.headers}
    request = Request(FORM, method="POST")
    response = HtmlResponse(FORM, status=200, headers=result.fetch.headers, body=result.fetch.body, request=request)
    decoded = HttpCompressionMiddleware().process_response(request, response, None)
    assert decoded.body == RESULTS


@pytest.mark.asyncio
async def test_the_slots_delay_starts_again_after_a_solve():
    """Canary 2026-10-27: a search took 76 s inside its download (the token), and the next one went 0.1 s after
    the captcha's resubmit, the slot's delay having been counted from the first one's start."""
    import time as _time
    from types import SimpleNamespace

    from scrapy.http import Request

    from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
    from scrapy_calyprium.routing.auto import RouteResult

    slot = SimpleNamespace(lastseen=_time.time() - 80.0)

    class Router:
        cache = None

        def __init__(self, method):
            self.method = method

        async def fetch(self, url, **kwargs):
            return RouteResult(fetch=_fetch(200, RESULTS), routing_method=self.method, blocked=False,
                               domain_level="cookies")

    for method, restarted in (("captcha_solve", True), ("httpcloak_cookies", False)):
        slot.lastseen = _time.time() - 80.0
        mimic = MimicBrowserMiddleware(service_url="http://mimic", api_key="clp_k")
        mimic._local_enabled, mimic._local_router = True, Router(method)
        mimic.crawler = SimpleNamespace(engine=SimpleNamespace(downloader=SimpleNamespace(slots={HOST: slot})),
                                        settings=SimpleNamespace(getbool=lambda k, d=None: False))
        request = Request(FORM, method="POST", body=BODY, meta={"download_slot": HOST})
        await mimic.process_request(request, None)
        assert (_time.time() - slot.lastseen < 5) is restarted, method
