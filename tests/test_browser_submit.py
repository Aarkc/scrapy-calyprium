"""Browser-made form submits: ``meta["mimic_browser_submit"]`` goes to Mimic's /api/browser/submit.

wyobiz.wyo.gov (F5 BIG-IP bot defence, 2026-10-26) refuses a search POST its own page script did not
make; Mimic submits it from a real browser page and the spider gets the page after the submit.
"""
from __future__ import annotations

from unittest import mock

import httpx
import pytest
from scrapy.http import FormRequest, HtmlResponse, Request

from scrapy_calyprium import _browser_submit as bsub
from scrapy_calyprium._policy import POLICY_SETTINGS
from scrapy_calyprium.middleware.mimic import MimicBrowserMiddleware
from tests._helpers import make_crawler

URL = "https://wyobiz.wyo.gov/Business/FilingSearch.aspx"
P = "ctl00$MainContent$"
RESULTS = ('<html><body><form><span id="MainContent_lblResultsHeader">Results: 1-20 of 5003</span>'
           '<ol class="search-results"><li><a href="FilingDetails.aspx?eFNum=1">ABC LLC</a></li></ol>'
           "</form>" + "<p>business search</p>" * 50 + "</body></html>")
CAPTCHA = ('<html><head><script>window["bobcmn"] = "1";</script></head><body>This question is for testing whether '
           "you are a human visitor and to prevent automated spam submission. What code is in the image?</body></html>")
SETTINGS = {"MIMIC_SERVICE_URL": "http://mimic", "CALYPRIUM_API_KEY": "clp_k", "SPIDER_ID": "42", "RUN_ID": "7",
            "VEIL_COUNTRY": "us", "MIMIC_SUBMIT_PROXY_TYPE": "residential", "MIMIC_ALLOWED_ENGINES": "camoufox"}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in POLICY_SETTINGS:
        monkeypatch.delenv(key, raising=False)


class _Spider:
    name = "us_wy_companies"


def _mw(settings=None):
    mw = MimicBrowserMiddleware.from_crawler(make_crawler({**SETTINGS, **(settings or {})}))
    return mw


def _reply(status=200, json_body=None, headers=None):
    req = httpx.Request("POST", "http://mimic/api/browser/submit")
    return httpx.Response(status, json=json_body or {}, headers=headers, request=req)


def _ok(html=RESULTS, diagnosis=None, **kw):
    body = {"url": URL, "final_url": URL, "status_code": 200, "headers": {"content-type": "text/html; charset=utf-8"},
            "html": html, "cookies": [{"name": "TS01ab71c3", "value": "secret", "domain": "wyobiz.wyo.gov"}],
            "browser_engine": "camoufox", "session_key": "us_wy_companies:7", "session_id": "s1", "new_session": True,
            "session_submits": 1, "navigated": True, "trigger": "click", "outcome": "async", "session_closed": False,
            "fetch_time_ms": 9000,
            "routing": {"routing_method": "browser_submit", "engine": "camoufox", "network": "residential",
                        "bytes": len(html), "blocked": False,
                        "diagnosis": diagnosis or {"class": "ok", "number": 0, "side": "none", "move": "none"}}}
    body.update(kw)
    return body


def _search(prefix="ab", **meta):
    data = {"__VIEWSTATE": "stale", "__EVENTTARGET": "", P + "txtFilingName": prefix,
            P + "searchOpt": "chkSearchStartWith", P + "cmdSearch": " Search"}
    return FormRequest(URL, formdata=data, meta={"mimic_browser_submit": {"submit": P + "cmdSearch", **meta}})


async def _call(mw, request, reply):
    client = mock.AsyncMock()
    client.is_closed = False
    if isinstance(reply, Exception):
        client.post.side_effect = reply
    else:
        client.post.return_value = reply
    mw._client = client
    response = await mw.process_request(request, _Spider())
    return response, client


class TestPayload:
    def test_a_form_request_posts_its_fields_and_the_runs_policy(self):
        mw = _mw()
        body = bsub.payload(_search(), {"submit": P + "cmdSearch"}, mw.crawler.settings, mw.policy, "k")
        assert body["url"] == URL and body["navigate"] == "auto" and body["submit"] == P + "cmdSearch"
        assert body["fields"][P + "txtFilingName"] == "ab" and body["fields"]["__VIEWSTATE"] == "stale"
        assert body["proxy_type"] == "residential" and body["proxy_country"] == "us"
        assert body["browser_engine"] == "camoufox" and body["spider_id"] == "42" and body["run_id"] == "7"
        assert body["allowed_engines"] == ["camoufox"] and body["return_cookies"] is False
        assert body["timeout_ms"] == 120000

    def test_a_follow_up_names_its_target_and_no_fields(self):
        mw = _mw()
        req = Request(URL, meta={"mimic_browser_submit": {"submit": P + "lbtnNextHeader", "navigate": "never",
                                                          "timeout": 600, "max_submits": 30}})
        body = bsub.payload(req, bsub.options(req), mw.crawler.settings, mw.policy, "k")
        assert body["fields"] == {} and body["navigate"] == "never" and body["max_submits"] == 30
        assert body["timeout_ms"] == 240000                       # Mimic's ceiling

    def test_the_engine_stays_inside_the_policy(self):
        mw = _mw({"MIMIC_ALLOWED_ENGINES": "nodriver"})
        body = bsub.payload(_search(), {}, mw.crawler.settings, mw.policy, "k")
        assert body["browser_engine"] == "nodriver"


@pytest.mark.asyncio
class TestMiddleware:
    async def test_the_page_after_the_submit_comes_back_as_a_normal_response(self):
        mw = _mw()
        req = _search()
        response, client = await _call(mw, req, _reply(json_body=_ok()))
        url = client.post.call_args.args[0]
        sent = client.post.call_args.kwargs["json"]
        assert url == "http://mimic/api/browser/submit" and sent["session_key"] == "us_wy_companies:7"
        assert isinstance(response, HtmlResponse) and response.status == 200
        assert response.css("#MainContent_lblResultsHeader::text").get() == "Results: 1-20 of 5003"
        assert req.meta["calyprium_diagnosis"]["class"] == "ok"
        assert req.meta["calyprium_routing_method"] == "browser_submit"
        assert req.meta["calyprium_routing"]["routing_method"] == "browser_submit"
        assert req.meta["mimic_submit"]["outcome"] == "async" and req.meta["mimic_submit"]["session_submits"] == 1
        assert "mimic_submit_cookies" not in req.meta and b"Set-Cookie" not in response.headers
        assert mw._submit_keys == {"us_wy_companies:7"} and req.meta["dont_retry"] is True

    async def test_cookies_only_when_asked(self):
        mw = _mw()
        req = _search(return_cookies=True)
        await _call(mw, req, _reply(json_body=_ok()))
        assert [c["name"] for c in req.meta["mimic_submit_cookies"]] == ["TS01ab71c3"]

    async def test_a_captcha_comes_back_as_it_is_and_is_not_retried(self):
        mw = _mw()
        req = _search()
        diag = {"class": "captcha", "number": 10, "side": "site", "move": "climb", "vendor": "f5",
                "evidence": "f5_captcha"}
        response, client = await _call(mw, req, _reply(json_body=_ok(
            html=CAPTCHA, diagnosis=diag, outcome="blocked", session_closed=True)))
        assert req.meta["calyprium_diagnosis"]["evidence"] == "f5_captcha"
        assert b"human visitor" in response.body
        assert await mw.process_response(req, response, _Spider()) is response   # fail closed: no retry
        assert client.post.call_count == 1

    async def test_a_lost_page_is_a_409_response_with_no_body(self):
        mw = _mw()
        req = Request(URL, meta={"mimic_browser_submit": {"submit": P + "lbtnNextHeader", "navigate": "never"}})
        response, _ = await _call(mw, req, _reply(409, {"detail": {"error": "state_lost", "detail": "no page"}}))
        assert response.status == 409 and response.body == b""
        assert req.meta["mimic_submit"] == {"error": "state_lost", "status": 409}
        assert await mw.process_response(req, response, _Spider()) is response

    async def test_a_cooled_down_key_keeps_its_retry_after(self):
        mw = _mw()
        response, _ = await _call(mw, _search(), _reply(429, {"detail": {"error": "captcha_cooldown"}},
                                                        headers={"Retry-After": "600"}))
        assert response.status == 429 and response.headers.get("Retry-After") == b"600"

    async def test_no_answer_from_mimic_is_a_504_and_never_re_sent(self):
        mw = _mw()
        req = _search()
        response, client = await _call(mw, req, httpx.ReadTimeout("slow"))
        assert response.status == 504 and client.post.call_count == 1
        assert req.meta["calyprium_diagnosis"]["class"]
        assert await mw.process_exception(req, httpx.ReadTimeout("slow"), _Spider()) is None

    async def test_other_requests_are_untouched(self):
        mw = _mw()
        client = mock.AsyncMock()
        mw._client = client
        assert await mw.process_request(Request("https://wyobiz.wyo.gov/Business/FilingDetails.aspx?eFNum=1"),
                                        _Spider()) is None
        client.post.assert_not_called()

    async def test_the_runs_pages_are_released_when_the_spider_closes(self):
        mw = _mw()
        _, client = await _call(mw, _search(), _reply(json_body=_ok()))
        client.post.return_value = _reply(json_body={"released": True})
        await mw.spider_closed(_Spider())
        assert client.post.call_args_list[-1].args[0] == "http://mimic/api/browser/submit/release"
        assert client.post.call_args_list[-1].kwargs["json"] == {"session_key": "us_wy_companies:7"}
