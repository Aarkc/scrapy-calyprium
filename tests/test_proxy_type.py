"""VEIL_PROXY_TYPE names a supply class the gateway routes by (datacenter, isp, residential,
mobile): each reaches the gateway as its -type_ parameter, so a policy's proxy type picks the
class and the gateway picks the source within it (prepaid first)."""
from __future__ import annotations

import base64

import pytest
from scrapy.http import Request

from scrapy_calyprium._policy import POLICY_SETTINGS
from scrapy_calyprium.middleware.veil import VeilProxyMiddleware
from tests._helpers import make_crawler

BASE = {"CALYPRIUM_API_KEY": "clp_k", "VEIL_GATEWAY_URL": "http://gw:8080", "VEIL_USER_ID": "user-1"}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for key in POLICY_SETTINGS:
        monkeypatch.delenv(key, raising=False)


def _username(settings: dict) -> str:
    veil = VeilProxyMiddleware.from_crawler(make_crawler({**BASE, **settings}))
    req = Request("https://example.com")
    veil.process_request(req, None)
    return base64.b64decode(req.headers["Proxy-Authorization"][6:]).decode().split(":", 1)[0]


@pytest.mark.parametrize("proxy_type,param", [
    ("datacenter", "type_dc"),
    ("isp", "type_isp"),
    ("residential", "type_res"),
    ("mobile", "type_mobile"),
])
def test_each_class_reaches_the_gateway(proxy_type, param):
    assert param in _username({"VEIL_PROXY_TYPE": proxy_type}).split("-")


def test_no_proxy_type_sends_none():
    assert not any(p.startswith("type_") for p in _username({}).split("-"))
