"""Forge auth: spider-facing calls use the run's own key (AAR-47 / AAR-32).

Before: every Forge call sent ``X-Service-Secret: FORGE_SERVICE_SECRET``, which
forge only injected on recrawl runs, so stats/traces/checkpoints 401'd on every
other run, and the recrawl pipeline sent the *API key* in ``X-Service-Secret``.
"""
from __future__ import annotations

from unittest import mock

import pytest
from scrapy.exceptions import NotConfigured

from tests._helpers import make_crawler, make_spider

from scrapy_calyprium._forge import ForgeAuth
from scrapy_calyprium._veil import build_local_proxy_url, resolve_local_proxy_url


def _resp(status=200, body=None):
    r = mock.Mock()
    r.status_code = status
    r.json = mock.Mock(return_value=body or {})
    r.raise_for_status = mock.Mock()
    r.text = ""
    return r


class TestForgeAuth:
    def test_prefers_bearer_api_key(self):
        auth = ForgeAuth(api_key="clp_k", service_secret="master", user_id="u1")
        assert auth.headers() == {"Authorization": "Bearer clp_k"}

    def test_secret_only_without_api_key(self):
        auth = ForgeAuth(service_secret="master", user_id="u1")
        assert auth.headers() == {"X-Service-Secret": "master", "X-User-Id": "u1"}

    def test_no_credentials(self):
        auth = ForgeAuth()
        assert not auth
        assert auth.headers() == {}

    def test_call_falls_back_to_secret_on_401(self):
        auth = ForgeAuth(api_key="clp_k", service_secret="master", user_id="u1")
        seen = []

        def send(h):
            seen.append(h)
            return _resp(401 if "Authorization" in h else 200)

        assert auth.call(send).status_code == 200
        assert seen[0] == {"Authorization": "Bearer clp_k"}
        assert seen[1]["X-Service-Secret"] == "master"
        # Sticky: later calls go straight to the secret.
        auth.call(send)
        assert "X-Service-Secret" in seen[2]

    def test_call_no_retry_without_secret(self):
        auth = ForgeAuth(api_key="clp_k")
        send = mock.Mock(return_value=_resp(401))
        assert auth.call(send).status_code == 401
        assert send.call_count == 1

    def test_from_settings_reads_env(self, monkeypatch):
        monkeypatch.setenv("CALYPRIUM_API_KEY", "clp_env")
        auth = ForgeAuth.from_settings(None)
        assert auth.headers() == {"Authorization": "Bearer clp_env"}


class TestExtensionsUseBearer:
    SETTINGS = {
        "FORGE_API_URL": "http://forge",
        "CALYPRIUM_API_KEY": "clp_k",
        "RECRAWL_SPIDER_SLUG": "slug",
        "SPIDER_RUN_NUMBER": "7",
        "SPIDER_USER_ID": "u1",
    }

    def test_run_stats_sends_bearer(self):
        from scrapy_calyprium.extensions.run_stats import CalypriumRunStats

        ext = CalypriumRunStats.from_crawler(make_crawler(self.SETTINGS))
        with mock.patch(
            "scrapy_calyprium.extensions.run_stats.httpx.post", return_value=_resp()
        ) as post:
            ext._flush()
        assert post.call_args.kwargs["headers"] == {"Authorization": "Bearer clp_k"}

    def test_request_tracer_sends_bearer(self):
        from scrapy_calyprium.extensions.request_tracer import CalypriumRequestTracer

        ext = CalypriumRequestTracer.from_crawler(make_crawler(self.SETTINGS))
        with mock.patch(
            "scrapy_calyprium.extensions.request_tracer.httpx.post", return_value=_resp()
        ) as post:
            ext._post_batch([{"url": "x"}])
        assert post.call_args.kwargs["headers"] == {"Authorization": "Bearer clp_k"}

    def test_checkpoint_enabled_with_api_key_only(self):
        from scrapy_calyprium.extensions.prism_checkpoint import PrismOffsetCheckpoint

        crawler = make_crawler({**self.SETTINGS, "PRISM_CHECKPOINT_ENABLED": True})
        ext = PrismOffsetCheckpoint.from_crawler(crawler)  # used to raise NotConfigured
        assert ext._headers() == {"Authorization": "Bearer clp_k"}

    def test_checkpoint_not_configured_without_any_credential(self, monkeypatch):
        from scrapy_calyprium.extensions.prism_checkpoint import PrismOffsetCheckpoint

        monkeypatch.delenv("CALYPRIUM_API_KEY", raising=False)
        monkeypatch.delenv("FORGE_SERVICE_SECRET", raising=False)
        crawler = make_crawler({"PRISM_CHECKPOINT_ENABLED": True})
        with pytest.raises(NotConfigured):
            PrismOffsetCheckpoint.from_crawler(crawler)


class TestPipelinesUseBearer:
    def test_recrawl_pipeline_sends_bearer_not_key_as_secret(self):
        from scrapy_calyprium.pipelines.recrawl import RecrawlTrackingPipeline

        crawler = make_crawler({
            "RECRAWL_TRACKING_ENABLED": True,
            "CALYPRIUM_API_KEY": "clp_k",
            "FORGE_API_URL": "http://forge",
            "RECRAWL_SPIDER_SLUG": "slug",
        })
        p = RecrawlTrackingPipeline.from_crawler(crawler)
        with mock.patch("httpx.post", return_value=_resp()) as post:
            p._post([{"url": "http://a", "status": 200}])
        headers = post.call_args.kwargs["headers"]
        assert headers == {"Authorization": "Bearer clp_k"}

    def test_recrawl_pipeline_legacy_secret(self):
        from scrapy_calyprium.pipelines.recrawl import RecrawlTrackingPipeline

        crawler = make_crawler({
            "RECRAWL_TRACKING_ENABLED": True,
            "FORGE_SERVICE_SECRET": "master",
            "SPIDER_USER_ID": "u1",
        })
        p = RecrawlTrackingPipeline.from_crawler(crawler)
        assert p.auth.headers() == {"X-Service-Secret": "master", "X-User-Id": "u1"}

    @pytest.mark.parametrize("cls_name,extra", [
        ("TargetDiscoveryPipeline", {"TARGETS_DISCOVERY_ENABLED": True,
                                     "TARGETS_SPIDER_SLUG": "t"}),
        ("TargetCompletionPipeline", {"TARGETS_COMPLETION_ENABLED": True}),
    ])
    def test_targets_pipelines_enabled_with_api_key_only(self, cls_name, extra):
        from scrapy_calyprium.pipelines import targets

        crawler = make_crawler({
            "FORGE_API_URL": "http://forge", "CALYPRIUM_API_KEY": "clp_k", **extra,
        })
        p = getattr(targets, cls_name).from_crawler(crawler)
        assert p.auth.headers() == {"Authorization": "Bearer clp_k"}


def test_prism_spider_filter_stale_sends_bearer():
    from scrapy_calyprium.spiders.prism_sitemap import PrismSitemapSpider

    class S(PrismSitemapSpider):
        name = "s"
        prism_domain = "example.com"

    spider = make_spider(S, {
        "RECRAWL_TRACKING_ENABLED": True,
        "FORGE_API_URL": "http://forge",
        "CALYPRIUM_API_KEY": "clp_k",
    })
    ok = _resp(body={"stale_urls": ["http://a"], "fresh_count": 1})
    with mock.patch("requests.post", return_value=ok) as post:
        assert spider._filter_fresh_urls(["http://a", "http://b"]) == ["http://a"]
    assert post.call_args.kwargs["headers"] == {"Authorization": "Bearer clp_k"}


class TestLocalProxyUrl:
    def test_built_from_gateway_and_key(self):
        url = build_local_proxy_url("http://proxy-gateway:8080", "clp_a/b")
        assert url == "http://calyprium:clp_a%2Fb@proxy-gateway:8080"

    def test_explicit_setting_wins(self):
        crawler = make_crawler({
            "MIMIC_LOCAL_PROXY_URL": "http://x:y@h:1",
            "VEIL_GATEWAY_URL": "http://proxy-gateway:8080",
            "CALYPRIUM_API_KEY": "clp_k",
        })
        assert resolve_local_proxy_url(crawler.settings) == "http://x:y@h:1"

    def test_derived_when_absent(self):
        crawler = make_crawler({
            "VEIL_GATEWAY_URL": "http://proxy-gateway:8080",
            "CALYPRIUM_API_KEY": "clp_k",
        })
        assert resolve_local_proxy_url(crawler.settings) == (
            "http://calyprium:clp_k@proxy-gateway:8080"
        )

    def test_none_without_key(self):
        crawler = make_crawler({"VEIL_GATEWAY_URL": "http://g:1"})
        assert resolve_local_proxy_url(crawler.settings) is None


def test_solve_client_omits_master_secret_when_key_present():
    from scrapy_calyprium.routing.solve_client import SolveClient

    h = SolveClient("http://m", api_key="clp_k", service_secret="master")._build_headers()
    assert h["Authorization"] == "Bearer clp_k"
    assert "X-Service-Secret" not in h
