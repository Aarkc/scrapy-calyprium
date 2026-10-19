"""SnapshotRecorder: a bounded, best-effort sample of raw pages per run."""
from __future__ import annotations

import gzip
import hashlib
from datetime import datetime
from unittest import mock

import pytest
from scrapy.exceptions import NotConfigured
from scrapy.http import HtmlResponse, Request, TextResponse

from scrapy_calyprium.extensions import snapshot_recorder as sr
from scrapy_calyprium.extensions.snapshot_recorder import SnapshotRecorder, snapshot_key
from tests._helpers import make_crawler

SETTINGS = {
    "FORGE_API_URL": "http://forge:8000/",
    "CALYPRIUM_API_KEY": "clp_run",
    "SPIDER_USER_ID": "u1",
    "SPIDER_NAME": "spider-slug",
    "SPIDER_SLUG": "spider-slug",
    "SPIDER_RUN_NUMBER": "7",
    "S3_BUCKET": "calyprium",
    "S3_BATCH_PATH": "u1/3f2a-storage/runs/7/batch_{batch_id}.jl",
}

REAL_PAGE = (
    b"<html><head><title>Part</title></head><body><main><h1>Widget</h1>"
    + b"<p>" + b"real content " * 1200 + b"</p></main></body></html>"
)


class FakeS3:
    def __init__(self, fail=False):
        self.fail = fail
        self.objects = {}
        self.calls = []

    def put_object(self, Bucket, Key, Body, ContentLength, ContentType):
        self.calls.append((Bucket, Key, ContentType))
        if self.fail:
            raise RuntimeError("An error occurred (500) when calling PutObject")
        self.objects[Key] = Body.read()
        assert len(self.objects[Key]) == ContentLength


def _resp(status=200, body=REAL_PAGE, url="http://example.com/p", ctype=b"text/html; charset=utf-8",
          headers=None):
    h = {"Content-Type": ctype} if ctype else {}
    h.update(headers or {})
    cls = HtmlResponse if ctype and b"html" in ctype else TextResponse
    return cls(url=url, status=status, body=body, headers=h, request=Request(url),
               encoding="utf-8")


def make_ext(settings=None, fake=None):
    crawler = make_crawler({**SETTINGS, **(settings or {})})
    ext = SnapshotRecorder.from_crawler(crawler)
    ext._client = fake if fake is not None else FakeS3()
    return ext


def feed(ext, responses):
    for r in responses:
        ext.response_received(r, r.request, None)


def close(ext, post_status=200):
    resp = mock.Mock(status_code=post_status)
    with mock.patch.object(sr.httpx, "post", return_value=resp) as post:
        ext.spider_closed(None, "finished")
    return post


def kinds(ext):
    return sorted(s["kind"] for s in ext._snapshots)


# -- enablement --------------------------------------------------------------


class TestEnablement:
    def test_enabled_by_default_on_platform_runs(self):
        ext = make_ext()
        assert ext.run_number == 7 and ext.spider_slug == "spider-slug"
        assert ext.sample_limit == 5 and ext.error_limit == 5
        assert ext.max_bytes == 2 * 1024 * 1024

    def test_disabled_flag(self):
        with pytest.raises(NotConfigured):
            make_ext({"SNAPSHOTS_ENABLED": False})

    def test_disabled_without_run_number(self):
        with pytest.raises(NotConfigured):
            make_ext({"SPIDER_RUN_NUMBER": "0"})

    def test_disabled_without_credentials(self, monkeypatch):
        for var in ("CALYPRIUM_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(NotConfigured):
            make_ext({"CALYPRIUM_API_KEY": ""})


# -- capture limits and kinds ------------------------------------------------


class TestCapture:
    def test_samples_are_spread_over_the_run(self):
        """The 1st, 2nd, 3rd, 10th and 30th page: the first five alone were often one kind of
        page (degson_parts' sitemap starts with news and events)."""
        fake = FakeS3()
        ext = make_ext(fake=fake)
        feed(ext, [_resp(url=f"http://example.com/{i}", body=REAL_PAGE + str(i).encode())
                   for i in range(40)])
        close(ext)
        assert kinds(ext) == ["sample"] * 5
        assert sorted((s["url"] for s in ext._snapshots), key=lambda u: int(u.rsplit("/", 1)[1])) == [
            f"http://example.com/{i}" for i in (0, 1, 2, 9, 29)]
        assert len(fake.objects) == 5

    def test_sample_positions(self):
        assert sr.sample_positions(0) == []
        assert sr.sample_positions(5) == [1, 2, 3, 10, 30]
        assert sr.sample_positions(8) == [1, 2, 3, 10, 30, 100, 300, 1000]

    def test_a_json_success_is_an_api_snapshot_and_other_non_html_is_ignored(self):
        ext = make_ext()
        feed(ext, [_resp(body=b'{"a": 1}', ctype=b"application/json"),
                   _resp(body=b"%PDF-1.7 ...", ctype=b"application/pdf")])
        close(ext)
        assert kinds(ext) == ["api"]
        (snap,) = ext._snapshots
        assert snap["method"] == "GET" and snap["content_type"] == "application/json"
        assert "request_body" not in snap

    def test_api_snapshots_keep_their_post_request_redacted(self):
        """vitrohm_parts POSTs part numbers to a search API: the answer is saved with the
        request that fetched it, credential-like fields redacted."""
        ext = make_ext()
        api = "https://yageogroup.com/search/query/raw"
        req = Request(api, method="POST",
                      body=b'{"input": "KHS400JB", "apiKey": "s3cr3t", "setting": {"token": "t"}}')
        resp = TextResponse(url=api, status=200, body=b'{"results": []}', request=req,
                            headers={"Content-Type": "application/json"}, encoding="utf-8")
        ext.response_received(resp, req, None)
        close(ext)
        (snap,) = ext._snapshots
        assert snap["kind"] == "api" and snap["method"] == "POST"
        body = snap["request_body"]
        assert '"input":"KHS400JB"' in body and "s3cr3t" not in body and '"t"' not in body
        assert body.count("[redacted]") == 2

    def test_internal_requests_are_never_saved(self):
        """Prism's URL listing (JSON) and scans are the SDK's requests, not the site's."""
        ext = make_ext()
        req = Request("http://prism/api/domains/x/urls", meta={"_internal": True})
        resp = TextResponse(url=req.url, status=200, body=b'{"urls": []}', request=req,
                            headers={"Content-Type": "application/json"}, encoding="utf-8")
        ext.response_received(resp, req, None)
        close(ext)
        assert ext._snapshots == []

    def test_samples_name_their_callback_and_skip_sdk_navigation(self):
        """A discovery spider's listing pages are not samples; its item pages are, recorded as
        parse_item (the wrapper's name maps to the spider's callback)."""
        class Spider:
            def _discovery_page(self, response):
                pass

            def _discovery_parse(self, response):
                pass

            def parse_detail(self, response):
                pass

        sp = Spider()
        ext = make_ext()

        def page(url, cb):
            req = Request(url, callback=cb)
            return HtmlResponse(url=url, status=200, body=REAL_PAGE, request=req,
                                headers={"Content-Type": "text/html"}, encoding="utf-8"), req

        for url, cb in [("http://e/list", sp._discovery_page), ("http://e/p/1", sp._discovery_parse),
                        ("http://e/d/2", sp.parse_detail), ("http://e/home", None)]:
            resp, req = page(url, cb)
            ext.response_received(resp, req, None)
        close(ext)
        got = {s["url"]: s["callback"] for s in ext._snapshots}
        assert got == {"http://e/p/1": "parse_item", "http://e/d/2": "parse_detail",
                       "http://e/home": "parse"}

    def test_api_limit(self):
        ext = make_ext({"SNAPSHOTS_API": 2})
        feed(ext, [_resp(body=b'{"n": %d}' % i, ctype=b"application/json",
                         url=f"http://example.com/api/{i}") for i in range(4)])
        close(ext)
        assert kinds(ext) == ["api", "api"]

    def test_errors_and_blocked(self):
        ext = make_ext()
        cf = _resp(status=403, body=b"<html><title>Just a moment...</title></html>",
                   url="http://example.com/cf", headers={"Server": "cloudflare"})
        e500 = _resp(status=500, body=b"<html><body>" + b"x" * 30_000 + b"</body></html>",
                     url="http://example.com/500")
        e404 = _resp(status=404, body=b"not found", ctype=b"text/plain",
                     url="http://example.com/404")
        challenge_200 = _resp(status=200, url="http://example.com/chl",
                              body=b"<html><script>window._cf_chl_opt={}</script></html>")
        feed(ext, [cf, e500, e404, challenge_200])
        close(ext)
        by_url = {s["url"]: s for s in ext._snapshots}
        assert by_url["http://example.com/cf"]["kind"] == "blocked"
        assert by_url["http://example.com/cf"]["status_code"] == 403
        assert by_url["http://example.com/500"]["kind"] == "error"
        assert by_url["http://example.com/404"]["kind"] == "error"
        assert by_url["http://example.com/chl"]["kind"] == "blocked"
        assert ext._samples == 0  # a challenge 200 is not a sample

    def test_error_limit(self):
        ext = make_ext({"SNAPSHOTS_ERRORS": 2, "SNAPSHOTS_SAMPLE": 1})
        feed(ext, [_resp(status=500, url=f"http://example.com/{i}",
                         body=b"<html>" + b"e" * 30_000 + bytes([65 + i]) + b"</html>")
                   for i in range(6)])
        feed(ext, [_resp(url="http://example.com/ok1"), _resp(url="http://example.com/ok2")])
        close(ext)
        assert kinds(ext) == ["error", "error", "sample"]

    def test_zero_limits_capture_nothing(self):
        ext = make_ext({"SNAPSHOTS_ERRORS": 0, "SNAPSHOTS_SAMPLE": 0})
        feed(ext, [_resp(), _resp(status=500)])
        close(ext)
        assert ext._snapshots == []

    def test_size_cap_skips_without_using_a_slot(self):
        ext = make_ext({"SNAPSHOTS_MAX_BYTES": 50_000, "SNAPSHOTS_SAMPLE": 1})
        big = _resp(url="http://example.com/big", body=b"<html>" + b"a" * 60_000 + b"</html>")
        feed(ext, [big, _resp(url="http://example.com/small")])
        close(ext)
        assert [s["url"] for s in ext._snapshots] == ["http://example.com/small"]
        assert ext.crawler.stats.get_value("snapshots/skipped_too_large") == 1

    def test_capture_after_close_is_ignored(self):
        ext = make_ext()
        close(ext)
        feed(ext, [_resp()])
        assert ext._futures == []

    def test_bad_response_never_raises(self):
        ext = make_ext()
        ext.response_received(object(), None, None)  # no headers/body at all
        assert ext._snapshots == []


# -- storage format ------------------------------------------------------------


class TestStorage:
    def test_gzip_hash_and_key_format(self):
        fake = FakeS3()
        ext = make_ext(fake=fake)
        feed(ext, [_resp()])
        close(ext)
        (snap,) = ext._snapshots
        digest = hashlib.sha256(REAL_PAGE).hexdigest()
        assert snap["content_hash"] == digest
        assert snap["storage_key"] == f"u1/3f2a-storage/runs/7/snapshots/{digest}.html.gz"
        assert fake.calls == [("calyprium", snap["storage_key"], "application/gzip")]
        assert gzip.decompress(fake.objects[snap["storage_key"]]) == REAL_PAGE
        assert snap["status_code"] == 200 and snap["url"] == "http://example.com/p"
        assert datetime.fromisoformat(snap["captured_at"]).tzinfo is not None

    def test_prefix_follows_default_batch_template(self):
        settings = {k: v for k, v in SETTINGS.items() if k != "S3_BATCH_PATH"}
        crawler = make_crawler(settings)
        ext = SnapshotRecorder.from_crawler(crawler)
        assert ext.run_prefix == "u1/spider-slug/runs/7"

    def test_snapshot_key_flat_template(self):
        assert snapshot_key("", "ab") == "snapshots/ab.html.gz"

    def test_identical_bodies_upload_once(self):
        fake = FakeS3()
        ext = make_ext(fake=fake)
        feed(ext, [_resp(url="http://example.com/a")])
        ext._futures[0].result()
        feed(ext, [_resp(url="http://example.com/b")])
        close(ext)
        assert len(fake.calls) == 1
        assert len(ext._snapshots) == 2
        assert ext._snapshots[0]["storage_key"] == ext._snapshots[1]["storage_key"]


# -- forge report -------------------------------------------------------------


class TestReport:
    def test_payload_url_and_bearer(self):
        ext = make_ext()
        feed(ext, [_resp(), _resp(status=500, url="http://example.com/err",
                                  body=b"<html>" + b"e" * 30_000 + b"</html>")])
        post = close(ext)
        assert post.call_count == 1
        args, kwargs = post.call_args
        assert args[0] == "http://forge:8000/jobs/spiders/spider-slug/runs/7/snapshots"
        assert kwargs["headers"] == {"Authorization": "Bearer clp_run"}
        assert kwargs["timeout"] > 0
        snaps = kwargs["json"]["snapshots"]
        assert sorted(s["kind"] for s in snaps) == ["error", "sample"]
        for s in snaps:
            assert set(s) == {"url", "status_code", "content_hash", "storage_key",
                              "kind", "captured_at", "method", "content_type", "callback"}
            assert s["method"] == "GET" and s["callback"] == "parse"

    def test_a_forge_without_the_api_kind_still_gets_the_pages(self):
        ext = make_ext()
        feed(ext, [_resp(), _resp(body=b'{"a": 1}', ctype=b"application/json",
                                  url="http://example.com/api")])
        responses = [mock.Mock(status_code=422), mock.Mock(status_code=200)]
        with mock.patch.object(sr.httpx, "post", side_effect=responses) as post:
            ext.spider_closed(None, "finished")
        assert post.call_count == 2
        retried = post.call_args_list[1].kwargs["json"]["snapshots"]
        assert [s["kind"] for s in retried] == ["sample"]
        assert set(retried[0]) == {"url", "status_code", "content_hash", "storage_key",
                                   "kind", "captured_at"}

    def test_legacy_secret_auth_without_api_key(self, monkeypatch):
        monkeypatch.delenv("CALYPRIUM_API_KEY", raising=False)
        ext = make_ext({"CALYPRIUM_API_KEY": "", "AWS_ACCESS_KEY_ID": "k",
                        "AWS_SECRET_ACCESS_KEY": "k", "FORGE_SERVICE_SECRET": "master"})
        feed(ext, [_resp()])
        post = close(ext)
        assert post.call_args.kwargs["headers"] == {
            "X-Service-Secret": "master", "X-User-Id": "u1"}

    def test_recrawl_slug_wins(self):
        ext = make_ext({"RECRAWL_SPIDER_SLUG": "renamed"})
        feed(ext, [_resp()])
        post = close(ext)
        assert "/jobs/spiders/renamed/runs/7/snapshots" in post.call_args.args[0]

    def test_nothing_captured_means_no_post(self):
        ext = make_ext()
        post = close(ext)
        post.assert_not_called()

    @pytest.mark.parametrize("status", [404, 500, 503])
    def test_forge_error_is_tolerated(self, status, caplog):
        ext = make_ext()
        feed(ext, [_resp()])
        with caplog.at_level("WARNING"):
            close(ext, post_status=status)
        assert any(str(status) in r.getMessage() for r in caplog.records)

    def test_forge_unreachable_is_tolerated(self, caplog):
        ext = make_ext()
        feed(ext, [_resp()])
        with mock.patch.object(sr.httpx, "post", side_effect=sr.httpx.ConnectError("down")):
            with caplog.at_level("WARNING"):
                ext.spider_closed(None, "finished")
        assert any("failed" in r.getMessage() for r in caplog.records)


# -- failure tolerance ----------------------------------------------------------


class TestFailures:
    def test_upload_error_is_swallowed_and_not_reported(self, caplog):
        fake = FakeS3(fail=True)
        ext = make_ext(fake=fake)
        with caplog.at_level("WARNING"):
            feed(ext, [_resp(), _resp(status=500, url="http://example.com/e")])
            post = close(ext)
        assert ext._snapshots == []
        post.assert_not_called()
        assert ext.crawler.stats.get_value("snapshots/upload_failed") == 2
        assert any("upload" in r.getMessage() for r in caplog.records)

    def test_partial_upload_failure_reports_the_rest(self):
        fake = FakeS3()
        ext = make_ext(fake=fake)
        feed(ext, [_resp(url="http://example.com/a")])
        ext._futures[0].result()
        fake.fail = True
        feed(ext, [_resp(url="http://example.com/b", body=REAL_PAGE + b"b")])
        post = close(ext)
        snaps = post.call_args.kwargs["json"]["snapshots"]
        assert [s["url"] for s in snaps] == ["http://example.com/a"]

    def test_slow_uploads_do_not_hold_close_past_timeout(self):
        import threading

        gate = threading.Event()

        class SlowS3(FakeS3):
            def put_object(self, **kw):
                gate.wait(5)
                super().put_object(**kw)

        ext = make_ext({"SNAPSHOTS_CLOSE_TIMEOUT": 0.2}, fake=SlowS3())
        feed(ext, [_resp()])
        post = close(ext)
        gate.set()
        post.assert_not_called()  # nothing finished in time; close still returned

    def test_close_errors_are_swallowed(self):
        ext = make_ext()
        feed(ext, [_resp()])
        with mock.patch.object(ext, "report", side_effect=RuntimeError("boom")):
            ext.spider_closed(None, "finished")  # must not raise
