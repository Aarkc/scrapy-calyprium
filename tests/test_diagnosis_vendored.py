"""routing/diagnosis.py is Mimic's failure classifier, vendored verbatim (never edited here).

Re-copy it from the monorepo (stealth/mimic/mimic/routing/diagnosis.py) when Mimic changes it; these
checks catch a copy that is missing, edited into something else, or no longer standard-library only.
"""
import ast
import pathlib

from scrapy_calyprium.routing import diagnosis as dg


def test_version_constant_exists():
    assert isinstance(dg.VERSION, int) and dg.VERSION >= 1


def test_module_is_standard_library_only():
    src = pathlib.Path(dg.__file__).read_text(encoding="utf-8")
    roots = set()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            roots.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            roots.add((node.module or "").split(".")[0])
    assert roots <= {"__future__", "re", "dataclasses", "typing", "urllib"}, roots


def test_the_contract_the_sdk_relies_on():
    assert dg.diagnose_response(200, "%PDF-1.7", "application/pdf").cls == dg.OK
    assert dg.diagnose_error(TimeoutError("read timed out")).cls == dg.PROXY_FAILURE
    d = dg.diagnose_response(429, "<html>slow</html>", "text/html", {"Retry-After": "3"}).to_dict()
    assert {k: d[k] for k in ("class", "number", "side", "move", "retry_after")} == {
        "class": "rate_limited", "number": 5, "side": "ours", "move": "slow_down", "retry_after": 3.0}
    assert sorted(c.number for c in dg.CLASSES.values()) == list(range(17))


def test_the_copy_has_what_block_detect_reads():
    """VERSION 4 (Mimic PR #241) reads block pages served as success; block_detect.challenge_family
    reads its akamai_reference and its Akamai 200 evidence."""
    assert dg.VERSION >= 4
    assert (dg.VERCEL, dg.FASTLY) == ("vercel", "fastly")
    assert dg.akamai_reference("Reference&#32;&#35;18&#46;1a2b3c4d&#46;1700000000")
    assert not dg.akamai_reference("<p>no reference here</p>")


def test_version_5_reads_amazons_robot_check_by_its_form_path():
    """VERSION 5 (Mimic PR #256): a page that only names `ValidateCaptchaInputs` in a script (musicstore.com's
    listings) is not Amazon's robot check; a Cloudflare challenge is a block under any content type."""
    from scrapy_calyprium.routing import diagnosis as dg
    assert dg.VERSION >= 5
    page = ("<html><head><title>Guitars | MUSIC STORE</title></head><body>" + "<p>x</p>" * 2000
            + "<script>var a='ViewCheckoutPayment-ValidateCaptchaInputs';</script></body></html>")
    assert dg.diagnose_response(200, page).cls != "captcha"


def test_version_6_names_f5s_bot_defence():
    """VERSION 6: F5 BIG-IP's bot defence (wyobiz.wyo.gov). Its image captcha, served 200, was read as a page; its
    empty answer to a search POST without its script-made cookies was an anonymous low-confidence soft block. Both
    reach the SDK through block_detect, with Scrapy's byte headers or the local fetch's flattened ones."""
    from scrapy_calyprium.routing import block_detect as bd

    assert dg.VERSION >= 6 and dg.F5 == "f5"
    captcha = ('<html><head><script>window["bobcmn"] = "1011TSPD_101";</script></head><body>This question is for '
               'testing whether you are a human visitor and to prevent automated spam submission.<br><img src="data:'
               'image/png;base64,' + "A" * 4000 + '"><b>What code is in the image?</b><input id="ans" name="answer">'
               '<button id="jar" type="button">submit</button></body></html>')
    d = bd.diagnose(200, captcha.encode(), "text/html")
    assert (d.cls, d.vendor) == (dg.CAPTCHA, dg.F5) and bd.is_blocked(200, captcha.encode(), "text/html")
    scrapy_headers = {b"Content-Type": [b"text/html"],
                      b"Set-Cookie": [b"TS758a99fc029=01ab; Path=/", b"TS00000000027=08c2; path=/"]}
    flattened = {"content-type": "text/html", "set-cookie": "TS758a99fc029=01ab; Path=/, TS00000000027=08c2; path=/"}
    for headers in (scrapy_headers, flattened):
        d = bd.diagnose(200, b"", headers=headers)
        assert (d.cls, d.vendor, d.evidence) == (dg.CHALLENGE, dg.F5, "f5_empty_answer"), headers
        assert bd.is_blocked(200, b"", headers=headers)
    # no solve family names F5 (no solver for it): the solve is the plain browser one
    assert bd.challenge_family(200, b"", headers=flattened) is None
    assert bd.challenge_family(200, captcha.encode()) is None
