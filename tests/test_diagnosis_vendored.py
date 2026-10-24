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
