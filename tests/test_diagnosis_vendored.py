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
