"""_response_from_envelope: the Mimic /api/fetch envelope must be wrapped in the
Scrapy Response class matching its Content-Type, so a JSON/XML API body parses
instead of being HTML-parsed into 0 items (AAR-12 / Legistar OData regression)."""
from scrapy.http import HtmlResponse, Request, Response, TextResponse, XmlResponse

from scrapy_calyprium.middleware.mimic import _response_from_envelope

REQ = Request("https://webapi.legistar.com/v1/broward/events")
XML = (
    b"<ArrayOfGranicusEvent><GranicusEvent>"
    b"<EventBodyName>County Commission</EventBodyName>"
    b"</GranicusEvent></ArrayOfGranicusEvent>"
)


def _wrap(ct, body=XML):
    return _response_from_envelope(
        REQ, url="https://x/", status=200, headers={"Content-Type": ct} if ct else None,
        body=body, content_type=ct,
    )


def test_xml_becomes_xmlresponse_and_xpath_works():
    r = _wrap("application/xml")
    assert isinstance(r, XmlResponse)
    # The whole point: XML xpath (case-sensitive tags) resolves.
    assert r.xpath("//EventBodyName/text()").get() == "County Commission"


def test_xml_with_charset():
    r = _wrap("application/xml; charset=utf-8")
    assert isinstance(r, XmlResponse)
    assert len(r.xpath("//GranicusEvent")) == 1


def test_json_becomes_textresponse():
    r = _wrap("application/json", body=b'{"items":[1,2,3]}')
    assert isinstance(r, TextResponse) and not isinstance(r, (HtmlResponse, XmlResponse))
    assert r.json() == {"items": [1, 2, 3]}


def test_html_stays_htmlresponse():
    assert isinstance(_wrap("text/html", body=b"<html><body>hi</body></html>"), HtmlResponse)


def test_missing_content_type_defaults_to_html():
    assert isinstance(_wrap("", body=b"<html></html>"), HtmlResponse)


def test_binary_stays_raw_response():
    r = _wrap("application/pdf", body=b"%PDF-1.7 ...")
    assert type(r) is Response  # raw bytes, not decoded
    assert r.body.startswith(b"%PDF")


def test_str_body_is_encoded():
    r = _response_from_envelope(
        REQ, url="https://x/", status=200, headers=None,
        body="<a>café</a>", content_type="application/xml",
    )
    assert isinstance(r, XmlResponse)
    assert r.xpath("//a/text()").get() == "café"
