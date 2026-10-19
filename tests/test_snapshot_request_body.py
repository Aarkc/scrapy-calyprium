"""What a snapshot keeps of the request that fetched it: never a credential."""
import pytest
from scrapy.http import Request

from scrapy_calyprium.extensions.snapshot_recorder import REDACTED, request_body_for_snapshot


def test_a_get_or_an_empty_body_keeps_nothing():
    assert request_body_for_snapshot(Request("https://x.example/")) is None
    assert request_body_for_snapshot(Request("https://x.example/", method="POST")) is None


def test_json_fields_named_like_credentials_are_redacted_at_any_depth():
    body = b'{"q": "LM358", "password": "p", "nested": [{"access_token": "t", "n": 1}]}'
    out = request_body_for_snapshot(Request("https://x.example/", method="POST", body=body))
    assert out == '{"q":"LM358","password":"[redacted]","nested":[{"access_token":"[redacted]","n":1}]}'


def test_form_fields_named_like_credentials_are_redacted():
    body = b"q=LM358&page=2&session_id=abc&pass=hunter2"
    out = request_body_for_snapshot(Request("https://x.example/", method="POST", body=body))
    assert out == "q=LM358&page=2&session_id=%5Bredacted%5D&pass=%5Bredacted%5D"


def test_an_unparsed_body_mentioning_a_credential_is_not_kept():
    body = b"<login><password>p</password></login>"
    assert request_body_for_snapshot(Request("https://x.example/", method="POST", body=body)) == REDACTED


@pytest.mark.parametrize("limit", [10, 4096])
def test_the_body_is_capped(limit):
    body = ('{"q": "' + "x" * 10000 + '"}').encode()
    out = request_body_for_snapshot(Request("https://x.example/", method="POST", body=body), limit)
    assert len(out) == limit
