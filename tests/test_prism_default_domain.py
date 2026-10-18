"""A PrismSitemapSpider with neither url_source nor prism_domain reads its first allowed domain's
URLs from Prism instead of closing without a request (2026-10-18 fleet check: 9 spiders)."""
from scrapy_calyprium.spiders.prism_sitemap import PrismSitemapSpider


class NoSource(PrismSitemapSpider):
    name = "nosource"
    allowed_domains = ["www.jauch.com", "jauch.com"]


class Explicit(PrismSitemapSpider):
    name = "explicit"
    allowed_domains = ["www.jauch.com"]
    prism_domain = "www.other.example"


class Nothing(PrismSitemapSpider):
    name = "nothing"


def test_falls_back_to_the_first_allowed_domain():
    assert NoSource().url_source == "prism://www.jauch.com"


def test_an_explicit_prism_domain_wins():
    assert Explicit().url_source == "prism://www.other.example"


def test_an_explicit_url_source_wins():
    assert NoSource(url_source="sitemap://www.jauch.com/sitemap.xml").url_source == \
        "sitemap://www.jauch.com/sitemap.xml"


def test_no_allowed_domains_keeps_the_old_behaviour():
    assert Nothing().url_source is None
