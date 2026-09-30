"""Shared test helpers."""
from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

from scrapy.settings import Settings
from scrapy.signalmanager import SignalManager


def make_crawler(settings_dict=None, spider=None):
    """A reactor-free stand-in for ``scrapy.crawler.Crawler``: real Settings
    and SignalManager, mock stats. ``get_crawler`` needs an installed reactor on
    Scrapy >= 2.13, which unit tests shouldn't have to set up."""
    stats = mock.Mock()
    stats._values = {}
    stats.inc_value.side_effect = lambda k, count=1, start=0, **kw: stats._values.__setitem__(
        k, stats._values.get(k, start) + count)
    stats.set_value.side_effect = lambda k, v, **kw: stats._values.__setitem__(k, v)
    stats.get_value.side_effect = lambda k, default=None, **kw: stats._values.get(k, default)
    return SimpleNamespace(
        settings=Settings(settings_dict or {}),
        signals=SignalManager(),
        stats=stats,
        spider=spider,
        engine=None,
        extensions=None,
    )


def make_spider(spidercls, settings_dict=None, **kwargs):
    """Instantiate a spider with ``.settings``/``.crawler`` bound, no reactor."""
    crawler = make_crawler(settings_dict)
    spider = spidercls(**kwargs)
    spider.crawler = crawler
    spider.settings = crawler.settings
    crawler.spider = spider
    return spider
