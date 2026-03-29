"""
Base spider classes.

- ``PrismSitemapSpider`` — reads pre-indexed URLs from Prism's sitemap database,
  following Scrapy's SitemapSpider convention but skipping the fetch step.
"""

from scrapy_calyprium.spiders.prism_sitemap import PrismSitemapSpider

__all__ = ["PrismSitemapSpider"]
