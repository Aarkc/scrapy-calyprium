"""A run's progress, for the time it has left.

A spider that knows how much work it has says so in three Scrapy stats. ``CalypriumRunStats`` sends them with each
report, and Forge divides the work left by the recent pace of the work done to estimate when the run ends:

    calyprium/progress/done   work finished in this run (only grows)
    calyprium/progress/left   work still ahead, as best known (it may change as the spider learns more)
    calyprium/progress/unit   what both are counted in: "urls", "companies", "pages"...

``report(spider, done=..., left=..., unit=...)`` sets all three; call it whenever the counts change (it is cheap).
``PrismSitemapSpider`` reports its place in Prism's URL list by itself. A spider that reports nothing gets an
estimate from its earlier runs' durations instead.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from scrapy import Spider

DONE = "calyprium/progress/done"
LEFT = "calyprium/progress/left"
UNIT = "calyprium/progress/unit"


def _stats(spider_or_stats: Any):
    if isinstance(spider_or_stats, Spider):
        return getattr(getattr(spider_or_stats, "crawler", None), "stats", None)
    return spider_or_stats if hasattr(spider_or_stats, "set_value") else None


def report(spider_or_stats: Any, *, done: int, left: int, unit: str = "pages") -> None:
    """Set the run's progress on a spider (or a Scrapy stats collector): `done` and `left` in `unit`."""
    stats = _stats(spider_or_stats)
    if stats is None:
        return
    stats.set_value(DONE, max(0, int(done)))
    stats.set_value(LEFT, max(0, int(left)))
    stats.set_value(UNIT, str(unit))


def read(stats: Any) -> Dict[str, Optional[Any]]:
    """The progress a report carries: {"progress_done", "progress_left", "progress_unit"}, None where unset."""
    out: Dict[str, Optional[Any]] = {"progress_done": None, "progress_left": None, "progress_unit": None}
    if stats is None:
        return out
    try:
        done, left, unit = stats.get_value(DONE), stats.get_value(LEFT), stats.get_value(UNIT)
    except Exception:  # noqa: BLE001 -- progress never breaks a report
        return out
    if done is None or left is None:
        return out
    try:
        out.update(progress_done=int(done), progress_left=int(left), progress_unit=str(unit or "pages"))
    except (TypeError, ValueError):
        pass
    return out
