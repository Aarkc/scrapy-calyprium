"""How much a crawl has going at once: requests in flight, requests queued, solves in progress.

``ConcurrencySampler`` reads the engine and is sampled once a second from CalypriumRunStats' reporter thread:

    in_flight          requests in Scrapy's downloader, including their time in the downloader middlewares (Mimic's
                       local fetch and its solves among them), against CONCURRENT_REQUESTS (``concurrency_limit``)
    queued             requests waiting in the scheduler
    solves_in_flight   Mimic solves in progress (the stat SOLVES_IN_FLIGHT, kept by SpiderAutoRouter)

``window(name)`` gives the latest sample, the average and the peak since that name's last call, so the 30 s report
to Forge and the minute's log line each keep their own window. Counting another thread's queues can race a change
to them: a count that raises is left out of that sample.
"""
from __future__ import annotations

import threading
from typing import Dict, Optional

# A gauge in Scrapy's stats: +1 when a Mimic solve starts, -1 when it ends (SpiderAutoRouter._solve).
SOLVES_IN_FLIGHT = "calyprium/solves_in_flight"


class _Window:
    __slots__ = ("n", "in_flight_sum", "in_flight_max", "solves_max")

    def __init__(self) -> None:
        self.n = 0
        self.in_flight_sum = 0
        self.in_flight_max = 0
        self.solves_max = 0


class ConcurrencySampler:
    def __init__(self, crawler) -> None:
        self.crawler = crawler
        self._lock = threading.Lock()
        self._windows: Dict[str, _Window] = {}
        self._last: Optional[Dict[str, Optional[int]]] = None
        try:
            self.limit: Optional[int] = crawler.settings.getint("CONCURRENT_REQUESTS")
        except Exception:  # noqa: BLE001 -- a sampler never breaks a crawl
            self.limit = None

    def read(self) -> Optional[Dict[str, Optional[int]]]:
        """The counts now, or None before the engine runs (or if requests in flight can't be counted)."""
        engine = getattr(self.crawler, "engine", None)
        downloader = getattr(engine, "downloader", None)
        if downloader is None:
            return None
        try:
            in_flight = len(downloader.active)
        except Exception:  # noqa: BLE001
            return None
        queued = None
        slot = getattr(engine, "slot", None) or getattr(engine, "_slot", None)
        scheduler = getattr(slot, "scheduler", None)
        if scheduler is not None:
            try:
                queued = len(scheduler)
            except Exception:  # noqa: BLE001 -- the queues changed while counted
                queued = None
        stats = getattr(self.crawler, "stats", None)
        try:
            solves = max(0, int((stats.get_value(SOLVES_IN_FLIGHT) if stats is not None else 0) or 0))
        except Exception:  # noqa: BLE001
            solves = 0
        return {"in_flight": in_flight, "queued": queued, "solves_in_flight": solves}

    def sample(self) -> None:
        now = self.read()
        if now is None:
            return
        with self._lock:
            self._last = now
            for w in self._windows.values():
                w.n += 1
                w.in_flight_sum += now["in_flight"]
                w.in_flight_max = max(w.in_flight_max, now["in_flight"])
                w.solves_max = max(w.solves_max, now["solves_in_flight"])

    def watch(self, name: str) -> None:
        """Start a window (the first call of ``window(name)`` covers the samples since)."""
        with self._lock:
            self._windows.setdefault(name, _Window())

    def window(self, name: str) -> Dict[str, Optional[float]]:
        """The latest counts with the average and the peak since the last call for ``name``; {} without a sample."""
        with self._lock:
            w = self._windows.get(name) or _Window()
            self._windows[name] = _Window()
            last = self._last
        if last is None:
            return {}
        out = {
            "in_flight": last["in_flight"],
            "in_flight_avg": round(w.in_flight_sum / w.n, 2) if w.n else float(last["in_flight"]),
            "in_flight_max": w.in_flight_max if w.n else last["in_flight"],
            "queued": last["queued"],
            "solves_in_flight": last["solves_in_flight"],
            "solves_in_flight_max": w.solves_max if w.n else last["solves_in_flight"],
            "concurrency_limit": self.limit,
        }
        return out


def describe(window: Dict[str, Optional[float]], seconds: float) -> str:
    """One log line for a window (see ConcurrencySampler.window)."""
    limit = window.get("concurrency_limit")
    queued = window.get("queued")
    return (
        f"Concurrency over the last {int(seconds)} s: {window['in_flight']} requests in flight "
        f"(average {window['in_flight_avg']:g}, peak {window['in_flight_max']}"
        + (f" of {limit} allowed" if limit else "")
        + f"), {'unknown' if queued is None else f'{queued:,}'} queued, "
        f"{window['solves_in_flight']} solves in progress (peak {window['solves_in_flight_max']})"
    )
