"""Current-Chrome httpcloak presets the spider can replay a solve's clearance on.

Calyprium's solvers present the current Chrome release through a preset named
``chrome-<N>-windows`` that they derive inside their own process when httpcloak
does not bundle that release (Akamai denies a Chrome identity one release
behind). A clearance they earn names that preset, and httpcloak refuses a name it
does not know, so the spider derives it the same way before replaying: the
newest bundled Windows preset (same TLS / HTTP2 / header order) with the user
agent and the ``sec-ch-ua`` brand list regenerated for release N by Chromium's
own GREASE-brand algorithm.

The canonical copy is ``tessera/sigsolver/common/chrome.py`` in the Calyprium
repository (mimic keeps another); keep the three the same. They only agree when
the bundled base preset is the same, so pin the same httpcloak as the services.
"""
import json
import logging
import re
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# Chromium components/embedder_support/user_agent_utils.cc (GenerateBrandVersionList)
_GREASE_CHARS = [" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_"]
_GREASE_VERSIONS = ["8", "99", "24"]
_ORDERS = [(0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0)]
_DERIVABLE = re.compile(r"chrome-(\d+)-windows")


def sec_ch_ua(major: int) -> str:
    """The sec-ch-ua header Chrome ``major`` sends."""
    grease = f"Not{_GREASE_CHARS[major % 11]}A{_GREASE_CHARS[(major + 1) % 11]}Brand"
    brands = [(grease, _GREASE_VERSIONS[major % 3]), ("Chromium", str(major)), ("Google Chrome", str(major))]
    out = [None, None, None]
    for i, pos in enumerate(_ORDERS[major % 6]):
        out[pos] = brands[i]
    return ", ".join(f'"{b}";v="{v}"' for b, v in out)


def _base_preset(httpcloak) -> str:
    """Newest bundled chrome-<N>-windows preset name."""
    names = [n for n in httpcloak.available_presets() if re.fullmatch(r"chrome-\d+-windows", n)]
    return max(names, key=lambda n: int(n.split("-")[1]))


# httpcloak registers a preset name once per process and does not list the presets registered from JSON, so the
# derived ones are remembered here: every caller in a process gets the same.
_DERIVED = set()
_DERIVE_LOCK = threading.Lock()


def ensure_chrome_preset(major: Optional[int]) -> str:
    """Register ``chrome-<major>-windows`` (if httpcloak lacks it) and return the
    preset name to use. ``major`` None/0 -> the newest bundled Windows preset.
    Safe to call again, from any thread."""
    import httpcloak  # noqa: WPS433

    base = _base_preset(httpcloak)
    if not major:
        return base
    name = f"chrome-{major}-windows"
    if name in httpcloak.available_presets():
        return name
    with _DERIVE_LOCK:
        if name not in _DERIVED:
            _DERIVED.add(_derive_preset(httpcloak, base, major, name))
    return name


def _derive_preset(httpcloak, base: str, major: int, name: str) -> str:
    """Register ``name``: the bundled preset ``base`` presenting Chrome ``major``."""
    spec = json.loads(httpcloak.describe_preset(base))
    preset = spec["preset"]
    base_major = int(base.split("-")[1])
    old_ua, new_ua = sec_ch_ua(base_major), sec_ch_ua(major)
    headers = preset["headers"]
    headers["user_agent"] = headers["user_agent"].replace(f"Chrome/{base_major}.", f"Chrome/{major}.")
    for k, v in list(headers.get("values", {}).items()):
        if k.lower() == "sec-ch-ua":
            headers["values"][k] = new_ua
    for entry in headers.get("order", []):
        if entry.get("key", "").lower() == "sec-ch-ua":
            entry["value"] = new_ua
    preset["name"] = name
    blob = json.dumps(spec)
    if old_ua in blob:  # anything else still advertising the base version
        blob = blob.replace(json.dumps(old_ua)[1:-1], json.dumps(new_ua)[1:-1])
    registered = httpcloak.load_preset_from_json(blob)
    logger.info("httpcloak: registered %s (derived from %s)", registered, base)
    return registered


def replay_preset(preset: Optional[str]) -> Optional[str]:
    """The preset to replay a solve's clearance on: a derived ``chrome-<N>-windows`` the
    solver named is registered here first. Any other name passes through unchanged; so
    does this one when httpcloak cannot derive it (the fetch then fails as before)."""
    m = _DERIVABLE.fullmatch(preset or "")
    if not m:
        return preset
    try:
        return ensure_chrome_preset(int(m.group(1)))
    except Exception as exc:  # noqa: BLE001 — an old httpcloak without JSON presets
        logger.warning("could not derive httpcloak preset %s: %s", preset, exc)
        return preset
