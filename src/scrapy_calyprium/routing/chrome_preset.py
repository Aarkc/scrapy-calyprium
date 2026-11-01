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

``replay_preset`` also makes a replay present the Chrome its user agent names, as
mimic's ``chrome_preset.replay_preset`` does.
"""
import json
import logging
import re
import threading
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Chromium components/embedder_support/user_agent_utils.cc (GenerateBrandVersionList)
_GREASE_CHARS = [" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_"]
_GREASE_VERSIONS = ["8", "99", "24"]
_ORDERS = [(0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0)]
_DERIVABLE = re.compile(r"chrome-(\d+)-(windows|linux|macos)")


def sec_ch_ua(major: int) -> str:
    """The sec-ch-ua header Chrome ``major`` sends."""
    grease = f"Not{_GREASE_CHARS[major % 11]}A{_GREASE_CHARS[(major + 1) % 11]}Brand"
    brands = [(grease, _GREASE_VERSIONS[major % 3]), ("Chromium", str(major)), ("Google Chrome", str(major))]
    out = [None, None, None]
    for i, pos in enumerate(_ORDERS[major % 6]):
        out[pos] = brands[i]
    return ", ".join(f'"{b}";v="{v}"' for b, v in out)


# Chrome lists JPEG XL among the types it accepts again: a real Chrome 155's navigation and image requests carry
# image/jxl, a Chrome 153's did not (2026-10-21; 154 not seen).
JXL_SINCE = 155


def accept_document(major: int) -> str:
    """The Accept header of a navigation by Chrome ``major``."""
    jxl = "image/jxl," if major >= JXL_SINCE else ""
    return f"text/html,application/xhtml+xml,application/xml;q=0.9,{jxl}image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"


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


def ua_platform(user_agent: Optional[str]) -> Optional[str]:
    """The httpcloak preset platform a desktop Chrome ``user_agent`` runs on: windows, linux or
    macos; None for anything else (a phone, a tablet, no user agent)."""
    ua = user_agent or ""
    if "Windows NT" in ua:
        return "windows"
    if "Android" in ua or "Mobile" in ua:
        return None
    if "X11; Linux" in ua or "X11; Ubuntu" in ua:
        return "linux"
    if "Macintosh" in ua:
        return "macos"
    return None


def _base_platform_preset(httpcloak, platform: str) -> str:
    """Newest bundled chrome-<N>-<platform> preset name."""
    names = [n for n in httpcloak.available_presets() if re.fullmatch(rf"chrome-\d+-{platform}", n)]
    return max(names, key=lambda n: int(n.split("-")[1]))


def ensure_platform_preset(major: int, platform: str) -> str:
    """``chrome-<major>-<platform>``, registered (derived from the newest bundled preset of that platform, as
    ensure_chrome_preset does for Windows) when httpcloak lacks it. Mimic's browsers solve as a Linux Chrome
    (patchright, Chromium): their clearance replayed on httpcloak's newest Linux preset sent Chrome 152's
    client hints beside a Chrome/156 user agent (lowes.com, 2026-11-01)."""
    if platform == "windows":
        return ensure_chrome_preset(major)
    import httpcloak  # noqa: WPS433

    name = f"chrome-{major}-{platform}"
    if name in httpcloak.available_presets():
        return name
    base = _base_platform_preset(httpcloak, platform)
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
        elif k.lower() == "accept":
            headers["values"][k] = accept_document(major)
    for entry in headers.get("order", []):
        if entry.get("key", "").lower() == "sec-ch-ua":
            entry["value"] = new_ua
        elif entry.get("key", "").lower() == "accept":
            entry["value"] = accept_document(major)
    preset["name"] = name
    blob = json.dumps(spec)
    if old_ua in blob:  # anything else still advertising the base version
        blob = blob.replace(json.dumps(old_ua)[1:-1], json.dumps(new_ua)[1:-1])
    registered = httpcloak.load_preset_from_json(blob)
    logger.info("httpcloak: registered %s (derived from %s)", registered, base)
    return registered


_UA_CHROME_RE = re.compile(r"Chrome/(\d+)\.")
_PRESET_RE = re.compile(r"chrome-(\d+|latest)(?:-(\w+))?")
# Chromium browsers whose sec-ch-ua names another brand: a chrome preset cannot present them.
_OTHER_CHROMIUM = ("Edg/", "OPR/", "YaBrowser/", "SamsungBrowser/")

# (named preset, (the user agent's Chrome major, platform) or None) -> the preset replay_preset answered. Presets
# registered in a process stay registered, so an answer holds for its life; a failure is not kept.
_REPLAY_PRESETS: dict = {}


def _ua_target(preset: str, user_agent: Optional[str]) -> Optional[Tuple[int, str]]:
    """(Chrome major, platform) a replay on ``preset`` should present: the desktop Chrome ``user_agent``'s, when
    ``preset`` is a Chrome preset of that platform or of none; else None."""
    pm = _PRESET_RE.fullmatch(preset)
    um = _UA_CHROME_RE.search(user_agent or "")
    platform = ua_platform(user_agent)
    if not pm or not um or not platform or any(b in user_agent for b in _OTHER_CHROMIUM):
        return None
    if pm.group(2) not in (None, platform):
        return None
    return int(um.group(1)), platform


def _replay_name(preset: str, target: Optional[Tuple[int, str]]) -> str:
    """replay_preset's answer for ``preset`` and ``target`` (None: the preset's own Chrome)."""
    if target is not None:
        import httpcloak  # noqa: WPS433

        want, platform = target
        pm = _PRESET_RE.fullmatch(preset)
        base = _base_preset(httpcloak) if platform == "windows" else _base_platform_preset(httpcloak, platform)
        have = int(base.split("-")[1]) if pm.group(1) == "latest" else int(pm.group(1))
        if have == want and pm.group(2) == platform and preset != f"chrome-{want}-{platform}":
            return preset  # the alias of this platform's newest preset, which presents that Chrome
        # chrome-<N>-<platform> itself too: a derived one a solver named is registered here first. A preset of no
        # platform ("chrome-latest" is httpcloak's Linux one) gets the user agent's.
        return ensure_chrome_preset(want) if platform == "windows" else ensure_platform_preset(want, platform)
    m = _DERIVABLE.fullmatch(preset)
    return ensure_platform_preset(int(m.group(1)), m.group(2)) if m else preset


def replay_preset(preset: Optional[str], user_agent: Optional[str] = None) -> Optional[str]:
    """The preset to replay a solve's clearance on, presenting the Chrome that earned it.

    * For a Windows Chrome ``user_agent`` of major N, a Chrome preset of another major becomes
      ``chrome-<N>-windows`` (derived when httpcloak lacks it). httpcloak sends its preset's own
      sec-ch-ua and Accept and only the User-Agent is overridden: Tessera's Cloudflare clearance
      named ``chrome-latest-windows`` (Chrome 152 in httpcloak 1.7.2) for its Chrome/155 user
      agent, so its replay sent Chrome 152's client hints beside a Chrome 155 user agent, and 9 of
      11 such replays were refused. Mimic's ``chrome_preset.replay_preset`` does the same.
    * The same for a Linux or macOS Chrome ``user_agent``: ``chrome-<N>-linux`` / ``-macos``. Mimic's
      browsers solve as a Linux Chrome; replayed on httpcloak's newest Linux preset, a Chrome/156 clearance
      went out with Chrome 152's client hints (lowes.com, 2026-11-01).
    * A derived ``chrome-<N>-<platform>`` a solver named is registered first: httpcloak refuses a
      name it does not know.

    Anything else passes through (no user agent, another browser or platform, a preset of the
    same major, a non-Windows preset), as does ``preset`` when httpcloak cannot derive the one
    wanted (the fetch then runs on the named preset, as before)."""
    if not preset:
        return preset
    target = _ua_target(preset, user_agent)
    if target is None and not _DERIVABLE.fullmatch(preset):
        return preset
    known = _REPLAY_PRESETS.get((preset, target))
    if known:
        return known
    try:
        name = _replay_name(preset, target)
    except Exception as exc:  # noqa: BLE001 — an old httpcloak without JSON presets
        logger.warning("could not derive an httpcloak preset for %s (Chrome %s): %s", preset, target, exc)
        return preset
    _REPLAY_PRESETS[(preset, target)] = name  # every replay asks: answer from memory once it is known
    return name
