#!/usr/bin/env python3
"""
Generic Distrowatch/Diwa fetcher:
- Fetch "Distribution Release" items for any distro defined in DISTROS
- Keep the latest version per major (optionally restrict to certain minors)
- Write a compact snapshot JSON: { "source": ..., "series": { "<major>": {version, text, url} } }
USAGE
  python3 distro_releases.py                      # default distro: ubuntu
  DIWA_DISTRO=ubuntu python3 distro_releases.py   # pick by key
  DIWA_BASE=http://127.0.0.1:8000/api/distribution OUTFILE=ubuntu_releases.json python3 distro_releases.py
TO ADD A NEW DISTRO
  1) Add a new entry in DISTROS (see the examples)
  2) Run with DIWA_DISTRO=<your_key>
"""

import json
import os
import re
import sys
from urllib.request import urlopen
from urllib.error import URLError, HTTPError


# =========================
# HELPERS (rarely change)
# =========================

def version_key(v: str, width: int = 4):
    """Normalize versions for comparison (handles 1–4+ numeric segments)."""
    parts = []
    for x in v.split("."):
        try:
            parts.append(int(x))
        except ValueError:
            parts.append(0)
    if len(parts) < width:
        parts += [0] * (width - len(parts))
    return tuple(parts[:width])

def _safe_get_news_list(payload):
    """Diwa key name can vary; support a few likely variants."""
    for key in (
        "recent_related_news_and_releases",
        "recent related news and releases",
        "recent_news_and_releases",
    ):
        if isinstance(payload, dict) and key in payload and isinstance(payload[key], list):
            return payload[key]
    return []

def _build_release_regex(distro_title: str):
    """
    Match titles like:
      'Distribution Release: <Title> <version> ...'
    Capture version with 0–3 dot segments so it works for:
      '22', '22.1', '24.04', '24.04.3', '25.04.1'
    """
    name = re.escape(distro_title.strip())
    return re.compile(
        rf'^Distribution Release:\s*{name}\s+([0-9]{{1,3}}(?:\.[0-9]+){{0,3}})\b',
        re.I,
    )

def _allowed_for_major(version: str, major: str, allowed_prefixes: dict[str, tuple]) -> bool:
    """
    If the major is listed in allowed_prefixes, only versions starting with
    any of its prefixes are accepted; otherwise everything is allowed.
    """
    pfx = allowed_prefixes.get(major)
    if not pfx:
        return True
    return any(version.startswith(x) for x in pfx)

def _save_snapshot(snapshot: dict, outfile: str):
    outdir = os.path.dirname(os.path.abspath(outfile)) or "."
    os.makedirs(outdir, exist_ok=True)
    tmp = outfile + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
    os.replace(tmp, outfile)
    print(f"[OK] wrote {outfile}")


# ===========================================
# DISTROS CONFIG — ADD NEW ONES HERE (EASY)
# ===========================================
# Each entry defines how to parse & filter releases for a distro.
# - slug: path segment in your Diwa API (e.g., /api/distribution/<slug>)
# - title: exact headline name used by DistroWatch
# - target_majors: set of majors to track; None = track all majors found
# - allowed_prefixes: optional per-major constraints (e.g., LTS pins)
# - version_regex: optional custom regex; omit to use default builder

DISTROS = {
    # Ubuntu: track 25,24,22; pin LTS lines for 24 & 22; 25 unrestricted
    "ubuntu": {
        "slug": "ubuntu",
        "title": "Ubuntu",
        "target_majors": set(
            (os.environ.get("DIWA_MAJORS") or "25,24,22").replace(" ", "").split(",")
        ),
        "allowed_prefixes": {
            "24": ("24.04",),
            "22": ("22.04",),
            # Add "26": ("26.04",) when 26.04 LTS lands
        },
        # version_regex: default (_build_release_regex("Ubuntu"))
    },

    # Parrot (example): track all majors, no restrictions
    "parrot": {
        "slug": "parrot",
        "title": "Parrot",
        "target_majors": None,
        "allowed_prefixes": {},
    },

    # Linux Mint: versions can be "22", "22.1", "22.2" ... default regex handles both
    "mint": {
        "slug": "mint",               # adjust to your Diwa slug if needed: "linux_mint" or "linux-mint"
        "title": "Linux Mint",
        "target_majors": None,
        "allowed_prefixes": {},
    },

    # Fedora (example): track all; no restrictions
    "fedora": {
        "slug": "fedora",
        "title": "Fedora",
        "target_majors": None,
        "allowed_prefixes": {},
    },
}


# =========================
# GENERIC FETCHER
# =========================

def fetch_latest_for_distro(
    diwa_base: str,
    distro_cfg: dict,
    timeout_sec: int = 20,
):
    slug   = distro_cfg["slug"]
    title  = distro_cfg["title"]
    target = distro_cfg.get("target_majors")  # set[str] or None
    allow  = distro_cfg.get("allowed_prefixes", {})
    regex  = distro_cfg.get("version_regex") or _build_release_regex(title)

    endpoint = f"{diwa_base.rstrip('/')}/{slug}"

    try:
        with urlopen(endpoint, timeout=timeout_sec) as r:
            raw = r.read().decode("utf-8", "replace")
        payload = json.loads(raw)
    except (URLError, HTTPError, json.JSONDecodeError) as e:
        print(f"[ERR] fetch failed from {endpoint}: {e}", file=sys.stderr)
        return None

    items = _safe_get_news_list(payload)
    latest_by_major = {}

    for it in items:
        if not isinstance(it, dict):
            continue
        text, url = it.get("text"), it.get("url")
        if not text or not url:
            continue

        m = regex.match(text.strip())
        if not m:
            # Uncomment for debugging if needed:
            # if text.lower().startswith("distribution release:"):
            #     print("[DBG] unmatched title:", text)
            continue

        ver = m.group(1)             # e.g., "24.04.3", "25.10", "22"
        major = ver.split(".", 1)[0]  # "24", "25", "22"

        if (target is not None) and (major not in target):
            continue
        if not _allowed_for_major(ver, major, allow):
            continue

        cur = latest_by_major.get(major)
        if (not cur) or (version_key(ver) > version_key(cur["version"])):
            latest_by_major[major] = {"version": ver, "text": text, "url": url}

    return {"source": endpoint, "series": latest_by_major}


# =========================
# MAIN
# =========================

def main():
    DIWA_BASE   = os.environ.get("DIWA_BASE", "http://127.0.0.1:8000/api/distribution")
    DISTRO_KEY  = os.environ.get("DIWA_DISTRO", "ubuntu").lower()
    OUTFILE     = os.environ.get("OUTFILE", f"{DISTRO_KEY}_releases.json")

    if DISTRO_KEY not in DISTROS:
        print(f"[ERR] unknown DIWA_DISTRO='{DISTRO_KEY}'. Known: {', '.join(sorted(DISTROS))}")
        sys.exit(2)

    snap = fetch_latest_for_distro(DIWA_BASE, DISTROS[DISTRO_KEY])
    if snap is None:
        sys.exit(1)
    if not snap.get("series"):
        print("[WARN] empty series; not writing file (upstream format may have changed)")
        sys.exit(0)

    _save_snapshot(snap, OUTFILE)

if __name__ == "__main__":
    main()