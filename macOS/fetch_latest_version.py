import json
from urllib.request import Request, urlopen
from typing import Dict
from config import RELEASE_INFO_URL
#https://endoflife.date/api/v1/products/macos/

def get_maintained_macos_latest_by_codename() -> Dict[str, str]:
    """
    Returns a mapping like:
      {"tahoe": "26.0.1", "sequoia": "15.7.1", "sonoma": "14.8.1"}
    for all *maintained* macOS releases.
    """
    req = Request(RELEASE_INFO_URL, headers={"Accept": "application/json"})
    with urlopen(req, timeout=20) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Fetch failed: {resp.status} {resp.reason}")
        data = json.load(resp)

    releases = (data.get("result") or {}).get("releases") or []
    mapping: Dict[str, str] = {}

    for r in releases:
        if not r.get("isMaintained"):
            continue
        latest_name = (r.get("latest") or {}).get("name")
        codename = (r.get("codename") or "").strip()
        if not codename or not latest_name:
            continue

        key = codename.lower()  # normalize to "tahoe", "sequoia", "sonoma"
        if key not in mapping:   # de-dupe, preserve first-seen order
            mapping[key] = str(latest_name).strip()

    return mapping
