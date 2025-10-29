import datetime
import time
import re
from typing import Optional, List, Tuple, Dict
from typing import Dict, Tuple, Any, Optional
import os
import json
import time
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict

import requests
# If you keep per-OS indexes in config:
# from config import linux_dest_index as LINUX_DEST_INDEX
# Otherwise set a sane default here:
# --------------------------------

def _bulk_flush(
    actions: List[dict],
    docs: List[dict],
    es_url: str,
    api_key_b64: str,
    refresh: Optional[str],
    max_retries: int,
    retry_backoff_sec: float,
) -> Tuple[int, int]:
    """
    Sends one NDJSON bulk request.
    Returns (num_indexed_attempted, num_failed_items).
    Supports any bulk op (index/update/create/delete) in `actions`.
    """
    if not actions:
        return (0, 0)

    headers = {
        "Authorization": f"ApiKey {api_key_b64}",
        "Content-Type": "application/x-ndjson",
    }

    bulk_url = f"{es_url.rstrip('/')}/_bulk"
    if refresh is not None:
        bulk_url += f"?refresh={'true' if refresh is True else 'false' if refresh is False else refresh}"

    # Prepare NDJSON payload
    lines = []
    for meta, doc in zip(actions, docs):
        lines.append(json.dumps(meta, separators=(",", ":")))
        # For delete ops there is no source line; but we only send bodies for ops that expect them
        op = next(iter(meta))
        if op in ("index", "create", "update"):
            lines.append(json.dumps(doc, separators=(",", ":")))
    payload = "\n".join(lines) + "\n"

    # Retry transient issues (429/5xx)
    last_resp = None
    for attempt in range(1, max_retries + 1):
        resp = requests.post(bulk_url, data=payload, headers=headers, timeout=120)
        last_resp = resp
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            sleep_for = retry_backoff_sec * (2 ** (attempt - 1))
            print(f"[WARN] Bulk HTTP {resp.status_code} attempt {attempt}/{max_retries}; "
                  f"backing off {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue
        break

    if last_resp is None or not last_resp.ok:
        msg = f"Bulk failed: HTTP {getattr(last_resp, 'status_code', '???')} {getattr(last_resp, 'text', '')[:500]}"
        raise RuntimeError(msg)

    result = last_resp.json()
    failed = 0
    if result.get("errors"):
        for i, item in enumerate(result.get("items", [])):
            # item looks like {"update": {"_index":"...","_id":"...","status":200,...}}
            op = next(iter(item))
            ent = item.get(op, {})
            err = ent.get("error")
            if err:
                failed += 1
                if failed <= 10:
                    print(f"[ERROR] item #{i} failed: op={op} status={ent.get('status')} "
                          f"_id={ent.get('_id')} error={err}")
    else:
        took = result.get("took")
        print(f"[OK] Bulk sent {len(actions)} ops in {took} ms")

    # Count ops we attempted (one per meta)
    return (len(actions), failed)


def _parse_version_parts(version: str) -> Tuple[int, int, int]:
    """
    Parse versions like '25.10', '24.04.3', '22.04.5' -> (major, minor, patch).
    Missing parts default to 0. Extra parts are ignored.
    """
    m = re.match(r"^\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?", str(version))
    if not m:
        return (0, 0, 0)
    major = int(m.group(1))
    minor = int(m.group(2) or 0)
    patch = int(m.group(3) or 0)
    return major, minor, patch


def _infer_distro_name(payload: Dict[str, Any], fallback: Optional[str] = None) -> str:
    """
    Try to infer 'ubuntu' from source like '.../api/distribution/ubuntu'.
    """
    if fallback:
        return fallback.strip().lower()
    src = (payload.get("source") or "").strip().lower()
    m = re.search(r"/distribution/([^/?#]+)", src)
    return (m.group(1) if m else "unknown").lower()


def ship_linux_distribution_series(
    payload: Dict[str, Any],
    *,
    distro: Optional[str] = None,
    dest_index: Optional[str] = None,
    es_url: Optional[str] = None,
    api_key_b64: Optional[str] = None,
    refresh: Optional[str] = "wait_for",
    batch_size: int = 500,
    max_retries: int = 3,
    retry_backoff_sec: float = 1.0,
) -> None:
    """
    Upsert one document per (distro, series) from a payload like:

    {
      "source": "http://127.0.0.1:8000/api/distribution/ubuntu",
      "series": {
        "25": {"version": "25.10", "text": "...", "url": "..."},
        "24": {"version": "24.04.3", "text": "...", "url": "..."},
        "22": {"version": "22.04.5", "text": "...", "url": "..."}
      }
    }

    Fields written:
      - distro (e.g., "ubuntu")
      - series (int) e.g., 25
      - latest_version (string), major/minor/patch (ints)
      - text, announcement_url
      - source
      - updated_at, @timestamp  (identical)
    """
    from config import ES_URL, API_KEY_B64  # reuse your existing config

    es_url = es_url or ES_URL
    api_key_b64 = api_key_b64 or API_KEY_B64
    dest_index = dest_index 

    series_map = (payload or {}).get("series") or {}
    if not isinstance(series_map, dict) or not series_map:
        print("[INFO] Nothing to ship: 'series' is empty.")
        return

    distro_name = _infer_distro_name(payload, fallback=distro)
    source_url = payload.get("source")

    now_iso = datetime.now(timezone.utc).isoformat()
    actions, docs = [], []
    total = 0
    total_failed = 0

    def flush():
        nonlocal actions, docs, total, total_failed
        n_attempted, n_failed = _bulk_flush(
            actions, docs, es_url, api_key_b64, refresh, max_retries, retry_backoff_sec
        )
        total += n_attempted
        total_failed += n_failed
        actions.clear()
        docs.clear()

    # one UPDATE (upsert) per series
    for series_key, info in series_map.items():
        try:
            series_int = int(str(series_key).strip())
        except ValueError:
            # skip weird keys
            continue

        version = str((info or {}).get("version", "")).strip()
        text = (info or {}).get("text")
        ann_url = (info or {}).get("url")

        major, minor, patch = _parse_version_parts(version)

        _id = f"{distro_name}-{series_int}"  # ensures one doc per distro/series

        doc_body = {
            "distro": distro_name,
            "series": series_int,
            "latest_version": version,
            "major": major,
            "minor": minor,
            "patch": patch,
            "text": text,
            "announcement_url": ann_url,
            "source": source_url,
            "updated_at": now_iso,
            "@timestamp": now_iso,
        }

        meta = {"update": {"_index": dest_index, "_id": _id}}
        body = {"doc": doc_body, "doc_as_upsert": True, "detect_noop": True}

        actions.append(meta)
        docs.append(body)

        if len(actions) >= batch_size:
            flush()

    if actions:
        flush()

    print(f"[DONE] Upserted {total} linux doc(s) into '{dest_index}'. Failures: {total_failed}")
