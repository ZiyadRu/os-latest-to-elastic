from typing import Dict
import json
import time
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict
import requests
from config import ES_URL, API_KEY_B64, RELEASE_INFO_URL, DEST_INDEX as MACOS_DEST_INDEX
# ---------------------------
# add near your imports
import re
from typing import Tuple

def _parse_version_parts(version: str) -> Tuple[int, int, int]:
    """
    Parse strings like '26.0.1', '15.7.1', '14.8' → (major, minor, patch).
    Missing parts default to 0.
    """
    m = re.match(r"^\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?\s*$", str(version))
    if not m:
        return (0, 0, 0)
    major = int(m.group(1))
    minor = int(m.group(2) or 0)
    patch = int(m.group(3) or 0)
    return major, minor, patch

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


def ship_macos_latest(
    latest_by_codename: Dict[str, str],
    *,
    dest_index: str | None = None,
    es_url: str | None = None,
    api_key_b64: str | None = None,
    refresh: str | bool | None = "wait_for",
    batch_size: int = 500,
    max_retries: int = 3,
    retry_backoff_sec: float = 1.0,
) -> None:
    """
    Upsert one document per maintained macOS codename into `dest_index`.

    - _id = codename (lowercased)
    - Adds updated_at and @timestamp (identical)
    - Adds integer fields: major, minor, patch
    """
    es_url = es_url or ES_URL
    api_key_b64 = api_key_b64 or API_KEY_B64
    dest_index = dest_index or MACOS_DEST_INDEX

    if not latest_by_codename:
        print("[INFO] Nothing to ship: latest_by_codename is empty.")
        return

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

    for codename, version in latest_by_codename.items():
        _id = str(codename).strip().lower()
        major, minor, patch = _parse_version_parts(version)

        doc_body = {
            "codename": _id,
            "latest_version": str(version).strip(),
            "major": major,
            "minor": minor,
            "patch": patch,
            "os": "macos",
            "source": RELEASE_INFO_URL,
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

    print(f"[DONE] Upserted {total} macOS doc(s) into '{dest_index}'. Failures: {total_failed}")
