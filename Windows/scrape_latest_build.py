import sys
import requests
import sys
import re
import html
from config import RELEASE_INFO_URL, SUPPORTED_BUILDS

# Scrape the table on Microsoft's 'Windows 11 release information' page.
# Returns { build_prefix:int -> latest_ubr:int }, e.g. {22631: 6060, 26100: 6899, 26200: 6899}.

def fetch_ms_latest_builds():
    
    try:
        r = requests.get(RELEASE_INFO_URL, timeout=30)
        r.raise_for_status()
        html_text = r.text
    except requests.RequestException as e:
        print(f" Failed to fetch Microsoft page: {e}", file=sys.stderr)
        sys.exit(1)

    # Grab all tables
    tables = re.findall(r"<table.*?>.*?</table>", html_text, flags=re.I | re.S)
    latest_by_build = {}

    for tbl in tables:
        # Extract headers
        headers = re.findall(r"<th[^>]*>(.*?)</th>", tbl, flags=re.I | re.S)
        headers = [re.sub(r"<.*?>", "", html.unescape(h)).strip() for h in headers]
        if not headers:
            continue

        # We want the table that has both 'Version' and 'Latest build'
        try:
            v_idx = [h.lower() for h in headers].index("version")
            lb_idx = next(i for i, h in enumerate(headers) if "latest build" in h.lower())
        except (ValueError, StopIteration):
            continue  # not our table

        # Walk rows
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, flags=re.I | re.S)
        for row in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.I | re.S)
            if not cells or len(cells) <= max(v_idx, lb_idx):
                continue
            # Clean cell text
            cells = [re.sub(r"<.*?>", "", html.unescape(c)).strip() for c in cells]
            version_txt = cells[v_idx]          # e.g., "25H2"
            latest_build_txt = cells[lb_idx]    # e.g., "26200.6899"

            m = re.search(r"(\d{5})\.(\d+)", latest_build_txt)
            if not m:
                continue
            build_prefix = int(m.group(1))
            ubr = int(m.group(2))

            # Only keep Windows 11 lines we care about
            if build_prefix in SUPPORTED_BUILDS:
                latest_by_build[build_prefix] = max(ubr, latest_by_build.get(build_prefix, 0))

    if not latest_by_build:
        print(" Could not parse 'Latest build' from Microsoft table.", file=sys.stderr)
        sys.exit(1)

    return latest_by_build
