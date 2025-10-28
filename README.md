# os-latest-to-elastic

Fetches the latest supported OS versions for **Windows 11** and **macOS**, then **upserts** them into **Elasticsearch** for dashboards and alerts.

- **Windows 11:** Scrapes Microsoft’s *Windows 11 release information* page to capture the latest build (e.g., `26200.6899`) per tracked **build prefix** (e.g., `26200`).  
- **macOS:** Calls the endoflife.date macOS API and maps maintained **codenames** → **latest version** (e.g., `tahoe → 26.0.1`).  
- Uses Elasticsearch **bulk update with `doc_as_upsert`** so there’s exactly **one doc per key** (Windows: build prefix; macOS: codename).

---

## Repo layout

```
os-latest-to-elastic/
├─ Windows/
│  ├─ config.py
│  ├─ main.py
│  ├─ scrape_latest_build.py
│  └─ shipper.py
├─ macOS/
│  ├─ config.py
│  ├─ fetch_latest_version.py
│  ├─ main.py
│  └─ shipper.py
└─ .gitignore
```

---

## What it ships

**Windows 11** (one doc per `build_prefix`):  
```json
{
  "_id": "26200",
  "build_prefix": 26200,
  "latest_ubr": 6899,
  "latest_build": "26200.6899",
  "os": "windows11",
  "source": "<RELEASE_INFO_URL>",
  "updated_at": "2025-10-28T00:00:00Z",
  "@timestamp": "2025-10-28T00:00:00Z"
}
```

**macOS** (one doc per `codename`):  
```json
{
  "_id": "tahoe",
  "codename": "tahoe",
  "latest_version": "26.0.1",
  "major": 26,
  "minor": 0,
  "patch": 1,
  "os": "macos",
  "source": "<RELEASE_INFO_URL>",
  "updated_at": "2025-10-28T00:00:00Z",
  "@timestamp": "2025-10-28T00:00:00Z"
}
```

---

## Quick start

### Prereqs
- Python **3.9+**
- Network access to Elasticsearch
- `pip install python-dotenv requests`

### Configure

Each module loads a **.env file in its own folder** (`Windows/.env`, `macOS/.env`). You can also use real env vars.

**Common** (both modules):
```
ES_URL=https://your-es:9200
API_KEY_B64=base64-id-colon-key   # value for Authorization: ApiKey <API_KEY_B64>
DEST_INDEX=os_latest_versions
```

**macOS**:
```
RELEASE_INFO_URL=https://endoflife.date/api/v1/products/macos
```

**Windows**:
```
RELEASE_INFO_URL=https://learn.microsoft.com/en-us/windows/release-health/windows11-release-information
SUPPORTED_BUILDS=(22621, 22631, 26100, 26200)   # track only these build prefixes
# each year one build prefix should be added and one build prefix should be removed manually
```

> `SUPPORTED_BUILDS` accepts simple comma/space-separated text (`22631, 26100`).

### Run

```bash
# macOS latest versions by codename → Elasticsearch
python macOS/main.py

# Windows 11 latest build per tracked build_prefix → Elasticsearch
python Windows/main.py
```

If `refresh="wait_for"` is kept (default), readers will see the changes after each bulk completes.

---

## Notes

- Bulk writes use `update` with `doc_as_upsert` + `detect_noop=true`.  
- Document `_id` is stable: **macOS** = `codename`, **Windows** = `build_prefix`.  
- Timestamps: both `updated_at` and `@timestamp` are set to the same UTC ISO time.
