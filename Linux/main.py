from shipper import ship_linux_distribution_series
import json
from config import API_KEY_B64, DEST_INDEX, ES_URL
if __name__ == "__main__":
    # Suppose you loaded your JSON into `payload` (dict) already:
    with open("Linux/mint_releases.json", "r") as f:
        payload = json.load(f)
    ship_linux_distribution_series(payload, api_key_b64=API_KEY_B64, es_url=ES_URL, dest_index=DEST_INDEX)  # index: linux_latest_version
