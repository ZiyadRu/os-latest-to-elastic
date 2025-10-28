from scrape_latest_build import fetch_ms_latest_builds
from shipper import ship_latest_builds
from config import DEST_INDEX

if __name__ == "__main__":
    
    latest = fetch_ms_latest_builds()
    ship_latest_builds(latest, dest_index=DEST_INDEX, refresh="wait_for")
    