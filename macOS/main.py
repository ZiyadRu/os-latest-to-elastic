from fetch_latest_version import get_maintained_macos_latest_by_codename
from shipper import ship_macos_latest
from config import DEST_INDEX

if __name__ == "__main__":
    latest = get_maintained_macos_latest_by_codename()
    ship_macos_latest(latest, dest_index=DEST_INDEX, refresh="wait_for")