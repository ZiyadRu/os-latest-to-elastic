# config.py
from pathlib import Path
from dotenv import load_dotenv
import os, re, ast

# Always resolve path so it works no matter where you run from
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)  # set override=True if you want .env to win over OS env

def int_set_env(name: str, default: set[int] | None = None) -> set[int]:
    raw = os.getenv(name)
    if not raw:
        return set() if default is None else set(default)

    # Strip surrounding quotes
    s = raw.strip().strip('"').strip("'")

    # Try Python literal formats first: "(1,2)", "[1,2]", "{1,2}"
    try:
        lit = ast.literal_eval(s)
        if isinstance(lit, (list, tuple, set, frozenset)):
            return {int(x) for x in lit}
    except Exception:
        pass

    # Fallback: split on commas/whitespace: "1, 2  3"
    parts = re.split(r"[,\s]+", s.strip("()[]{} "))
    return {int(p) for p in parts if p}

ES_URL = (os.getenv("ES_URL"))
SOURCE_INDEX = os.getenv("SOURCE_INDEX")
API_KEY_B64 = os.getenv("API_KEY_B64")

RELEASE_INFO_URL = os.getenv("RELEASE_INFO_URL")
DEST_INDEX = os.getenv("DEST_INDEX")
SUPPORTED_BUILDS: set[int] = int_set_env("SUPPORTED_BUILDS")
