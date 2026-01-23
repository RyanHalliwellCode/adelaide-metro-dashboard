# Dumps the static GTFS feed (the timetable) into gtfs.db so we can query it.
#   python gtfs_static.py            build the db
#   python gtfs_static.py --refresh  re-download first
#   python gtfs_static.py --validate just check our route sets

import argparse
import csv
import io
import sqlite3
import urllib.request
import zipfile
from pathlib import Path

STATIC_URL = "https://gtfs.adelaidemetro.com.au/v1/static/latest/google_transit.zip"
ZIP_PATH = Path("gtfs_static.zip")
DB_PATH = Path("gtfs.db")

# 39 MB of route geometry we don't draw yet. Delete this to load it.
SKIP_FILES = {"shapes.txt"}

# route_type codes -> our names. 700s are just more buses, 4 is the ferry.
ROUTE_TYPES = {
    "0": "tram",
    "2": "train",
    "3": "bus",
    "4": "ferry",
    "700": "bus",
    "701": "bus",
    "712": "bus",
    "715": "bus",
}

# Without these, "stops on this trip" takes forever.
INDEXES = [
    ("trips", "route_id"),
    ("trips", "service_id"),
    ("stop_times", "trip_id"),
    ("stop_times", "stop_id"),
    ("routes", "route_type"),
]


def download(force: bool = False) -> None:
    if ZIP_PATH.exists() and not force:
        print(f"Using existing {ZIP_PATH} ({ZIP_PATH.stat().st_size / 1_048_576:.1f} MB)")
        return
    print(f"Downloading {STATIC_URL} ...")
    urllib.request.urlretrieve(STATIC_URL, ZIP_PATH)
    print(f"Saved {ZIP_PATH} ({ZIP_PATH.stat().st_size / 1_048_576:.1f} MB)")


def load_table(conn: sqlite3.Connection, zf: zipfile.ZipFile, filename: str) -> int:
    # One table per .txt file, named the same minus the extension.
    table = filename.removesuffix(".txt")

    # utf-8-sig strips the BOM, otherwise it sticks to the first column name.
    with zf.open(filename) as raw:
        reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
        columns = next(reader, None)
        if not columns:
            return 0

        # All TEXT - ids look numeric but "01234" would lose its leading zero.
        quoted = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" ({quoted})')

        placeholders = ", ".join("?" * len(columns))
        insert = f'INSERT INTO "{table}" VALUES ({placeholders})'

        # Some rows have trailing commas, and one bad row kills the whole batch.
        rows = ((row + [""] * len(columns))[: len(columns)] for row in reader)
        cursor = conn.executemany(insert, rows)
        return cursor.rowcount


def build(force_download: bool = False) -> None:
    download(force=force_download)

    conn = sqlite3.connect(DB_PATH)
    # Fine to be reckless here - if it dies we just rebuild.
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")

    with zipfile.ZipFile(ZIP_PATH) as zf:
        names = [n for n in zf.namelist() if n.endswith(".txt") and n not in SKIP_FILES]
        for name in sorted(names):
            count = load_table(conn, zf, name)
            print(f"  {name:22s} {count:>9,} rows")

    for table, column in INDEXES:
        conn.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{table}_{column}" ON "{table}" ("{column}")'
        )
    conn.commit()

    version = conn.execute(
        "SELECT feed_start_date, feed_end_date, feed_version FROM feed_info"
    ).fetchone()
    conn.close()
    print(f"\nBuilt {DB_PATH} ({DB_PATH.stat().st_size / 1_048_576:.1f} MB)")
    print(f"Feed version {version[2]}, valid {version[0]} to {version[1]}")


def validate() -> None:
    # Checks our hardcoded route sets against what the feed actually says.
    from main import RAIL_ROUTES, TRAM_ROUTES, FERRY_ROUTES, classify_vehicle

    if not DB_PATH.exists():
        print(f"{DB_PATH} not found - run 'python gtfs_static.py' first.")
        return

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT route_id, route_type FROM routes").fetchall()
    conn.close()

    truth = {rid: ROUTE_TYPES.get(rtype, "unknown") for rid, rtype in rows}
    hardcoded = {"train": RAIL_ROUTES, "tram": TRAM_ROUTES, "ferry": FERRY_ROUTES}

    problems = 0
    for expected, ids in hardcoded.items():
        for rid in sorted(ids):
            actual = truth.get(rid)
            if actual != expected:
                problems += 1
                print(f"  {rid}: we call it {expected}, feed says {actual or 'not in feed'}")

    # Other direction - catches a new train line we haven't added yet.
    for rid, actual in sorted(truth.items()):
        if actual in hardcoded and rid not in hardcoded[actual]:
            problems += 1
            print(f"  {rid}: feed says {actual}, missing from our list")

    mismatched = [r for r, t in truth.items() if classify_vehicle(r) != t]
    print(f"\n{len(truth)} routes checked, {len(mismatched)} classified differently.")
    print("All route sets match the feed." if not problems else f"{problems} problems above.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build gtfs.db from the static GTFS feed.")
    parser.add_argument("--refresh", action="store_true", help="force a fresh download")
    parser.add_argument("--validate", action="store_true", help="check route sets only")
    args = parser.parse_args()

    if args.validate:
        validate()
    else:
        build(force_download=args.refresh)
        print()
        validate()
