# Dumps the static GTFS feed (the timetable) into gtfs.db so we can query it.
#   python gtfs_static.py            build the db
#   python gtfs_static.py --refresh  re-download first
#
# Only tables named after a GTFS .txt file are dropped and rebuilt, so the
# vehicle_positions the server records survive a refresh.

import argparse
import csv
import io
import sqlite3
import urllib.request
import zipfile

from db import DB_PATH

STATIC_URL = "https://gtfs.adelaidemetro.com.au/v1/static/latest/google_transit.zip"
ZIP_PATH = DB_PATH.parent / "gtfs_static.zip"

# Nothing skipped now - shapes.txt is big but it's the only thing that knows
# where the tracks actually go. Straight lines between stops cut every corner.
SKIP_FILES = set()

# Without these, "stops on this trip" takes forever. The trip_id and route_id
# ones matter most: joining stop_times -> trips -> routes without them scans
# all 26,535 trips per row, which turned one stop lookup into 4.4 seconds.
INDEXES = [
    ("trips", "trip_id"),
    ("trips", "route_id"),
    ("trips", "service_id"),
    ("trips", "shape_id"),
    ("stop_times", "trip_id"),
    ("stop_times", "stop_id"),
    ("stops", "stop_id"),
    ("routes", "route_id"),
    ("routes", "route_type"),
    ("shapes", "shape_id"),
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build gtfs.db from the static GTFS feed.")
    parser.add_argument("--refresh", action="store_true", help="force a fresh download")
    args = parser.parse_args()
    build(force_download=args.refresh)
