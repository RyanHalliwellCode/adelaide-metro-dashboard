# Everything that touches gtfs.db lives here, so the rest of the project never
# opens a connection or hardcodes a query itself.

import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "gtfs.db"

# GTFS route_type codes. Anything unlisted is a bus variant - 700, 701, 712.
GTFS_TYPE_NAMES = {"0": "tram", "2": "train", "3": "bus", "4": "ferry"}
SCHOOL_ROUTE_TYPE = "712"

WEEKDAY_COLUMNS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

# Recorded positions are only useful for recent history, and they add up fast.
RETENTION_DAYS = 7


class MissingDatabase(RuntimeError):
    pass


def connect(writable: bool = False) -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise MissingDatabase("gtfs.db missing - run 'python gtfs_static.py' first.")
    if not writable:
        return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn = sqlite3.connect(DB_PATH)
    # WAL so the recorder writing doesn't block requests reading.
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def gtfs_type_name(route_type: str) -> str:
    return GTFS_TYPE_NAMES.get(route_type, "bus")


def route_types() -> dict[str, str]:
    """
    route_id -> train/tram/bus/ferry, straight from the feed's own route_type.
    This replaced a hardcoded list of route IDs that had to be kept in sync by
    hand; the timetable already knows, so it should be the one to say.
    """
    with connect() as conn:
        return {
            route_id: gtfs_type_name(route_type)
            for route_id, route_type in conn.execute("SELECT route_id, route_type FROM routes")
        }


def services_on(conn: sqlite3.Connection, day: date) -> set[str]:
    """
    Which service_ids run on a date. Without this you get every timetable
    stacked together - weekday, Saturday, Sunday and school services at once.
    """
    stamp = day.strftime("%Y%m%d")
    active = {
        row[0]
        for row in conn.execute(
            f"SELECT service_id FROM calendar"
            f" WHERE {WEEKDAY_COLUMNS[day.weekday()]}='1' AND start_date<=? AND end_date>=?",
            (stamp, stamp),
        )
    }
    # calendar_dates overrides the weekly pattern: 1 adds a day, 2 removes it.
    for service_id, exception in conn.execute(
        "SELECT service_id, exception_type FROM calendar_dates WHERE date=?", (stamp,)
    ):
        if exception == "1":
            active.add(service_id)
        else:
            active.discard(service_id)
    return active


# --- Recorded live positions -------------------------------------------------
# The realtime feed used to be written to a JSON file that went stale the moment
# it was created. Positions now land in the same database as the timetable, so
# there is one store, and so the history is there to compare against schedule.

SCHEMA = """
CREATE TABLE IF NOT EXISTS vehicle_positions (
    recorded_at    TEXT NOT NULL,
    feed_timestamp INTEGER,
    vehicle_id     TEXT,
    trip_id        TEXT,
    route_id       TEXT,
    latitude       REAL,
    longitude      REAL,
    bearing        REAL,
    speed          REAL
);
CREATE INDEX IF NOT EXISTS idx_positions_recorded ON vehicle_positions (recorded_at);
CREATE INDEX IF NOT EXISTS idx_positions_trip ON vehicle_positions (trip_id);
"""


def prepare_recorder() -> None:
    """Create the table and drop anything past the retention window."""
    with connect(writable=True) as conn:
        conn.executescript(SCHEMA)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
        removed = conn.execute(
            "DELETE FROM vehicle_positions WHERE recorded_at < ?", (cutoff,)
        ).rowcount
        conn.commit()
    if removed:
        print(f"Dropped {removed:,} position records older than {RETENTION_DAYS} days", flush=True)


def record_positions(vehicles: list[dict], feed_timestamp: int | None) -> None:
    # Opens its own connection because this runs on a worker thread, and sqlite
    # refuses to use a connection created on a different one.
    recorded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with connect(writable=True) as conn:
        conn.executemany(
            """INSERT INTO vehicle_positions
               (recorded_at, feed_timestamp, vehicle_id, trip_id, route_id,
                latitude, longitude, bearing, speed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    recorded_at,
                    feed_timestamp,
                    v["vehicle_id"],
                    v["trip_id"],
                    v["route_id"],
                    v["latitude"],
                    v["longitude"],
                    v["bearing"],
                    v["speed"],
                )
                for v in vehicles
            ],
        )
        conn.commit()
