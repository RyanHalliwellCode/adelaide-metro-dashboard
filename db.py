# Everything that touches gtfs.db lives here, so the rest of the project never
# opens a connection or hardcodes a query itself.

import math
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


# --- Which modes serve each stop ---------------------------------------------
# Worked out once and stored, because doing it per map pan costs 300 ms+ and
# the answer only changes when the timetable does. Platforms are rolled up into
# their parent station, so a station knows it's served by trains even though
# only its platforms carry the stop_times.

STOP_MODES_SQL = """
DROP TABLE IF EXISTS stop_modes;
CREATE TABLE stop_modes (stop_id TEXT PRIMARY KEY, modes TEXT);
"""


def build_stop_modes(conn: sqlite3.Connection) -> int:
    conn.executescript(STOP_MODES_SQL)
    rows = conn.execute(
        """SELECT COALESCE(NULLIF(s.parent_station,''), s.stop_id) AS anchor, r.route_type
           FROM stop_times st
           JOIN stops s ON s.stop_id = st.stop_id
           JOIN trips t ON t.trip_id = st.trip_id
           JOIN routes r ON r.route_id = t.route_id
           GROUP BY anchor, r.route_type"""
    ).fetchall()

    grouped: dict[str, set] = {}
    for anchor, route_type in rows:
        grouped.setdefault(anchor, set()).add(gtfs_type_name(route_type))
    conn.executemany(
        "INSERT OR REPLACE INTO stop_modes (stop_id, modes) VALUES (?, ?)",
        [(stop_id, ",".join(sorted(modes))) for stop_id, modes in grouped.items()],
    )
    conn.commit()
    return len(grouped)


def ensure_stop_modes() -> None:
    """Build the lookup if this database predates it."""
    with connect(writable=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_modes'"
        ).fetchone()
        if exists:
            return
        print("Building stop_modes lookup (one-off, a couple of seconds)...", flush=True)
        count = build_stop_modes(conn)
        print(f"stop_modes: {count:,} stops classified", flush=True)


# --- Walking between nearby stops --------------------------------------------
# Buses and trains never share a stop_id, not even at an interchange: Elizabeth
# Interchange and Elizabeth Railway Station are two records 50 m apart. Matching
# transfers on stop_id alone finds no journey at all from anywhere without rail,
# so changes have to be matched on distance.

TRANSFER_METRES = 350
WALK_SPEED_MPS = 1.3
GRID_CELL = 0.004  # about 450 m, so neighbours are always within one cell

WALK_SQL = """
DROP TABLE IF EXISTS walk_transfers;
CREATE TABLE walk_transfers (from_stop TEXT, to_stop TEXT, metres REAL);
CREATE INDEX idx_walk_from ON walk_transfers (from_stop);
"""


def _grid(stops: list[tuple]) -> tuple[dict, dict]:
    grid: dict[tuple, list] = {}
    coords = {}
    for stop_id, lat, lon in stops:
        lat, lon = float(lat), float(lon)
        coords[stop_id] = (lat, lon)
        grid.setdefault((int(lat / GRID_CELL), int(lon / GRID_CELL)), []).append(stop_id)
    return grid, coords


def metres_between(a: tuple, b: tuple) -> float:
    return math.hypot(
        (a[0] - b[0]) * 111320,
        (a[1] - b[1]) * 111320 * math.cos(math.radians(a[0])),
    )


def build_walk_transfers(conn: sqlite3.Connection) -> int:
    conn.executescript(WALK_SQL)
    stops = conn.execute("SELECT stop_id, stop_lat, stop_lon FROM stops WHERE stop_lat != ''").fetchall()
    grid, coords = _grid(stops)

    rows = []
    for stop_id, here in coords.items():
        gy, gx = int(here[0] / GRID_CELL), int(here[1] / GRID_CELL)
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                for other in grid.get((gy + dy, gx + dx), ()):
                    if other == stop_id:
                        continue
                    distance = metres_between(here, coords[other])
                    if distance <= TRANSFER_METRES:
                        rows.append((stop_id, other, round(distance, 1)))

    conn.executemany("INSERT INTO walk_transfers VALUES (?, ?, ?)", rows)
    conn.commit()
    return len(rows)


def ensure_walk_transfers() -> None:
    with connect(writable=True) as conn:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='walk_transfers'"
        ).fetchone():
            return
        print("Building walk_transfers lookup (one-off)...", flush=True)
        print(f"walk_transfers: {build_walk_transfers(conn):,} pairs within {TRANSFER_METRES} m", flush=True)


def stops_near(conn: sqlite3.Connection, lat: float, lon: float, radius: float) -> list[dict]:
    """Stops you could walk to from a point, nearest first."""
    span = radius / 111320
    rows = conn.execute(
        """SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, m.modes
           FROM stops s LEFT JOIN stop_modes m ON m.stop_id = s.stop_id
           WHERE s.parent_station = ''
             AND CAST(s.stop_lat AS REAL) BETWEEN ? AND ?
             AND CAST(s.stop_lon AS REAL) BETWEEN ? AND ?""",
        (lat - span, lat + span, lon - span * 1.3, lon + span * 1.3),
    ).fetchall()

    near = []
    for stop_id, name, slat, slon, modes in rows:
        distance = metres_between((lat, lon), (float(slat), float(slon)))
        if distance <= radius:
            near.append({
                "stop_id": stop_id,
                "name": name,
                "latitude": float(slat),
                "longitude": float(slon),
                "metres": round(distance),
                "walk_seconds": int(distance / WALK_SPEED_MPS),
                "modes": modes.split(",") if modes else [],
            })
    near.sort(key=lambda s: s["metres"])
    return near


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
