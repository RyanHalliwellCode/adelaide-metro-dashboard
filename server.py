"""
Local server for the dashboard.

One background task polls the feed and caches it, so ten open tabs still only
cost the feed one request per cycle.

Run with:
    python -m uvicorn server:app --reload
then open http://127.0.0.1:8000/

(Use `python -m uvicorn` - a bare `uvicorn` often isn't on PATH.)
"""

import asyncio
import re
import sqlite3
import time
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from main import DEBUG_URL, fetch_debug_text, parse_vehicles

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "gtfs.db"

# The feed only republishes every ~15s, so polling faster is wasted bandwidth.
POLL_INTERVAL_SECONDS = 15

# Trains only for now - add "tram" and "bus" here to go network-wide.
LIVE_TYPES = ("train",)

# Tells us how old the data is, not just when we last asked for it.
HEADER_TIMESTAMP_RE = re.compile(r"timestamp:\s*(\d+)")


def parse_feed_timestamp(text: str) -> int | None:
    # Slice off the entities first or we'd match a vehicle's own timestamp.
    header_block = text.split("entity {", 1)[0]
    match = HEADER_TIMESTAMP_RE.search(header_block)
    return int(match.group(1)) if match else None


class FeedCache:
    """Last successful poll, plus whatever went wrong since."""

    def __init__(self) -> None:
        self.vehicles: list[dict] = []
        self.feed_timestamp: int | None = None
        self.updated_at: str | None = None
        # Monotonic so the countdown doesn't jump if the system clock changes.
        self.fetched_at: float | None = None
        self.error: str | None = None

    def store(self, vehicles: list[dict], feed_timestamp: int | None) -> None:
        self.vehicles = vehicles
        self.feed_timestamp = feed_timestamp
        self.updated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.fetched_at = time.monotonic()
        self.error = None

    def seconds_until_refresh(self) -> float:
        if self.fetched_at is None:
            return 0.0
        elapsed = time.monotonic() - self.fetched_at
        return max(0.0, POLL_INTERVAL_SECONDS - elapsed)

    def age_seconds(self) -> float | None:
        if self.fetched_at is None:
            return None
        return time.monotonic() - self.fetched_at


async def poll_feed(cache: FeedCache) -> None:
    """Refresh the cache forever, one fetch per POLL_INTERVAL_SECONDS."""
    while True:
        started = time.monotonic()
        try:
            # urllib blocks, so run it off the event loop or requests stall.
            text = await asyncio.to_thread(fetch_debug_text, DEBUG_URL)
            vehicles = parse_vehicles(text)
            cache.store(vehicles, parse_feed_timestamp(text))
            print(f"Polled feed: {len(vehicles)} vehicles at {cache.updated_at}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Keep the old snapshot - stale positions beat a blank map.
            cache.error = f"{type(exc).__name__}: {exc}"
            print(f"Feed poll failed: {cache.error}")

        # Subtract the fetch time so the cycle stays on a steady 15s beat.
        await asyncio.sleep(max(1.0, POLL_INTERVAL_SECONDS - (time.monotonic() - started)))


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = FeedCache()
    app.state.cache = cache
    task = asyncio.create_task(poll_feed(cache))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Adelaide Metro Dashboard", lifespan=lifespan)


@app.get("/api/snapshot")
async def api_snapshot() -> dict:
    """
    Every vehicle type, for the first render. The background poll already has
    fresh data for all of them, so serving the file on disk here would show
    positions from whenever main.py last ran - possibly hours ago.
    """
    cache: FeedCache = app.state.cache
    return {
        "vehicles": cache.vehicles,
        "count": len(cache.vehicles),
        "updated_at": cache.updated_at,
        "age_seconds": round(cache.age_seconds(), 1) if cache.age_seconds() is not None else None,
        "error": cache.error,
    }


@app.get("/api/vehicles")
async def api_vehicles() -> dict:
    """Live vehicles plus the timing info the client's countdown runs off."""
    cache: FeedCache = app.state.cache
    live = [v for v in cache.vehicles if v["type"] in LIVE_TYPES]
    return {
        "live_types": list(LIVE_TYPES),
        "vehicles": live,
        "count": len(live),
        "feed_timestamp": cache.feed_timestamp,
        "updated_at": cache.updated_at,
        "age_seconds": round(cache.age_seconds(), 1) if cache.age_seconds() is not None else None,
        "next_refresh_in": round(cache.seconds_until_refresh(), 1),
        "poll_interval": POLL_INTERVAL_SECONDS,
        "error": cache.error,
    }


# Not async on purpose - FastAPI runs sync endpoints in a threadpool, which
# keeps these blocking sqlite queries off the event loop for free.
@app.get("/api/trip/{trip_id}")
def api_trip(trip_id: str) -> dict:
    if not DB_PATH.exists():
        raise HTTPException(503, "gtfs.db missing - run 'python gtfs_static.py' first.")

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        trip = conn.execute(
            """SELECT t.route_id, t.trip_headsign, t.direction_id, t.shape_id,
                      r.route_long_name, r.route_short_name, r.route_type
               FROM trips t JOIN routes r ON r.route_id = t.route_id
               WHERE t.trip_id = ?""",
            (trip_id,),
        ).fetchone()
        if not trip:
            raise HTTPException(404, f"trip {trip_id} is not in the timetable")

        # The real path. Joining the stops with straight lines instead cuts
        # every corner and puts trains through paddocks.
        shape = conn.execute(
            """SELECT shape_pt_lat, shape_pt_lon FROM shapes WHERE shape_id = ?
               ORDER BY CAST(shape_pt_sequence AS INTEGER)""",
            (trip[3],),
        ).fetchall()

        stops = conn.execute(
            """SELECT st.stop_sequence, st.arrival_time, st.departure_time,
                      s.stop_name, s.stop_lat, s.stop_lon
               FROM stop_times st JOIN stops s ON s.stop_id = st.stop_id
               WHERE st.trip_id = ? ORDER BY CAST(st.stop_sequence AS INTEGER)""",
            (trip_id,),
        ).fetchall()
    finally:
        conn.close()

    # First stop it hasn't reached yet. GTFS writes after-midnight times as
    # 24:xx, which sort after everything, so a plain string compare is enough.
    now = datetime.now().strftime("%H:%M:%S")
    next_index = next((i for i, s in enumerate(stops) if (s[1] or s[2]) > now), None)

    route_id, headsign, direction, _shape_id, long_name, short_name, route_type = trip
    return {
        "trip_id": trip_id,
        "route_id": route_id,
        "route_name": long_name or short_name or route_id,
        "headsign": headsign,
        "direction_id": direction,
        "route_type": route_type,
        "next_stop_index": next_index,
        "now": now,
        "shape": [[float(lat), float(lon)] for lat, lon in shape],
        "stops": [
            {
                "sequence": seq,
                "arrival": arrival,
                "departure": departure,
                "name": name,
                "latitude": float(lat) if lat else None,
                "longitude": float(lon) if lon else None,
            }
            for seq, arrival, departure, name, lat, lon in stops
        ],
    }


# Explicit routes, not a static mount - a mount would also serve .git.
@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "map.html")


@app.get("/vehicles.json")
async def vehicles_snapshot() -> FileResponse:
    """The snapshot from main.py, used for the initial render."""
    return FileResponse(BASE_DIR / "vehicles.json")
