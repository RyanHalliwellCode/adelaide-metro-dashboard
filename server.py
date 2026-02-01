"""
Local server for the dashboard.

One background task polls the feed, caches it in memory for serving and records
it to gtfs.db, so ten open tabs still cost the feed one request per cycle.

Run with:
    python -m uvicorn server:app --reload
then open http://127.0.0.1:8000/

(Use `python -m uvicorn` - a bare `uvicorn` often isn't on PATH.)
"""

import asyncio
import math
import time
from contextlib import asynccontextmanager, suppress
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

import db
from main import fetch_debug_text, parse_feed_timestamp, parse_vehicles

BASE_DIR = Path(__file__).parent

# The feed only republishes every ~15s, so polling faster is wasted bandwidth.
POLL_INTERVAL_SECONDS = 15

# Stop hammering the feed when nobody's watching. Every 15s round the clock is
# ~5,700 requests and well over a gigabyte a day, almost all of it for an empty
# room. Polling resumes the moment a page asks for data again.
IDLE_AFTER_SECONDS = 90

# Treat a vehicle within this of a stop as sitting at it. Platforms are long and
# GPS wanders, so anything tighter misses vehicles that are plainly there.
AT_STOP_METRES = 120


class FeedCache:
    """Last successful poll, plus whatever went wrong since."""

    def __init__(self) -> None:
        self.vehicles: list[dict] = []
        self.feed_timestamp: int | None = None
        self.updated_at: str | None = None
        # Monotonic so the countdown doesn't jump if the system clock changes.
        self.fetched_at: float | None = None
        self.error: str | None = None
        # Starts "active" so the first poll runs before anyone opens the page.
        self.last_request_at: float = time.monotonic()

    def touch(self) -> None:
        self.last_request_at = time.monotonic()

    def has_listeners(self) -> bool:
        return (time.monotonic() - self.last_request_at) < IDLE_AFTER_SECONDS

    def store(self, vehicles: list[dict], feed_timestamp: int | None) -> None:
        self.vehicles = vehicles
        self.feed_timestamp = feed_timestamp
        self.updated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self.fetched_at = time.monotonic()
        self.error = None

    def age_seconds(self) -> float | None:
        if self.fetched_at is None:
            return None
        return time.monotonic() - self.fetched_at

    def seconds_until_refresh(self) -> float:
        age = self.age_seconds()
        return POLL_INTERVAL_SECONDS if age is None else max(0.0, POLL_INTERVAL_SECONDS - age)

    def timing(self) -> dict:
        age = self.age_seconds()
        return {
            "updated_at": self.updated_at,
            "age_seconds": round(age, 1) if age is not None else None,
            "next_refresh_in": round(self.seconds_until_refresh(), 1),
            "poll_interval": POLL_INTERVAL_SECONDS,
            "feed_timestamp": self.feed_timestamp,
            "error": self.error,
        }


async def poll_feed(cache: FeedCache, route_types: dict[str, str], recording: bool) -> None:
    """Refresh the cache forever, one fetch per POLL_INTERVAL_SECONDS."""
    idle_logged = False
    while True:
        started = time.monotonic()

        # flush because stdout is block-buffered when piped to a file, and a
        # server log that only appears 8 KB at a time is no use for watching.
        if not cache.has_listeners():
            if not idle_logged:
                print("No clients - pausing feed polling until someone asks.", flush=True)
                idle_logged = True
            await asyncio.sleep(1)
            continue
        if idle_logged:
            print("Client back - resuming feed polling.", flush=True)
            idle_logged = False

        try:
            # urllib blocks, so run it off the event loop or requests stall.
            text = await asyncio.to_thread(fetch_debug_text)
            vehicles = parse_vehicles(text, route_types)
            feed_timestamp = parse_feed_timestamp(text)
            cache.store(vehicles, feed_timestamp)
            print(f"Polled feed: {len(vehicles)} vehicles at {cache.updated_at}", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Keep the old snapshot - stale positions beat a blank map.
            cache.error = f"{type(exc).__name__}: {exc}"
            print(f"Feed poll failed: {cache.error}", flush=True)

        # Recording is history, not the live map, so its own try/except - a
        # write failure must never stop positions being served.
        if recording and cache.vehicles:
            try:
                await asyncio.to_thread(db.record_positions, cache.vehicles, cache.feed_timestamp)
            except Exception as exc:
                print(f"Recording positions failed: {type(exc).__name__}: {exc}", flush=True)

        # Subtract the fetch time so the cycle stays on a steady 15s beat.
        await asyncio.sleep(max(1.0, POLL_INTERVAL_SECONDS - (time.monotonic() - started)))


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = FeedCache()
    app.state.cache = cache

    # Without the timetable there is no route lookup and no trip detail, so the
    # server still starts but every data endpoint says why it can't help.
    try:
        app.state.route_types = db.route_types()
        db.prepare_recorder()
        recording = True
    except db.MissingDatabase as exc:
        print(f"{exc} Running without timetable data.", flush=True)
        app.state.route_types = {}
        recording = False

    task = asyncio.create_task(poll_feed(cache, app.state.route_types, recording))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="Adelaide Metro Dashboard", lifespan=lifespan)


def open_db():
    try:
        return db.connect()
    except db.MissingDatabase as exc:
        raise HTTPException(503, str(exc))


async def wait_for_fresh(cache: FeedCache, timeout: float = 8.0) -> None:
    """
    Hold a request until the poller has caught up. After an idle pause the
    cache can be hours old, and serving that is exactly the bug where a vehicle
    is drawn where it used to be while its timetable is read off the clock now.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        age = cache.age_seconds()
        if age is not None and age <= POLL_INTERVAL_SECONDS:
            return
        await asyncio.sleep(0.25)


# --- Geometry ----------------------------------------------------------------
# All of this used to exist twice, once here and once in JavaScript. The client
# now asks for the answer instead of working it out again.


def metres_between(a: tuple, b: tuple) -> float:
    dy = (a[0] - b[0]) * 111320
    dx = (a[1] - b[1]) * 111320 * math.cos(math.radians(a[0]))
    return math.hypot(dx, dy)


def project_on_segment(point: tuple, a: tuple, b: tuple) -> tuple[float, float]:
    """Distance from point to the segment a-b, and how far along it lands."""
    scale = math.cos(math.radians(point[0]))
    ax, ay = (a[1] - point[1]) * 111320 * scale, (a[0] - point[0]) * 111320
    bx, by = (b[1] - point[1]) * 111320 * scale, (b[0] - point[0]) * 111320
    dx, dy = bx - ax, by - ay
    length_sq = dx * dx + dy * dy
    if not length_sq:
        return math.hypot(ax, ay), 0.0
    t = max(0.0, min(1.0, -(ax * dx + ay * dy) / length_sq))
    return math.hypot(ax + t * dx, ay + t * dy), t


def next_stop_by_position(points: list[tuple], position: tuple) -> int | None:
    """
    Index of the stop a vehicle is heading for, from where it actually is.
    Going by the clock breaks the moment a vehicle runs late: the timetable
    claims it should be several stops on, so those get marked as already
    visited when it has not been near them.
    """
    best = None
    best_distance = float("inf")
    for i in range(len(points) - 1):
        if points[i] is None or points[i + 1] is None:
            continue
        distance, t = project_on_segment(position, points[i], points[i + 1])
        if distance < best_distance:
            best_distance = distance
            # t of 0 means it hasn't started this leg, so points[i] is still ahead.
            best = i + 1 if t > 0 else i
    return best


def stop_at(points: list[tuple], position: tuple) -> int | None:
    """Index of the stop the vehicle is parked at, if it is at one."""
    best = None
    best_distance = AT_STOP_METRES
    for i, point in enumerate(points):
        if point is None:
            continue
        distance = metres_between(position, point)
        if distance < best_distance:
            best_distance = distance
            best = i
    return best


def to_seconds(value: str | None) -> int | None:
    if not value:
        return None
    hours, minutes, seconds = (int(part) for part in value.split(":"))
    return hours * 3600 + minutes * 60 + seconds


def delay_minutes(stops: list[dict], index: int | None, now: str) -> int | None:
    """
    How late the vehicle is, from when it is due at the stop it is heading for.
    None near midnight, where GTFS's 24:xx times make the arithmetic nonsense.
    """
    if index is None or index >= len(stops):
        return None
    due = to_seconds(stops[index]["arrival"] or stops[index]["departure"])
    current = to_seconds(now)
    if due is None or current is None or abs(current - due) > 3 * 3600:
        return None
    return round((current - due) / 60)


# --- Live feed ---------------------------------------------------------------


@app.get("/api/snapshot")
async def api_snapshot() -> dict:
    """Every vehicle, for the first render."""
    cache: FeedCache = app.state.cache
    cache.touch()
    await wait_for_fresh(cache)
    return {"vehicles": cache.vehicles, "count": len(cache.vehicles), **cache.timing()}


@app.get("/api/vehicles")
async def api_vehicles() -> dict:
    """The same list, plus the timing the client's countdown runs off."""
    cache: FeedCache = app.state.cache
    cache.touch()
    await wait_for_fresh(cache)
    return {"vehicles": cache.vehicles, "count": len(cache.vehicles), **cache.timing()}


# --- Timetable ---------------------------------------------------------------
# Not async on purpose - FastAPI runs sync endpoints in a threadpool, which
# keeps these blocking sqlite queries off the event loop for free.


@app.get("/api/trip/{trip_id}")
def api_trip(trip_id: str, lat: float | None = None, lon: float | None = None) -> dict:
    """
    A trip's timetable, route geometry, and - when the caller passes the
    vehicle's position - where along it that vehicle currently is.
    """
    conn = open_db()
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

        rows = conn.execute(
            """SELECT st.stop_sequence, st.arrival_time, st.departure_time,
                      s.stop_name, s.stop_lat, s.stop_lon
               FROM stop_times st JOIN stops s ON s.stop_id = st.stop_id
               WHERE st.trip_id = ? ORDER BY CAST(st.stop_sequence AS INTEGER)""",
            (trip_id,),
        ).fetchall()
    finally:
        conn.close()

    stops = [
        {
            "sequence": seq,
            "arrival": arrival,
            "departure": departure,
            "name": name,
            "latitude": float(lat_) if lat_ else None,
            "longitude": float(lon_) if lon_ else None,
        }
        for seq, arrival, departure, name, lat_, lon_ in rows
    ]
    points = [
        (s["latitude"], s["longitude"]) if s["latitude"] is not None else None for s in stops
    ]
    now = datetime.now().strftime("%H:%M:%S")

    at_index = next_index = None
    if lat is not None and lon is not None:
        at_index = stop_at(points, (lat, lon))
        if at_index is not None:
            # Sitting at a stop means the one after it is next, even though the
            # timetable still lists this one as due.
            next_index = at_index + 1 if at_index + 1 < len(stops) else None
        else:
            next_index = next_stop_by_position(points, (lat, lon))
    else:
        # No position to go on, so fall back to the clock.
        next_index = next((i for i, s in enumerate(stops) if (s["arrival"] or s["departure"]) > now), None)

    route_id, headsign, direction, _shape_id, long_name, short_name, route_type = trip
    return {
        "trip_id": trip_id,
        "route_id": route_id,
        "route_name": long_name or short_name or route_id,
        # The number actually painted on the vehicle, e.g. 430 or M44.
        "route_short": short_name or route_id,
        "headsign": headsign,
        "direction_id": direction,
        "route_type": route_type,
        "at_stop_index": at_index,
        "next_stop_index": next_index,
        "delay_minutes": delay_minutes(stops, next_index, now),
        "now": now,
        "shape": [[float(lat_), float(lon_)] for lat_, lon_ in shape],
        "stops": stops,
    }


def departures_at(conn, stop_id: str, services: set[str], after: str, limit: int) -> list[dict]:
    if not services:
        return []
    marks = ",".join("?" * len(services))
    rows = conn.execute(
        f"""SELECT COALESCE(NULLIF(st.departure_time,''), st.arrival_time) AS t,
                   r.route_id, r.route_short_name, r.route_type, t2.trip_headsign, t2.trip_id
            FROM stop_times st
            JOIN trips t2 ON t2.trip_id = st.trip_id
            JOIN routes r ON r.route_id = t2.route_id
            WHERE st.stop_id = ? AND t2.service_id IN ({marks}) AND t > ?
            ORDER BY t LIMIT ?""",
        (stop_id, *services, after, limit),
    ).fetchall()
    return [
        {
            "time": when,
            "route_id": route_id,
            "route_short": short or route_id,
            "type": db.gtfs_type_name(route_type),
            "school": route_type == db.SCHOOL_ROUTE_TYPE,
            "headsign": headsign,
            "trip_id": trip_id,
        }
        for when, route_id, short, route_type, headsign, trip_id in rows
    ]


def approaching_vehicles(conn, stop_id: str, target: tuple, vehicles: list[dict], short_names: dict) -> list[dict]:
    """Live vehicles on a trip that calls at this stop, and how far off they are."""
    by_trip = {v["trip_id"]: v for v in vehicles if v.get("trip_id")}
    if not by_trip:
        return []

    marks = ",".join("?" * len(by_trip))
    calls = conn.execute(
        f"""SELECT st.trip_id, st.stop_sequence, st.arrival_time, st.departure_time
            FROM stop_times st
            WHERE st.stop_id = ? AND st.trip_id IN ({marks})""",
        (stop_id, *by_trip),
    ).fetchall()
    if not calls:
        return []

    calling_ids = [c[0] for c in calls]
    marks = ",".join("?" * len(calling_ids))
    sequences: dict[str, list] = {}
    for trip_id, seq, lat, lon in conn.execute(
        f"""SELECT st.trip_id, st.stop_sequence, s.stop_lat, s.stop_lon
            FROM stop_times st JOIN stops s ON s.stop_id = st.stop_id
            WHERE st.trip_id IN ({marks})""",
        calling_ids,
    ):
        sequences.setdefault(trip_id, []).append((int(seq), float(lat), float(lon)))
    for legs in sequences.values():
        legs.sort()

    headsigns = dict(
        conn.execute(
            f"SELECT trip_id, trip_headsign FROM trips WHERE trip_id IN ({marks})", calling_ids
        )
    )

    approaching = []
    for trip_id, seq, arrival, departure in calls:
        vehicle = by_trip[trip_id]
        legs = sequences.get(trip_id)
        if not legs or vehicle["latitude"] is None:
            continue
        target_index = next((i for i, leg in enumerate(legs) if leg[0] == int(seq)), None)
        if target_index is None:
            continue

        position = (vehicle["latitude"], vehicle["longitude"])
        points = [(leg[1], leg[2]) for leg in legs]
        current = next_stop_by_position(points, position) or 0
        stops_away = target_index - current
        approaching.append({
            "vehicle_id": vehicle["vehicle_id"],
            "trip_id": trip_id,
            "route_id": vehicle["route_id"],
            "route_short": short_names.get(vehicle["route_id"], vehicle["route_id"]),
            "type": vehicle["type"],
            "headsign": headsigns.get(trip_id),
            "speed": vehicle["speed"],
            "distance_m": round(metres_between(target, position)),
            "stops_away": stops_away,
            # Negative means it has already been past.
            "inbound": stops_away >= 0,
            "due": arrival or departure,
        })

    approaching.sort(key=lambda a: (not a["inbound"], a["stops_away"], a["distance_m"]))
    return approaching


@app.get("/api/stops")
def api_stops(south: float, west: float, north: float, east: float, limit: int = 400) -> dict:
    """Stops inside the current map view. All 9,167 at once is unusable."""
    conn = open_db()
    try:
        rows = conn.execute(
            """SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon FROM stops
               WHERE CAST(stop_lat AS REAL) BETWEEN ? AND ?
                 AND CAST(stop_lon AS REAL) BETWEEN ? AND ?
               LIMIT ?""",
            (south, north, west, east, limit + 1),
        ).fetchall()
    finally:
        conn.close()

    return {
        "stops": [
            {"stop_id": sid, "code": code, "name": name, "latitude": float(lat), "longitude": float(lon)}
            for sid, code, name, lat, lon in rows[:limit]
        ],
        "truncated": len(rows) > limit,
    }


@app.get("/api/stop/{stop_id}")
def api_stop(stop_id: str) -> dict:
    """Everything about one stop: what serves it, what's coming, what's due."""
    cache: FeedCache = app.state.cache
    cache.touch()

    conn = open_db()
    try:
        stop = conn.execute(
            "SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon FROM stops WHERE stop_id = ?",
            (stop_id,),
        ).fetchone()
        if not stop:
            raise HTTPException(404, f"stop {stop_id} not found")
        target = (float(stop[3]), float(stop[4]))

        # Trip count matters here: a stop can list a route that only calls once
        # a day. Adelaide's school services do exactly that, which is why this
        # list is longer than the one a journey planner shows.
        routes = conn.execute(
            """SELECT r.route_id, r.route_long_name, r.route_short_name, r.route_type,
                      COUNT(DISTINCT t.trip_id) AS trips
               FROM stop_times st
               JOIN trips t ON t.trip_id = st.trip_id
               JOIN routes r ON r.route_id = t.route_id
               WHERE st.stop_id = ?
               GROUP BY r.route_id
               ORDER BY trips DESC, r.route_id""",
            (stop_id,),
        ).fetchall()
        short_names = {rid: (short or rid) for rid, _long, short, _type, _n in routes}

        approaching = approaching_vehicles(conn, stop_id, target, cache.vehicles, short_names)

        # The timetable side. Matters most on a quiet stop, where "no live
        # vehicle is coming" tells you nothing about when one actually will.
        now = datetime.now().strftime("%H:%M:%S")
        today = date.today()
        scheduled = departures_at(conn, stop_id, db.services_on(conn, today), now, 12)

        # Nothing left today, so say which day it next runs rather than leaving
        # a school-only stop looking dead.
        next_day = None
        if not scheduled:
            for offset in range(1, 8):
                day = today + timedelta(days=offset)
                later = departures_at(conn, stop_id, db.services_on(conn, day), "00:00:00", 6)
                if later:
                    next_day = {
                        "date": day.isoformat(),
                        "weekday": day.strftime("%A"),
                        "departures": later,
                    }
                    break
    finally:
        conn.close()

    return {
        "stop": {
            "stop_id": stop[0],
            "code": stop[1],
            "name": stop[2],
            "latitude": target[0],
            "longitude": target[1],
        },
        "routes": [
            {
                "route_id": rid,
                "name": long_name or short or rid,
                "type": db.gtfs_type_name(route_type),
                "school": route_type == db.SCHOOL_ROUTE_TYPE,
                "trips": trips,
            }
            for rid, long_name, short, route_type, trips in routes
        ],
        "approaching": approaching,
        "scheduled": scheduled,
        "next_service_day": next_day,
        "now": now,
    }


# Explicit route, not a static mount - a mount would also serve .git.
@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "map.html")
