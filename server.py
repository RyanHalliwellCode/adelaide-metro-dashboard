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
import re
import time
from contextlib import asynccontextmanager, suppress
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

import db
import journey
from main import (
    SERVICE_ALERTS_URL,
    TRIP_UPDATES_URL,
    fetch_debug_text,
    parse_alerts,
    parse_feed_timestamp,
    parse_trip_updates,
    parse_vehicles,
)

BASE_DIR = Path(__file__).parent

# The feed only republishes every ~15s, so polling faster is wasted bandwidth.
POLL_INTERVAL_SECONDS = 15

# Predictions are a 1.5 MB download and they shift slowly, so they get their own
# slower cycle rather than riding along with the 220 KB position feed.
PREDICTION_INTERVAL_SECONDS = 30

# Disruptions are written by hand and last for weeks, so there is no point
# checking them often.
ALERT_INTERVAL_SECONDS = 300

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
        # trip_id -> stop_id -> predicted arrival (unix seconds).
        self.predictions: dict[str, dict[str, int]] = {}
        self.predictions_at: float | None = None
        self.alerts: list[dict] = []
        self.alerts_at: float | None = None

    def predictions_due(self) -> bool:
        return (
            self.predictions_at is None
            or (time.monotonic() - self.predictions_at) >= PREDICTION_INTERVAL_SECONDS
        )

    def alerts_due(self) -> bool:
        return (
            self.alerts_at is None
            or (time.monotonic() - self.alerts_at) >= ALERT_INTERVAL_SECONDS
        )

    def active_alerts(self) -> list[dict]:
        """
        In force right now. Ten of the sixty published alerts have a start date
        in the future - roadworks announced ahead of time - so filtering only on
        the end date advertises disruptions that haven't begun.
        """
        now = time.time()
        return [
            a
            for a in self.alerts
            if (not a["starts"] or a["starts"] <= now) and (not a["ends"] or a["ends"] > now)
        ]

    def alerts_for(self, route_ids: set[str]) -> list[dict]:
        """Alerts naming any of these routes, plus any that name none at all."""
        return [
            a
            for a in self.active_alerts()
            if not a["routes"] or route_ids.intersection(a["routes"])
        ]

    def predicted_arrival(self, trip_id: str, stop_id: str) -> int | None:
        return self.predictions.get(trip_id, {}).get(stop_id)

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

        # Predictions are a bonus on top of positions, so a failure here leaves
        # the map working with whatever it had.
        if cache.predictions_due():
            try:
                text = await asyncio.to_thread(fetch_debug_text, TRIP_UPDATES_URL)
                cache.predictions = parse_trip_updates(text)
                cache.predictions_at = time.monotonic()
                print(f"Polled predictions: {len(cache.predictions)} trips", flush=True)
            except Exception as exc:
                print(f"Prediction poll failed: {type(exc).__name__}: {exc}", flush=True)

        if cache.alerts_due():
            try:
                text = await asyncio.to_thread(fetch_debug_text, SERVICE_ALERTS_URL)
                alerts = parse_alerts(text)
                await asyncio.to_thread(locate_alerts, alerts)
                cache.alerts = alerts
                cache.alerts_at = time.monotonic()
                mapped = sum(1 for a in alerts if a["stops"])
                print(
                    f"Polled alerts: {len(alerts)} disruptions, {mapped} pinned to a stop",
                    flush=True,
                )
            except Exception as exc:
                print(f"Alert poll failed: {type(exc).__name__}: {exc}", flush=True)

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
        db.ensure_stop_modes()
        db.ensure_walk_transfers()
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


# Alert headlines name the stop they concern - "Stop U1 Halifax Street" - but
# the feed gives no coordinates, only affected routes. Pulling the stop code out
# of the text and matching it against the timetable is what puts them on a map.
ALERT_STOP_TOKEN_RE = re.compile(r"\bStop ([0-9]+[A-Z]?|[A-Z]{1,2}[0-9]+[A-Z]?)\b")

# Stop names abbreviate; headlines spell it out.
STREET_ABBREVIATIONS = {
    "road": "rd", "street": "st", "highway": "hwy", "avenue": "av", "drive": "dr",
    "terrace": "tce", "parade": "pde", "crescent": "cr", "boulevard": "blvd",
    "place": "pl", "court": "ct", "esplanade": "esp", "circuit": "cct", "lane": "la",
}
HEADLINE_NOISE = {
    "stop", "stops", "closure", "closures", "change", "changes", "detour", "detours",
    "and", "temporary", "relocation", "side", "north", "south", "east", "west",
    "bus", "service", "services", "from", "the", "until", "further", "notice",
}


def headline_streets(header: str) -> list[str]:
    """Words from a headline that might name a street, in stop-name spelling."""
    head = ALERT_STOP_TOKEN_RE.sub(" ", header.split(" - From ")[0])
    words = re.findall(r"[A-Za-z]+", head.lower())
    return [
        STREET_ABBREVIATIONS.get(w, w)
        for w in words
        if w not in HEADLINE_NOISE and len(w) > 2
    ]


def locate_alerts(alerts: list[dict]) -> None:
    """
    Attach coordinates to alerts we can pin to a stop, in place.

    Stop numbers repeat across the network - there are many "Stop 2" - so a
    candidate only counts if its street also appears in the headline. Without
    that check "Stop 2 South Road" matches a stop on Sir Edwin Smith Ave.
    """
    try:
        conn = db.connect()
    except db.MissingDatabase:
        return

    try:
        for alert in alerts:
            alert["stops"] = []
            tokens = set(ALERT_STOP_TOKEN_RE.findall(alert["header"]))
            if not tokens or not alert["routes"]:
                continue

            streets = headline_streets(alert["header"])
            marks = ",".join("?" * len(alert["routes"]))
            found = {}
            for token in tokens:
                rows = conn.execute(
                    f"""SELECT DISTINCT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
                        FROM stops s
                        JOIN stop_times st ON st.stop_id = s.stop_id
                        JOIN trips t ON t.trip_id = st.trip_id
                        WHERE t.route_id IN ({marks}) AND s.stop_name LIKE ?""",
                    (*alert["routes"], f"Stop {token} %"),
                ).fetchall()
                for stop_id, name, lat, lon in rows:
                    if any(word in name.lower() for word in streets):
                        found[stop_id] = {
                            "stop_id": stop_id,
                            "name": name,
                            "latitude": float(lat),
                            "longitude": float(lon),
                        }
            alert["stops"] = list(found.values())
    finally:
        conn.close()


def describe_prediction(epoch: int | None, scheduled: str | None) -> dict:
    """
    Turn a predicted unix timestamp into what the panel needs: a clock time,
    minutes from now, and how far off the timetable it is.
    """
    if not epoch:
        return {"predicted": None, "minutes_away": None, "predicted_delay": None}

    when = datetime.fromtimestamp(epoch)
    minutes_away = round((epoch - time.time()) / 60)

    predicted_delay = None
    due = to_seconds(scheduled)
    if due is not None:
        # GTFS writes after-midnight as 24:xx, so compare inside one day's
        # seconds rather than trying to build a real datetime from it.
        actual = when.hour * 3600 + when.minute * 60 + when.second
        diff = actual - (due % 86400)
        # A trip either side of midnight otherwise looks nearly a day out.
        if diff > 43200:
            diff -= 86400
        elif diff < -43200:
            diff += 86400
        predicted_delay = round(diff / 60)

    return {
        "predicted": when.strftime("%H:%M:%S"),
        "minutes_away": minutes_away,
        "predicted_delay": predicted_delay,
    }


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


@app.get("/api/alerts")
async def api_alerts() -> dict:
    """Disruptions in force now, and any announced for later."""
    cache: FeedCache = app.state.cache
    cache.touch()
    now = time.time()
    active = cache.active_alerts()
    upcoming = [a for a in cache.alerts if a["starts"] and a["starts"] > now]
    return {
        "alerts": active,
        "count": len(active),
        "upcoming": upcoming,
        "mapped": sum(1 for a in active if a["stops"]),
        "checked_at": (
            datetime.fromtimestamp(time.time() - (time.monotonic() - cache.alerts_at)).isoformat(
                timespec="seconds"
            )
            if cache.alerts_at
            else None
        ),
    }


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
                      s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
               FROM stop_times st JOIN stops s ON s.stop_id = st.stop_id
               WHERE st.trip_id = ? ORDER BY CAST(st.stop_sequence AS INTEGER)""",
            (trip_id,),
        ).fetchall()
    finally:
        conn.close()

    cache: FeedCache = app.state.cache
    stops = []
    for seq, arrival, departure, stop_id, name, lat_, lon_ in rows:
        stops.append({
            "sequence": seq,
            "arrival": arrival,
            "departure": departure,
            "stop_id": stop_id,
            "name": name,
            "latitude": float(lat_) if lat_ else None,
            "longitude": float(lon_) if lon_ else None,
            **describe_prediction(cache.predicted_arrival(trip_id, stop_id), arrival or departure),
        })
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
        # The operator's own figure when they publish one, ours only as backup.
        "delay_minutes": (
            stops[next_index]["predicted_delay"]
            if next_index is not None and stops[next_index].get("predicted_delay") is not None
            else delay_minutes(stops, next_index, now)
        ),
        "delay_source": (
            "predicted"
            if next_index is not None and stops[next_index].get("predicted_delay") is not None
            else "timetable"
        ),
        "has_predictions": any(s["predicted"] for s in stops),
        "alerts": cache.alerts_for({route_id}),
        "now": now,
        "shape": [[float(lat_), float(lon_)] for lat_, lon_ in shape],
        "stops": stops,
    }


def normalise_time(value: str | None) -> str | None:
    """Accept '17:30' or '17:30:00' from the query string, or nothing."""
    if not value:
        return None
    parts = value.split(":")
    if len(parts) < 2 or not all(p.isdigit() for p in parts):
        raise HTTPException(400, f"bad time '{value}' - expected HH:MM")
    hours, minutes = int(parts[0]), int(parts[1])
    seconds = int(parts[2]) if len(parts) > 2 else 0
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def platforms_of(conn, stop_id: str) -> list[str]:
    """
    The stop itself plus any platforms grouped under it.

    A station like Elizabeth is three records: one platform towards the city,
    one towards Gawler, and a parent that holds no departures at all. Clicking
    the parent has to answer for the whole station or it looks broken.
    """
    children = [
        row[0]
        for row in conn.execute("SELECT stop_id FROM stops WHERE parent_station = ?", (stop_id,))
    ]
    return [stop_id, *children]


def departures_at(
    conn, stop_ids: list[str], services: set[str], after: str, limit: int, before: str | None = None
) -> list[dict]:
    if not services:
        return []
    marks = ",".join("?" * len(services))
    stop_marks = ",".join("?" * len(stop_ids))
    window = "AND t < ?" if before else ""
    rows = conn.execute(
        f"""SELECT COALESCE(NULLIF(st.departure_time,''), st.arrival_time) AS t,
                   r.route_id, r.route_short_name, r.route_type, t2.trip_headsign, t2.trip_id
            FROM stop_times st
            JOIN trips t2 ON t2.trip_id = st.trip_id
            JOIN routes r ON r.route_id = t2.route_id
            WHERE st.stop_id IN ({stop_marks}) AND t2.service_id IN ({marks}) AND t >= ? {window}
            ORDER BY t LIMIT ?""",
        (*stop_ids, *services, after, *([before] if before else []), limit),
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


def approaching_vehicles(conn, stop_ids: list[str], target: tuple, vehicles: list[dict], short_names: dict, cache=None) -> list[dict]:
    """Live vehicles on a trip that calls at this stop, and how far off they are."""
    by_trip = {v["trip_id"]: v for v in vehicles if v.get("trip_id")}
    if not by_trip:
        return []

    marks = ",".join("?" * len(by_trip))
    stop_marks = ",".join("?" * len(stop_ids))
    calls = conn.execute(
        f"""SELECT st.trip_id, st.stop_sequence, st.arrival_time, st.departure_time, st.stop_id
            FROM stop_times st
            WHERE st.stop_id IN ({stop_marks}) AND st.trip_id IN ({marks})""",
        (*stop_ids, *by_trip),
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
    for trip_id, seq, arrival, departure, called_at in calls:
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
        predicted = cache.predicted_arrival(trip_id, called_at) if cache else None
        approaching.append({
            "vehicle_id": vehicle["vehicle_id"],
            "trip_id": trip_id,
            "route_id": vehicle["route_id"],
            "route_short": short_names.get(vehicle["route_id"], vehicle["route_id"]),
            "type": vehicle["type"],
            "headsign": headsigns.get(trip_id),
            "speed": vehicle["speed"],
            "occupancy_text": vehicle.get("occupancy_text"),
            "wheelchair": vehicle.get("wheelchair"),
            "air_conditioned": vehicle.get("air_conditioned"),
            "schedule_relationship": vehicle.get("schedule_relationship"),
            "distance_m": round(metres_between(target, position)),
            "stops_away": stops_away,
            # Negative means it has already been past.
            "inbound": stops_away >= 0,
            "due": arrival or departure,
            **describe_prediction(predicted, arrival or departure),
        })

    # Soonest first when we have a real prediction, falling back to stop count.
    approaching.sort(
        key=lambda a: (
            not a["inbound"],
            a["minutes_away"] if a["minutes_away"] is not None else 999,
            a["stops_away"],
        )
    )
    return approaching


@app.get("/api/plan")
def api_plan(
    from_lat: float, from_lon: float, to_lat: float, to_lon: float, after: str | None = None
) -> dict:
    """
    Fastest way between two points, walking included at both ends.

    Every stop within range of the start is considered, not just the nearest -
    so a stop five minutes further away wins when the service from it is
    fifteen minutes better.
    """
    if not db.DB_PATH.exists():
        raise HTTPException(503, "gtfs.db missing - run 'python gtfs_static.py' first.")
    return journey.plan((from_lat, from_lon), (to_lat, to_lon), normalise_time(after))


@app.get("/api/places")
def api_places(q: str, limit: int = 8) -> dict:
    """Stop and station name search, for typing a destination instead of clicking."""
    conn = open_db()
    try:
        rows = conn.execute(
            """SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, m.modes
               FROM stops s LEFT JOIN stop_modes m ON m.stop_id = s.stop_id
               WHERE s.parent_station = '' AND s.stop_name LIKE ?
               ORDER BY LENGTH(s.stop_name) LIMIT ?""",
            (f"%{q}%", limit),
        ).fetchall()
    finally:
        conn.close()
    return {
        "places": [
            {
                "stop_id": sid,
                "name": name,
                "latitude": float(lat),
                "longitude": float(lon),
                "modes": modes.split(",") if modes else [],
            }
            for sid, name, lat, lon, modes in rows
        ]
    }


@app.get("/api/network")
def api_network(every: int = 12) -> dict:
    """
    Every route shape in the network, thinned, for drawing as a backdrop.
    977,376 points is too many to hand a browser, so take every nth.
    """
    conn = open_db()
    try:
        kinds = {
            shape_id: db.gtfs_type_name(route_type)
            for shape_id, route_type in conn.execute(
                """SELECT DISTINCT t.shape_id, r.route_type FROM trips t
                   JOIN routes r ON r.route_id = t.route_id WHERE t.shape_id != ''"""
            )
        }
        points: dict[str, list] = {}
        for shape_id, lat, lon in conn.execute(
            """SELECT shape_id, shape_pt_lat, shape_pt_lon FROM shapes
               ORDER BY shape_id, CAST(shape_pt_sequence AS INTEGER)"""
        ):
            points.setdefault(shape_id, []).append([round(float(lat), 5), round(float(lon), 5)])
    finally:
        conn.close()

    lines: dict[str, list] = {}
    for shape_id, pts in points.items():
        thin = pts[::every]
        if thin[-1] != pts[-1]:
            thin.append(pts[-1])
        if len(thin) > 1:
            lines.setdefault(kinds.get(shape_id, "bus"), []).append(thin)

    return {"lines": lines, "counts": {k: len(v) for k, v in lines.items()}}


@app.get("/api/stops")
def api_stops(south: float, west: float, north: float, east: float, limit: int = 400) -> dict:
    """Stops inside the current map view. All 9,167 at once is unusable."""
    conn = open_db()
    try:
        # Platforms are excluded in favour of the station that groups them.
        # Elizabeth's two platforms sit 60 m apart and would just stack on the
        # map; the station answers for both directions anyway.
        rows = conn.execute(
            """SELECT s.stop_id, s.stop_code, s.stop_name, s.stop_lat, s.stop_lon, m.modes
               FROM stops s LEFT JOIN stop_modes m ON m.stop_id = s.stop_id
               WHERE s.parent_station = ''
                 AND CAST(s.stop_lat AS REAL) BETWEEN ? AND ?
                 AND CAST(s.stop_lon AS REAL) BETWEEN ? AND ?
               LIMIT ?""",
            (south, north, west, east, limit + 1),
        ).fetchall()
    finally:
        conn.close()

    return {
        "stops": [
            {
                "stop_id": sid,
                "code": code,
                "name": name,
                "latitude": float(lat),
                "longitude": float(lon),
                "modes": modes.split(",") if modes else [],
            }
            for sid, code, name, lat, lon, modes in rows[:limit]
        ],
        "truncated": len(rows) > limit,
    }


@app.get("/api/stop/{stop_id}")
def api_stop(stop_id: str, from_time: str | None = None, to_time: str | None = None) -> dict:
    """
    Everything about one stop: what serves it, what's coming, what's due.

    from_time/to_time narrow the timetable to a window - "what leaves here
    between 5:30 and 6:30" - instead of the next few from right now.
    """
    cache: FeedCache = app.state.cache
    cache.touch()
    window_from = normalise_time(from_time)
    window_to = normalise_time(to_time)
    # "from 5 to 5" is an empty range and would return nothing, which reads as
    # broken. Treat any end that isn't after the start as open-ended instead.
    if window_from and window_to and window_to <= window_from:
        window_to = None

    conn = open_db()
    try:
        stop = conn.execute(
            "SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon FROM stops WHERE stop_id = ?",
            (stop_id,),
        ).fetchone()
        if not stop:
            raise HTTPException(404, f"stop {stop_id} not found")
        target = (float(stop[3]), float(stop[4]))
        stop_ids = platforms_of(conn, stop_id)
        stop_marks = ",".join("?" * len(stop_ids))

        # Trip count matters here: a stop can list a route that only calls once
        # a day. Adelaide's school services do exactly that, which is why this
        # list is longer than the one a journey planner shows.
        routes = conn.execute(
            f"""SELECT r.route_id, r.route_long_name, r.route_short_name, r.route_type,
                       COUNT(DISTINCT t.trip_id) AS trips
                FROM stop_times st
                JOIN trips t ON t.trip_id = st.trip_id
                JOIN routes r ON r.route_id = t.route_id
                WHERE st.stop_id IN ({stop_marks})
                GROUP BY r.route_id
                ORDER BY trips DESC, r.route_id""",
            stop_ids,
        ).fetchall()
        short_names = {rid: (short or rid) for rid, _long, short, _type, _n in routes}

        approaching = approaching_vehicles(conn, stop_ids, target, cache.vehicles, short_names, cache)

        # The timetable side. Matters most on a quiet stop, where "no live
        # vehicle is coming" tells you nothing about when one actually will.
        now = datetime.now().strftime("%H:%M:%S")
        today = date.today()
        today_services = db.services_on(conn, today)

        if window_from or window_to:
            # An explicit window can hold a lot more than the next few, and the
            # point is to see everything in it.
            scheduled = departures_at(
                conn, stop_ids, today_services, window_from or "00:00:00", 60, window_to
            )
        else:
            scheduled = departures_at(conn, stop_ids, today_services, now, 12)

        # Nothing left today, so say which day it next runs rather than leaving
        # a school-only stop looking dead. Only when browsing from now - an
        # empty window just means nothing runs then, which is the answer.
        next_day = None
        if not scheduled and not (window_from or window_to):
            for offset in range(1, 8):
                day = today + timedelta(days=offset)
                later = departures_at(conn, stop_ids, db.services_on(conn, day), "00:00:00", 6)
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
        # Anything disrupting a route that calls here.
        "alerts": cache.alerts_for({rid for rid, *_ in routes}),
        "window": {"from": window_from, "to": window_to},
        "now": now,
    }


@app.get("/api/shapes")
def api_shapes(trip_ids: str, every: int = 4) -> dict:
    """
    Route geometry for several trips at once, thinned. Used to sketch every
    route leaving a stop in a time window - context lines, so full precision
    would just be a slower way to draw the same picture.
    """
    wanted = [t for t in trip_ids.split(",") if t][:40]
    if not wanted:
        return {"shapes": {}}

    conn = open_db()
    try:
        marks = ",".join("?" * len(wanted))
        shape_of = dict(
            conn.execute(f"SELECT trip_id, shape_id FROM trips WHERE trip_id IN ({marks})", wanted)
        )
        shape_ids = sorted({s for s in shape_of.values() if s})
        if not shape_ids:
            return {"shapes": {}}

        marks = ",".join("?" * len(shape_ids))
        points: dict[str, list] = {}
        for shape_id, lat, lon in conn.execute(
            f"""SELECT shape_id, shape_pt_lat, shape_pt_lon FROM shapes
                WHERE shape_id IN ({marks})
                ORDER BY shape_id, CAST(shape_pt_sequence AS INTEGER)""",
            shape_ids,
        ):
            points.setdefault(shape_id, []).append([float(lat), float(lon)])
    finally:
        conn.close()

    thinned = {}
    for shape_id, pts in points.items():
        # Keep the last point so the line still reaches the end of the route.
        kept = pts[::every]
        if kept[-1] != pts[-1]:
            kept.append(pts[-1])
        thinned[shape_id] = kept

    return {"shapes": {trip: thinned.get(shape_of.get(trip), []) for trip in wanted}}


# Explicit route, not a static mount - a mount would also serve .git.
@app.get("/")
async def index() -> FileResponse:
    # no-store because this is a dev server and a cached map.html means edits
    # silently don't appear - you end up debugging code the browser isn't running.
    return FileResponse(
        BASE_DIR / "map.html",
        headers={"Cache-Control": "no-store, must-revalidate"},
    )
